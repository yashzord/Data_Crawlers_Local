import logging
import pymongo
import os
import time
from dotenv import load_dotenv
from faktory import Client, Worker
from youtube_client import YouTubeClient
from datetime import datetime
import multiprocessing
import requests
from requests.exceptions import HTTPError

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['youtube_data']
channels_collection = db['channels']
videos_collection = db['videos']
comments_collection = db['comments']

logger = logging.getLogger("YouTubeCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL")

MAX_RETRIES = 5
RETRY_DELAY = 5  

def retry_on_network_and_http_errors(func, *args):
    retries = 0
    delay = RETRY_DELAY
    
    while retries < MAX_RETRIES:
        try:
            return func(*args)
        
        except HTTPError as http_err:
            status_code = http_err.response.status_code

            if 400 <= status_code < 500:
                if status_code == 404:
                    logger.warning(f"Resource not found (404). video/channel {args[1]} might be deleted.")
                else:
                    logger.error(f"Client error occurred (status {status_code}) for {args[1]}. No retry.")
                break  

            elif 500 <= status_code < 600:
                logger.error(f"Server error (status {status_code}) occurred. Retrying in {delay} seconds...")
                time.sleep(delay)
                retries += 1
                delay *= 2  
                
        except RequestException as req_err:
            logger.error(f"Network error: {req_err}. Retrying in {delay} seconds...")
            time.sleep(delay)
            retries += 1
            delay *= 2  

    logger.error(f"Max retries reached. Failed to execute {func.__name__} after {MAX_RETRIES} attempts.")
    return None

def crawl_video(channel_id, video_id):

    youtube_client = YouTubeClient()
    video_data = retry_on_network_and_http_errors(youtube_client.get_video_details, video_id)

    print(f"video_data{video_data}")

    if video_data is None:
        logger.warning(f"Video {video_id} might be deleted or unavailable.")
        try:
            result = videos_collection.update_one(
                {"video_id": video_id},
                {"$set": {"isDeleted": True, "crawled_at": datetime.now()}},  
                upsert=True  
            )
            if result.matched_count:
                logger.info(f"Updated video {video_id} as deleted in MongoDB.")
            else:
                logger.info(f"Inserted deleted video {video_id} into MongoDB.")
        except Exception as e:
            logger.error(f"Error updating video {video_id} as deleted in MongoDB: {e}")
        return

    comments_data = retry_on_network_and_http_errors(youtube_client.get_video_comments, video_id)
    comment_count = len(comments_data) if comments_data else 0

    video_info = {
        "channel_id": channel_id,
        "video_id": video_id,
        "title": video_data['snippet'].get('title', '[Deleted Title]'),
        "description": video_data['snippet'].get('description', '[Deleted Description]'),
        "view_count": video_data['statistics'].get('viewCount', 0),
        "like_count": video_data['statistics'].get('likeCount', 0),
        #"comment_count": video_data['statistics'].get('commentCount', 0),
        "comment_count": comment_count, 
        "comments": comments_data, 
        "published_at": video_data['snippet'].get('publishedAt', '[Unknown Date]'),
        "crawled_at": datetime.now(),
        "isDeleted": False  
    }

    try:
        result = videos_collection.update_one(
            {"video_id": video_id},
            {"$set": video_info},
            upsert=True  
        )
        if result.matched_count:
            logger.info(f"Updated video {video_id} in MongoDB.")
        else:
            logger.info(f"Inserted video {video_id} into MongoDB.")
    except Exception as e:
        logger.error(f"Error inserting or updating video {video_id} into MongoDB: {e}")


def crawl_channel(channel_id):
    youtube_client = YouTubeClient()
    channel_data = retry_on_network_and_http_errors(youtube_client.get_channel_details, channel_id)
    
    if channel_data is None:
        logger.error(f"Channel {channel_id} not found.")
        return

    channel_info = {
        "channel_id": channel_id,
        "channel_name": channel_data['snippet'].get('title', '[Deleted Channel]'),
        "subscriber_count": int(channel_data['statistics'].get('subscriberCount', 0)),
        "view_count": int(channel_data['statistics'].get('viewCount', 0)),
        "video_count": int(channel_data['statistics'].get('videoCount', 0)),
        "crawled_at": datetime.now()
    }

    try:
        result = channels_collection.update_one(
            {"channel_id": channel_id},
            {"$set": channel_info},
            upsert=True  # This will update if it exists, otherwise insert a new document
        )
        if result.matched_count:
            logger.info(f"Updated channel {channel_id} in MongoDB.")
        else:
            logger.info(f"Inserted channel {channel_id} into MongoDB.")
    except Exception as e:
        logger.error(f"Error inserting or updating channel {channel_id} into MongoDB: {e}")
    
    videos = retry_on_network_and_http_errors(youtube_client.get_channel_videos, channel_id)
    if videos is None:
        logger.error(f"Failed to retrieve videos for channel {channel_id}")
        return

    with Client() as client:
        for video in videos:
            video_id = video['id']['videoId']
            client.queue('crawl_video', args=(channel_id, video_id), queue='crawl_video')
            logger.info(f"Queued job to crawl video {video_id} from channel {channel_id}")

def start_worker():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    worker = Worker(queues=['crawl_channel', 'crawl_video'])
    worker.register('crawl_channel', crawl_channel)
    worker.register('crawl_video', crawl_video)
    logger.info("Worker started. Listening for jobs...")
    worker.run()

def schedule_crawl_jobs(interval = 40):
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    channel_ids = os.getenv("YOUTUBE_CHANNELS").split(',')
    while True:
        with Client() as client:
            for channel_id in channel_ids:
                client.queue('crawl_channel', args=(channel_id,), queue='crawl_channel')
                logger.info(f"Queued job to crawl channel: {channel_id}")

        logger.info(f"Waiting for {interval / 60} minutes before the next crawl...")
        time.sleep(interval)


def monitor_queue():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    while True:
        with Client() as client:
            info = client.info()
            total_enqueued = sum(q['size'] for q in info['queues'])
            total_in_progress = info['tasks']['active']
            logger.info(f"Jobs in queue: {total_enqueued}, Jobs in progress: {total_in_progress}")
        time.sleep(40)

if __name__ == "__main__":
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL

    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()

    monitor_process = multiprocessing.Process(target=monitor_queue)
    monitor_process.start()

    

    try:
        schedule_crawl_jobs(interval = 40)
    except KeyboardInterrupt:
        logger.info("Stopping processes...")
        worker_process.terminate()
        monitor_process.terminate()
        worker_process.join()
        monitor_process.join()
        logger.info("Processes stopped.")