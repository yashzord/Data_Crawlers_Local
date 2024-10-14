import logging
import pymongo
import os
import time
from dotenv import load_dotenv
from faktory import Client, Worker
from youtube_client import YouTubeClient
from datetime import datetime
import multiprocessing

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

def crawl_video(channel_id, video_id):

    if videos_collection.find_one({"video_id": video_id}):
        logger.info(f"Video {video_id} already exists")
        return
        
    youtube_client = YouTubeClient()
    video_data = youtube_client.get_video_details(video_id)

    if video_data is None:
        logger.warning(f"Video {video_id} might be deleted or unavailable.")
        return

    video_info = {
        "channel_id": channel_id,
        "video_id": video_id,
        "title": video_data['snippet'].get('title', '[Deleted Title]'),
        "description": video_data['snippet'].get('description', '[Deleted Description]'),
        "view_count": video_data['statistics'].get('viewCount', 0),
        "like_count": video_data['statistics'].get('likeCount', 0),
        "comment_count": video_data['statistics'].get('commentCount', 0),
        "published_at": video_data['snippet'].get('publishedAt', '[Unknown Date]'),
        "crawled_at": datetime.now()
    }

    try:
        result = videos_collection.insert_one(video_info)
        logger.info(f"Inserted video {video_id} into MongoDB with ID: {result.inserted_id}")
    except Exception as e:
        logger.error(f"Error inserting video {video_id} into MongoDB: {e}")


def crawl_channel(channel_id):
    youtube_client = YouTubeClient()
    channel_data = youtube_client.get_channel_details(channel_id)
    
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
        result = channels_collection.insert_one(channel_info)
        logger.info(f"Inserted channel {channel_id} into MongoDB with ID: {result.inserted_id}")
    except Exception as e:
        logger.error(f"Error inserting channel {channel_id} into MongoDB: {e}")

    videos = youtube_client.get_channel_videos(channel_id)
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

def schedule_crawl_jobs():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    channel_ids = os.getenv("YOUTUBE_CHANNELS").split(',')
    with Client() as client:
        for channel_id in channel_ids:
            client.queue('crawl_channel', args=(channel_id,), queue='crawl_channel')
            logger.info(f"Queued job to crawl channel: {channel_id}")

def monitor_queue():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    while True:
        with Client() as client:
            info = client.info()
            total_enqueued = sum(q['size'] for q in info['queues'])
            total_in_progress = info['tasks']['active']
            logger.info(f"Jobs in queue: {total_enqueued}, Jobs in progress: {total_in_progress}")
        time.sleep(30)

if __name__ == "__main__":
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL

    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()

    monitor_process = multiprocessing.Process(target=monitor_queue)
    monitor_process.start()

    schedule_crawl_jobs()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping processes...")
        worker_process.terminate()
        monitor_process.terminate()
        worker_process.join()
        monitor_process.join()
        logger.info("Processes stopped.")
