import logging
import pymongo
import os
import time
from dotenv import load_dotenv
from faktory import Client, Worker
from reddit_client import RedditClient
from datetime import datetime
import multiprocessing

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL") or "mongodb://localhost:27017/"
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['reddit_data']
posts_collection = db['posts']

logger = logging.getLogger("RedditCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL") or 'tcp://:raj123@localhost:7419'

def crawl_post(subreddit, post_id):
    reddit_client = RedditClient()
    post_data = reddit_client.get_comments(subreddit, post_id)

    if post_data is None:
        logger.warning(f"Post {post_id} might be deleted or unavailable.")
        return

    post_info = {
        "subreddit": subreddit,
        "post_id": post_id,
        "post_title": post_data[0]['data']['children'][0]['data'].get('title', '[Deleted Title]'),
        "post_content": post_data[0]['data']['children'][0]['data'].get('selftext', '[Deleted Content]'),
        "upvotes": post_data[0]['data']['children'][0]['data'].get('ups', 0),
        "downvotes": post_data[0]['data']['children'][0]['data'].get('downs', 0),
        "comment_count": post_data[0]['data']['children'][0]['data'].get('num_comments', 0),
        "comments": post_data[1]['data']['children'],
        "crawled_at": datetime.now()
    }

    try:
        result = posts_collection.insert_one(post_info)
        logger.info(f"Inserted post {post_id} into MongoDB with ID: {result.inserted_id}")
    except Exception as e:
        logger.error(f"Error inserting post {post_id} into MongoDB: {e}")

def crawl_subreddit(subreddit):
    reddit_client = RedditClient()
    hot_posts = reddit_client.get_hot_posts(subreddit)

    if hot_posts is None:
        logger.error(f"Failed to retrieve hot posts from {subreddit}")
        return

    with Client() as client:
        for post in hot_posts['data']['children']:
            post_id = post['data']['id']
            client.queue('crawl_post', args=(subreddit, post_id), queue='crawl_post')
            logger.info(f"Queued job to crawl post {post_id} from {subreddit}")

def start_worker():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    worker = Worker(queues=['crawl_subreddit', 'crawl_post'])
    worker.register('crawl_subreddit', crawl_subreddit)
    worker.register('crawl_post', crawl_post)
    logger.info("Worker started. Listening for jobs...")
    worker.run()



def schedule_crawl_jobs():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    subreddits = ['technology', 'movies']
    with Client() as client:
        for subreddit in subreddits:
            client.queue('crawl_subreddit', args=(subreddit,), queue='crawl_subreddit')
            logger.info(f"Queued job to crawl subreddit: {subreddit}")

def monitor_queue():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    while True:
        with Client() as client:
            info = client.info()
            total_enqueued = sum(q['size'] for q in info['queues'])
            total_in_progress = info['tasks']['active']
            logger.info(f"Jobs in queue: {total_enqueued}, Jobs in progress: {total_in_progress}")
        time.sleep(30)  # each 1 min

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
