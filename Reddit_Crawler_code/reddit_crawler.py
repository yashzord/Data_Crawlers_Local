import logging
import pymongo
import os
import time
from dotenv import load_dotenv
from faktory import Client, Worker
from reddit_client import RedditClient
from datetime import datetime
import multiprocessing

# Load environment variables
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL") or "mongodb://localhost:27017/"
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['reddit_data']
posts_collection = db['posts']

# Setup logger
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

    ##### ---------------------- Code to handle if the data is deleted ----------------#####
    if post_data is None or len(post_data[0]['data']['children']) == 0:
        logger.warning(f"Post {post_id} might be deleted or unavailable.")
        # If the post was already added to the database, mark it as deleted
        posts_collection.update_one(
            {"post_id": post_id},
            {"$set": {
                "post_title": "[Deleted Title]",
                "post_content": "[Deleted Content]",
                "is_deleted": True,
                "crawled_at": datetime.now()
            }},
            upsert=True
        )
        return

    # Post information from Reddit API
    post_info = {
        "subreddit": subreddit,
        "post_id": post_id,
        "post_title": post_data[0]['data']['children'][0]['data'].get('title', '[Deleted Title]'),
        "post_content": post_data[0]['data']['children'][0]['data'].get('selftext', '[Deleted Content]'),
        "upvotes": post_data[0]['data']['children'][0]['data'].get('ups', 0),
        "downvotes": post_data[0]['data']['children'][0]['data'].get('downs', 0),
        "comment_count": post_data[0]['data']['children'][0]['data'].get('num_comments', 0),
        "comments": post_data[1]['data']['children'],
        "crawled_at": datetime.now(),
        "is_deleted": False
    }

    try:
        ######################  Check if the post exists in the MongoDB database ######################
        existing_post = posts_collection.find_one({"post_id": post_id})

        if existing_post:
            # Log changes if any field has changed
            changes_detected = False
            changes = []

            # Compare fields and log changes
            if existing_post['upvotes'] != post_info['upvotes']:
                changes.append(f"Upvotes changed: {existing_post['upvotes']} -> {post_info['upvotes']}")
                changes_detected = True

            if existing_post['comment_count'] != post_info['comment_count']:
                changes.append(f"Comment count changed: {existing_post['comment_count']} -> {post_info['comment_count']}")
                changes_detected = True

            if existing_post['post_title'] != post_info['post_title']:
                changes.append(f"Title changed: {existing_post['post_title']} -> {post_info['post_title']}")
                changes_detected = True

            # Log any other fields you want to compare (e.g., content, downvotes)
            if changes_detected:
                # Update the existing post if any changes were detected
                posts_collection.update_one(
                    {"post_id": post_id},
                    {"$set": post_info}
                )
                logger.info(f"Updated post {post_id} in MongoDB. Changes: " + ", ".join(changes))
            else:
                # No changes were detected, no need to log anything
                logger.info(f"No changes detected for post {post_id}.")
        else:
            # Insert new post if it does not exist
            result = posts_collection.insert_one(post_info)
            logger.info(f"Inserted new post {post_id} into MongoDB with ID: {result.inserted_id}")

    except Exception as e:
        logger.error(f"Error inserting/updating post {post_id} into MongoDB: {e}")

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
        time.sleep(120)  # Check every 2 minutes

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
