import logging
import pymongo
import os
import time
from dotenv import load_dotenv
from faktory import Client, Worker
from chan_client import ChanClient
from datetime import datetime
import multiprocessing

# Load environment variables
load_dotenv()

# MongoDB connection setup
MONGO_DB_URL = os.getenv("MONGO_DB_URL") or "mongodb://localhost:27017/"
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['4chan_data']  # Database name
threads_collection = db['threads']  # Collection for threads and their engagement data

# Logger setup
logger = logging.getLogger("4chan_crawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

# Faktory server URL from .env
FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL") or 'tcp://:your_password@localhost:7419'

def crawl_thread(board, thread_number):
    """
    Crawl a given thread and insert posts and engagement data into MongoDB.
    """
    chan_client = ChanClient()
    thread_data = chan_client.get_thread(board, thread_number)

    if not thread_data or "posts" not in thread_data:
        logger.warning(f"Thread {thread_number} might be deleted or unavailable.")
        return

    original_post = thread_data["posts"][0]  # First post is the OP
    replies = thread_data["posts"][1:]  # All subsequent posts are replies

    # Gather engagement data
    num_replies = len(replies)
    thread_length = len(thread_data["posts"])

    thread_info = {
        "board": board,
        "thread_number": thread_number,
        "original_post": original_post,
        "num_replies": num_replies,
        "thread_length": thread_length,
        "crawled_at": datetime.now()
    }

    try:
        result = threads_collection.insert_one(thread_info)
        logger.info(f"Inserted thread {thread_number} into MongoDB with ID: {result.inserted_id}")
    except pymongo.errors.DuplicateKeyError:
        logger.info(f"Duplicate thread {thread_number} skipped.")

def crawl_catalog(board):
    """
    Crawl the catalog for a given board, enqueue jobs for each thread.
    """
    chan_client = ChanClient()
    catalog_data = chan_client.get_catalog(board)

    if not catalog_data:
        logger.error(f"Failed to retrieve catalog for board {board}")
        return

    thread_numbers = []
    for page in catalog_data:
        for thread in page.get("threads", []):
            thread_numbers.append(thread["no"])

    with Client(faktory_url=FAKTORY_SERVER_URL) as client:
        for thread_number in thread_numbers:
            client.queue('crawl_thread', args=(board, thread_number), queue='crawl_thread')
            logger.info(f"Queued job to crawl thread {thread_number} from board {board}")

def start_worker():
    """
    Start the Faktory worker to consume jobs from Faktory queues.
    """
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    worker = Worker(queues=['crawl_catalog', 'crawl_thread'])
    worker.register('crawl_catalog', crawl_catalog)
    worker.register('crawl_thread', crawl_thread)
    logger.info("Worker started. Listening for jobs...")
    worker.run()

def schedule_crawl_jobs():
    """
    Schedule jobs to crawl 4chan boards.
    """
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    boards = ['g', 'tv']  # Add other boards as needed
    with Client() as client:
        for board in boards:
            client.queue('crawl_catalog', args=(board,), queue='crawl_catalog')
            logger.info(f"Queued job to crawl catalog for board: {board}")

def monitor_queue():
    """
    Monitor the Faktory queue for job statuses.
    """
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    while True:
        with Client() as client:
            info = client.info()
            total_enqueued = sum(q['size'] for q in info.get('queues', []))
            total_in_progress = info.get('tasks', {}).get('active', 0)
            logger.info(f"Jobs in queue: {total_enqueued}, Jobs in progress: {total_in_progress}")
        time.sleep(30)

if __name__ == "__main__":
    # Start worker and queue monitoring processes
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
