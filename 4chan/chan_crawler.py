from chan_client import ChanClient
import logging
import pymongo
from dotenv import load_dotenv
import os
import time
from pyfaktory import Client, Consumer, Job, Producer
import multiprocessing
import datetime

# Load environment variables
load_dotenv()

# MongoDB connection setup
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
client = pymongo.MongoClient(MONGO_DB_URL)
db = client['4chan_data']  # Database name
threads_collection = db['threads']  # Collection name

# Logger setup
logger = logging.getLogger("ChanCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL")
BOARDS = os.getenv("BOARDS").split(',')

def thread_numbers_from_catalog(catalog):
    """
    Extract thread numbers from catalog JSON object.
    """
    thread_numbers = []
    for page in catalog:
        for thread in page["threads"]:
            thread_numbers.append(thread["no"])
    return thread_numbers

def crawl_thread(board, thread_number):
    """
    Crawl a thread and save its details into MongoDB.
    """
    chan_client = ChanClient()
    thread_data = chan_client.get_thread(board, thread_number)
    if thread_data:
        thread_info = {
            "board": board,
            "thread_number": thread_number,
            "original_post": thread_data["posts"][0],  # Assuming the first post is the original
            "replies": thread_data["posts"][1:],  # The rest are replies
            "number_of_replies": len(thread_data["posts"]) - 1,
            "crawled_at": datetime.datetime.now()
        }

        result = threads_collection.insert_one(thread_info)
        logger.info(f"Inserted thread {thread_number} from /{board}/ into MongoDB with ID: {result.inserted_id}")
    else:
        logger.error(f"Failed to fetch or process thread: {board}/{thread_number}")

def crawl_board(board):
    """
    Crawl all threads in a board's catalog and queue each thread for crawling.
    """
    chan_client = ChanClient()
    catalog = chan_client.get_catalog(board)
    if catalog:
        thread_numbers = thread_numbers_from_catalog(catalog)
        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            for thread_number in thread_numbers:
                job = Job(jobtype="crawl-thread", args=(board, thread_number), queue="crawl-thread")
                producer.push(job)
            logger.info(f"Queued crawl jobs for board: /{board}/")
    else:
        logger.error(f"Failed to retrieve catalog for board /{board}/")

def schedule_crawl_jobs():
    """
    Schedule crawling jobs for all configured boards.
    """
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for board in BOARDS:
            job = Job(jobtype="crawl-board", args=(board,), queue="crawl-board")
            producer.push(job)
        logger.info("Scheduled initial crawl jobs for all boards.")

def monitor_queue():
    """
    Periodically log the status of the queue.
    """
    # This function needs a specific monitoring setup that pyfaktory might not support directly
    logger.info("Monitor function needs specific implementation details based on the Faktory monitoring setup.")

def start_worker():
    """
    Start the Faktory worker to process queued jobs.
    """
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-board", "crawl-thread"], concurrency=5)
        consumer.register("crawl-board", crawl_board)
        consumer.register("crawl-thread", crawl_thread)
        logger.info("Worker started. Listening for jobs...")
        consumer.run()

if __name__ == "__main__":
    # Starting all processes
    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()

    monitor_process = multiprocessing.Process(target=monitor_queue)
    monitor_process.start()

    schedule_crawl_jobs()

    try:
        worker_process.join()
        monitor_process.join()
    except KeyboardInterrupt:
        logger.info("Stopping processes...")
        worker_process.terminate()
        monitor_process.terminate()
        worker_process.join()
        monitor_process.join()
        logger.info("Processes stopped.")
