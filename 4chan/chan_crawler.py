import logging
import pymongo
from dotenv import load_dotenv
import os
import time
from pyfaktory import Client, Producer, Consumer, Job
import multiprocessing
import datetime
from chan_client import ChanClient
from requests.exceptions import HTTPError, RequestException

# Load environment variables
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
client = pymongo.MongoClient(MONGO_DB_URL)
db = client['4chan_data']
threads_collection = db['threads']

# Setup logger
logger = logging.getLogger("ChanCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL")
BOARDS = os.getenv("BOARDS").split(',')

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
            if status_code == 404:
                logger.warning(f"Resource not found (404). Thread {args[1]} might be deleted.")
                return None
            elif 400 <= status_code < 500:
                logger.error(f"Client error (status {status_code}) occurred for thread {args[1]}. Retrying in {delay} seconds...")
            elif 500 <= status_code < 600:
                logger.error(f"Server error (status {status_code}) occurred. Retrying in {delay} seconds...")
            else:
                logger.error(f"Unexpected HTTP error: {http_err}")
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

def thread_numbers_from_catalog(catalog):
    thread_numbers = []
    for page in catalog:
        for thread in page["threads"]:
            thread_numbers.append(thread["no"])
    return thread_numbers

def crawl_thread(board, thread_number):
    chan_client = ChanClient()
    logger.info(f"Fetching thread {board}/{thread_number}...")  # Log initial fetch attempt

    # Fetch thread data with retries
    thread_data = retry_on_network_and_http_errors(chan_client.get_thread, board, thread_number)
    
    if thread_data is None:
        logger.warning(f"Thread {thread_number} might be deleted or unavailable.")
        # Fetch existing thread for archival details (if it was previously stored)
        existing_thread = threads_collection.find_one({"board": board, "thread_number": thread_number})
        
        if existing_thread:
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {
                    "$set": {
                        "original_post.com": "[deleted]",
                        "replies": [{**reply, "com": "[deleted]"} for reply in existing_thread.get("replies", [])],
                        "number_of_replies": 0,
                        "deleted_at": datetime.datetime.now(),
                        "previous_title": existing_thread.get("original_post", {}).get("com", "N/A"),
                        "previous_replies": existing_thread.get("replies", []),
                        "is_deleted": True
                    }
                },
                upsert=True
            )
            logger.info(f"Thread {thread_number} on /{board}/ has been marked as deleted in MongoDB.")
        else:
            logger.info(f"No existing data found for thread {thread_number} on /{board}/ to mark as deleted.")

        return 0
    else:
        logger.info(f"Successfully fetched thread {board}/{thread_number}.")

    # Check if the thread is already in the database
    existing_thread = threads_collection.find_one({"board": board, "thread_number": thread_number})
    op_content_changed = False
    replies_content_changed = False

    if existing_thread:
        # Check if the original post has changed
        if existing_thread['original_post'] != thread_data['posts'][0]:
            op_content_changed = True
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {"original_post": thread_data['posts'][0], "updated_at": datetime.datetime.now(), "is_deleted": False}}
            )
            logger.info(f"Updated original post content for thread {thread_number} on /{board}/.")

        # Check for changes in replies
        existing_replies = existing_thread.get("replies", [])
        new_replies = thread_data["posts"][1:]
        if len(existing_replies) == len(new_replies):
            for i in range(len(existing_replies)):
                if existing_replies[i] != new_replies[i]:
                    replies_content_changed = True
                    break

        if replies_content_changed:
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {"replies": new_replies, "updated_at": datetime.datetime.now(), "is_deleted": False}}
            )
            logger.info(f"Updated replies content for thread {thread_number} on /{board}/.")
        existing_replies_count = existing_thread.get("number_of_replies", 0)
        new_replies_count = len(thread_data["posts"]) - 1

        if new_replies_count > existing_replies_count:
            # Update with new replies
            new_replies = thread_data["posts"][existing_replies_count + 1:]
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$push": {"replies": {"$each": new_replies}}, "$set": {"number_of_replies": new_replies_count, "updated_at": datetime.datetime.now()}}
            )
            logger.info(f"Updated thread {thread_number} on /{board}/ with {new_replies_count - existing_replies_count} new replies.")
        elif new_replies_count < existing_replies_count:
            # Handle case where some replies are deleted
            updated_replies = thread_data["posts"][1:]  # All posts except OP
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {
                    "replies": updated_replies,
                    "number_of_replies": new_replies_count,
                    "updated_at": datetime.datetime.now()
                }}
            )
            logger.info(f"Updated thread {thread_number} on /{board}/ to reflect deleted replies.")
            logger.info(f"Thread {thread_number} on /{board}/ has had replies deleted and updated in MongoDB.")
        else:
            logger.info(f"No new posts detected for thread {thread_number} on /{board}/.")
    else:
        # Insert new thread
        thread_info = {
            "board": board,
            "thread_number": thread_number,
            "original_post": thread_data["posts"][0],  # First post is the OP
            "replies": thread_data["posts"][1:],  # All remaining posts are replies
            "number_of_replies": len(thread_data["posts"]) - 1,  # Replies count
            "crawled_at": datetime.datetime.now(),
            "is_deleted": False
        }
        result = threads_collection.insert_one(thread_info)
        logger.info(f"Inserted thread {thread_number} from /{board}/ into MongoDB with ID: {result.inserted_id}")

    return 1

def crawl_board(board):
    chan_client = ChanClient()
    catalog = retry_on_network_and_http_errors(chan_client.get_catalog, board)

    if catalog is None:
        logger.error(f"Failed to retrieve catalog for board /{board}/")
        return

    thread_numbers = thread_numbers_from_catalog(catalog)
    total_original_posts = len(thread_numbers)

    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for thread_number in thread_numbers:
            job = Job(jobtype="crawl-thread", args=(board, thread_number), queue="crawl-thread")
            producer.push(job)

    logger.info(f"Queued crawl jobs for all threads on /{board}/")
    logger.info(f"Total original posts crawled from /{board}/: {total_original_posts}")

def schedule_crawl_jobs_continuously(interval_minutes=2):
    crawl_count = 0
    while True:
        crawl_count += 1
        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            for board in BOARDS:
                job = Job(jobtype="crawl-board", args=(board,), queue="crawl-board")
                producer.push(job)
            logger.info(f"Scheduled crawl job #{crawl_count} for all boards.")

        logger.info(f"Crawl #{crawl_count} finished. Waiting for {interval_minutes} minutes before the next crawl.")
        time.sleep(interval_minutes * 60)

def start_worker():
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-board", "crawl-thread"], concurrency=5)
        consumer.register("crawl-board", crawl_board)
        consumer.register("crawl-thread", crawl_thread)
        logger.info("Worker started. Listening for jobs...")
        consumer.run()

if __name__ == "__main__":
    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()

    schedule_crawl_jobs_continuously(interval_minutes=1)

    try:
        worker_process.join()
    except KeyboardInterrupt:
        logger.info("Stopping processes...")
        worker_process.terminate()
        worker_process.join()
        logger.info("Processes stopped.")
