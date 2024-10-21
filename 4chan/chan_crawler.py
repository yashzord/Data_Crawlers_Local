from chan_client import ChanClient
import logging
import pymongo
from dotenv import load_dotenv
import os
import time
from pyfaktory import Client, Consumer, Job, Producer
import multiprocessing
import datetime


# Load environment variables from .env file
load_dotenv()


MONGO_DB_URL = os.getenv("MONGO_DB_URL")
client = pymongo.MongoClient(MONGO_DB_URL)
db = client['4chan_data']
threads_collection = db['threads']


logger = logging.getLogger("ChanCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)


FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL")
BOARDS = os.getenv("BOARDS").split(',')


# Helper function to extract thread numbers from the catalog
def thread_numbers_from_catalog(catalog):
    thread_numbers = []
    for page in catalog:
        for thread in page["threads"]:
            thread_numbers.append(thread["no"])
    return thread_numbers


# Function to crawl a specific thread and store its content in MongoDB
def crawl_thread(board, thread_number):
    chan_client = ChanClient()
    thread_data = None
    try:
        thread_data = chan_client.get_thread(board, thread_number)
    except Exception as e:
        # Log network failures to a separate collection for tracking
        db['failed_requests'].insert_one({
            "board": board,
            "thread_number": thread_number,
            "error": str(e),
            "attempted_at": datetime.datetime.now()
        })
        logger.error(f"Network failure while fetching thread {board}/{thread_number}: {e}")
        return 0


    if thread_data:
        # Test Case 2.2: Deleted Thread Handling
        if "posts" not in thread_data or len(thread_data["posts"]) == 0:
            logger.warning(f"Thread {board}/{thread_number} appears to have been deleted or contains no posts.")
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {
                    "original_post": "[deleted]",
                    "replies": ["[deleted]" for _ in existing_thread.get("replies", [])],
                    "number_of_replies": 0,
                    "deleted_at": datetime.datetime.now()
                }}
            )
            logger.info(f"Marked thread {thread_number} on /{board}/ as deleted in MongoDB.")
            return 0


        # Check if the thread is already in the database
        existing_thread = threads_collection.find_one({"board": board, "thread_number": thread_number})
        op_content_changed = False
        replies_content_changed = False


        # Test Case 2.1: New Thread Detection & Test Case 2.3: New Post Detection
        if existing_thread:
            # Test Case 4.2: OP or Replies Content Modified
            # Check if the original post has changed
            if existing_thread['original_post'] != thread_data['posts'][0]:
                op_content_changed = True
                threads_collection.update_one(
                    {"board": board, "thread_number": thread_number},
                    {"$set": {"original_post": thread_data['posts'][0], "updated_at": datetime.datetime.now()}}
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
                    {"$set": {"replies": new_replies, "updated_at": datetime.datetime.now()}}
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
                "crawled_at": datetime.datetime.now()
            }


            # Test Case 2.4: Duplicate Post Handling
            # Ensure no duplicate posts are inserted
            existing_post = threads_collection.find_one({"board": board, "thread_number": thread_number})
            if not existing_post:
                result = threads_collection.insert_one(thread_info)
                logger.info(f"Inserted thread {thread_number} from /{board}/ into MongoDB with ID: {result.inserted_id}")
            else:
                logger.warning(f"Thread {thread_number} from /{board}/ is already in MongoDB. Skipping insertion.")


        # Return 1 to indicate 1 original post (thread) has been crawled
        return 1
    else:
        logger.error(f"Failed to fetch or process thread: {board}/{thread_number}")
        return 0


# Function to crawl the catalog of a board and count original posts
def crawl_board(board):
    chan_client = ChanClient()
    catalog = chan_client.get_catalog(board)


    if catalog:
        thread_numbers = thread_numbers_from_catalog(catalog)
        total_original_posts = 0  # Initialize original post count


        # Queue each thread crawl task into Faktory
        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            for thread_number in thread_numbers:
                # Queue a job for crawling each thread
                job = Job(jobtype="crawl-thread", args=(board, thread_number), queue="crawl-thread")
                producer.push(job)


        # Use Faktory to process the jobs and wait for them to complete
        logger.info(f"Queued crawl jobs for all threads on /{board}/")


        # Count total original posts (each thread is one original post)
        total_original_posts = len(thread_numbers)


        # Log the total number of original posts (threads) for the board
        logger.info(f"Total original posts crawled from /{board}/: {total_original_posts}")
    else:
        logger.error(f"Failed to retrieve catalog for board /{board}/")


# Function to continuously schedule crawl jobs for all boards every 2 minutes
def schedule_crawl_jobs_continuously(interval_minutes=2):
    crawl_count = 0  # Initialize a crawl counter
    while True:
        crawl_count += 1  # Increment the crawl counter
        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            for board in BOARDS:
                job = Job(jobtype="crawl-board", args=(board,), queue="crawl-board")
                producer.push(job)
            logger.info(f"Scheduled crawl job #{crawl_count} for all boards.")


        # Log the completion of the crawl cycle and the wait for the next iteration
        logger.info(f"Crawl #{crawl_count} finished. Waiting for {interval_minutes} minutes before the next crawl.")


        # Sleep for the specified interval (n minutes by default)
        time.sleep(interval_minutes * 60)


# Function to monitor the Faktory queue (placeholder for actual implementation)
def monitor_queue():
    logger.info("Monitor function needs specific implementation details based on the Faktory monitoring setup.")


# Function to start a Faktory worker for processing crawl jobs
def start_worker():
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-board", "crawl-thread"], concurrency=5)
        consumer.register("crawl-board", crawl_board)
        consumer.register("crawl-thread", crawl_thread)  # Register thread crawling
        logger.info("Worker started. Listening for jobs...")
        consumer.run()


# Main process that starts the worker and monitor processes and schedules recurring crawl jobs
if __name__ == "__main__":
    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()


    monitor_process = multiprocessing.Process(target=monitor_queue)
    monitor_process.start()


    # Run the crawl jobs every n minutes
    schedule_crawl_jobs_continuously(interval_minutes=1)


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
