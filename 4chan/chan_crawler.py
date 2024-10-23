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

# Loading all environment variables from the .env file
load_dotenv()

# Setting up the connection with MongoDB
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
client = pymongo.MongoClient(MONGO_DB_URL)
# My database created will be called 4chan_data and the collection is called threads.
db = client['4chan_data']
threads_collection = db['threads']

# Logging to help with debugging
logger = logging.getLogger("ChanCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

# Retrieving Faktory URL and Dynamically reading the Boards to Crawl from .env file.
FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL")
BOARDS = os.getenv("BOARDS").split(',')

# Constants used when retrying incase of http errors
MAX_RETRIES = 5
RETRY_DELAY = 5

# Error handling incase of HTTP or network errors, same as the error handling in execute_request in chan_client.
def retry_on_network_and_http_errors(func, *args):
    retries = 0
    delay = RETRY_DELAY
    while retries < MAX_RETRIES:
        try:
            return func(*args)
        except HTTPError as http_err:
            status_code = http_err.response.status_code
            # Case when thread maybe deleted or fell into archive board or resource not found in general
            if status_code == 404:
                logger.warning(f"Resource not found (404). Thread {args[1]} might be deleted.")
                return None
            # Case when Client Side Error
            elif 400 <= status_code < 500:
                logger.error(f"Client error (status {status_code}) occurred for thread {args[1]}. Retrying in {delay} seconds...")
            # Case when Server Side Error
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

# Gets all the thread numbers of a catalog, Used in crawl_board function.
def thread_numbers_from_catalog(catalog):
    thread_numbers = []
    for page in catalog:
        for thread in page["threads"]:
            thread_numbers.append(thread["no"])
    return thread_numbers

# For a specific board, it fetches all the exisiting threads from the database.
def get_existing_thread_ids_from_db(board):

    return [thread["thread_number"] for thread in threads_collection.find({"board": board})]

# Compares newly crawled threads with the ones already exisiting in the database.
# Returns the difference which is the threads missing from current crawl so might be deleted.
def find_deleted_threads(previous_thread_numbers, current_thread_numbers):

    return set(previous_thread_numbers) - set(current_thread_numbers)

# Test Case - Handles Missing Values/Threads by replacing it with "Deleted" string and more....
# Marks a thread as deleted in MongoDB while keeping previous context.
def mark_thread_as_deleted(board, thread_number):

    logger.info(f"Marking thread {thread_number} on /{board}/ as deleted.")

    # We get the exisiting thread to collect context about that specific thread.
    existing_thread = threads_collection.find_one({"board": board, "thread_number": thread_number})

    if existing_thread:
        # Collecting history of a thread for further context after deleting.
        history_entry = {
            "timestamp": datetime.datetime.now(),
            "original_post": existing_thread.get("original_post", {}),
            "replies": existing_thread.get("replies", []),
            "number_of_replies": existing_thread.get("number_of_replies", 0)
        }

        # We actually mark the thread as deleted / updates the Database.
        # We also add History - previous context to modify json structure after Deletion.
        threads_collection.update_one(
            {"board": board, "thread_number": thread_number},
            {
                "$set": {
                    "original_post.com": "[deleted]",
                    "replies": [{**reply, "com": "[deleted]"} for reply in existing_thread.get("replies", [])],
                    "number_of_replies": 0,
                    "deleted_at": datetime.datetime.now(),
                    "is_deleted": True
                },
                "$push": {
                    "history": history_entry
                }
            }
        )
        logger.info(f"Thread {thread_number} on /{board}/ has been marked as deleted in MongoDB and historical data / previous context has been recorded.")
    else:
        logger.info(f"No existing data found for thread {thread_number} on /{board}/ to mark as deleted.")

# Function to Crawl a Single thread, Uses get_thread from chan_client which has the API endpoint for a specific Thread.
# Handles Deleted/Archived Data, Duplicate Data, Actual String content Changes for OP and Replies.
# Handles Number of replies, Added replies, Deleted Replies, Inserting a thread into DB.
def crawl_thread(board, thread_number):
    chan_client = ChanClient()
    logger.info(f"Fetching thread {board}/{thread_number}...")

    # Getting the thread data for a speciifc thread after running Http and Network Errors on it beforehand.
    thread_data = retry_on_network_and_http_errors(chan_client.get_thread, board, thread_number)
    
    # If a thread is deleted or archived or not found.
    if thread_data is None:
        logger.warning(f"Thread {thread_number} might be deleted or unavailable.")
        
        existing_thread = threads_collection.find_one({"board": board, "thread_number": thread_number})
        
        if existing_thread:
            # Updating existing thread to mark it as deleted
            mark_thread_as_deleted(board, thread_number)
        else:
            logger.info(f"No existing data found for thread {thread_number} on /{board}/ to mark as deleted.")

        return 0
    else:
        logger.info(f"Successfully fetched thread {board}/{thread_number}.")

    # Checking if the thread is already in the database
    existing_thread = threads_collection.find_one({"board": board, "thread_number": thread_number})
    # if original post and replies string content changed.
    op_content_changed = False
    replies_content_changed = False

    if existing_thread:
        # Checking if the original post has changed compared to the database.
        if existing_thread['original_post'] != thread_data['posts'][0]:
            op_content_changed = True
            # Updating the Json for a thread accordingly if Original Post Content changed.
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {"original_post": thread_data['posts'][0], "updated_at": datetime.datetime.now(), "is_deleted": False}}
            )
            logger.info(f"Updated original post content for thread {thread_number} on /{board}/.")

        # Checking if replies have changed
        existing_replies = existing_thread.get("replies", [])
        # [1:] -> Indicates all posts except first one which is original post.
        new_replies = thread_data["posts"][1:]
        if len(existing_replies) == len(new_replies):
            for i in range(len(existing_replies)):
                if existing_replies[i] != new_replies[i]:
                    replies_content_changed = True
                    break

        # If replies changed, update json accordingly in the database.
        if replies_content_changed:
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {"replies": new_replies, "updated_at": datetime.datetime.now(), "is_deleted": False}}
            )
            logger.info(f"Updated replies content for thread {thread_number} on /{board}/.")

        existing_replies_count = existing_thread.get("number_of_replies", 0)
        new_replies_count = len(thread_data["posts"]) - 1

        # If at all new replies are added in latest crawl when compared to previous crawl
        # Update the count and content of replies in the database accordingly by pushing/adding.
        if new_replies_count > existing_replies_count:
            new_replies = thread_data["posts"][existing_replies_count + 1:]
            threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$push": {"replies": {"$each": new_replies}}, "$set": {"number_of_replies": new_replies_count, "updated_at": datetime.datetime.now()}}
            )
            logger.info(f"Updated thread {thread_number} on /{board}/ with {new_replies_count - existing_replies_count} new replies.")

        # If at all new replies are deleted in latest crawl when compared to previous crawl
        # Update the count and content of replies in the database accordingly by removing/deleting.
        elif new_replies_count < existing_replies_count:
            updated_replies = thread_data["posts"][1:]
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
        # Normal case of inserting a thread into DB for the first time.
        thread_info = {
            "board": board,
            "thread_number": thread_number,
            "original_post": thread_data["posts"][0],
            "replies": thread_data["posts"][1:],
            "number_of_replies": len(thread_data["posts"]) - 1,
            "crawled_at": datetime.datetime.now(),
            "is_deleted": False
        }
        result = threads_collection.insert_one(thread_info)
        logger.info(f"Inserted thread {thread_number} from /{board}/ into MongoDB with ID: {result.inserted_id}")

    return 1

# Crawls a specific board, uses get_catalog from chan_client to list all active threads in a specific board.
def crawl_board(board):
    chan_client = ChanClient()
    catalog = retry_on_network_and_http_errors(chan_client.get_catalog, board)

    if catalog is None:
        logger.error(f"Failed to retrieve catalog for board /{board}/")
        return

    # Latest Crawl Thread Numbers which are active thread numbers in a specific board.
    current_thread_numbers = thread_numbers_from_catalog(catalog)
    total_original_posts = len(current_thread_numbers)

    # Fetching existing thread numbers from the database
    previous_thread_numbers = get_existing_thread_ids_from_db(board)

    # Using the find deleted thread defined above.
    # Handling Deleted Threads again just to make sure.
    deleted_threads = find_deleted_threads(previous_thread_numbers, current_thread_numbers)
    if deleted_threads:
        logger.info(f"Found {len(deleted_threads)} deleted threads on /{board}/: {deleted_threads}")
        for thread_number in deleted_threads:
            mark_thread_as_deleted(board, thread_number)

    # Queueing Jobs for crawl thread in faktory (Enqueued)
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for thread_number in current_thread_numbers:
            job = Job(jobtype="crawl-thread", args=(board, thread_number), queue="crawl-thread")
            producer.push(job)

    logger.info(f"Queued crawl jobs for all threads on /{board}/")
    logger.info(f"Total original posts crawled from /{board}/: {total_original_posts}")

# Schedules the Crawl after every specific interval. In our case it should be 6 hours.
def schedule_crawl_jobs_continuously(interval_minutes=360):
    # Keeps track of which crawl we are currently performing.
    crawl_count = 0
    while True:
        crawl_count += 1
        # Enqueues Job's for crawl-board (so 2 jobs as 1 for g and 1 for tv)
        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            for board in BOARDS:
                job = Job(jobtype="crawl-board", args=(board,), queue="crawl-board")
                producer.push(job)
            logger.info(f"Scheduled crawl job #{crawl_count} for all boards.")

        logger.info(f"Crawl #{crawl_count} finished. Waiting for {interval_minutes} minutes before the next crawl.")
        # Sleeps after every crawl, in our case it should be 6 hrs = 360 mins = 21600 s
        time.sleep(interval_minutes * 60)

# We Produced a job for crawl thread and crawl board and here we consume those jobs to be in sync.
# Producer-Consumer Model.
def start_worker():
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-board", "crawl-thread"], concurrency=5)
        consumer.register("crawl-board", crawl_board)
        consumer.register("crawl-thread", crawl_thread)
        logger.info("Worker started. Listening for jobs...")
        consumer.run()

# We multiprocess the start worker to run in parallel
# We call schedule_crawl_jobs_continuously here to start the crawls.
# We dont stop it/Interrupt the crawler till the end of the class.
# We specify the minutes to wait before crawling after the first crawl for subsequent crawl-> 360 mins 
if __name__ == "__main__":
    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()

    schedule_crawl_jobs_continuously(interval_minutes=2)

    try:
        worker_process.join()
    except KeyboardInterrupt:
        logger.info("Stopping processes...")
        worker_process.terminate()
        worker_process.join()
        logger.info("Processes stopped.")
