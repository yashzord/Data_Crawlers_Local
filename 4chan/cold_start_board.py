import logging
from pyfaktory import Client, Producer, Job
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger("faktory test")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

if __name__ == "__main__":
    # Get list of boards from environment variable (e.g., g for technology, tv for movies & tv)
    boards = os.environ.get("BOARDS", "").split(",")
    
    if not boards:
        print("No boards specified. Please check your environment configuration.")
        exit(1)
    
    print(f"Cold starting catalog crawl for boards: {boards}")
    faktory_server_url = os.environ.get("FAKTORY_SERVER_URL")

    # Push jobs for each board
    with Client(faktory_url=faktory_server_url, role="producer") as client:
        producer = Producer(client=client)
        
        for board in boards:
            job = Job(jobtype="crawl-catalog", args=(board,), queue="crawl-catalog")
            producer.push(job)
            print(f"Enqueued crawl job for board: {board}")
