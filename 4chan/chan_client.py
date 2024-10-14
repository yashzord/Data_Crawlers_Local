import logging
import requests
import time

# Logger setup
logger = logging.getLogger("4chan client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

class ChanClient:
    API_BASE = "http://a.4cdn.org"
    
    def __init__(self):
        self.last_modified_times = {}  # Store last modified times for each board/thread
    
    def get_threads(self, board):
        """
        List all threads on a specific board.
        Uses the If-Modified-Since header to reduce redundant requests.
        """
        api_call = f"{self.API_BASE}/{board}/threads.json"
        headers = {}
        if board in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[board]
        
        return self.execute_request(api_call, headers)

    def get_thread(self, board, thread_number):
        """
        Get all posts within a specific thread.
        """
        api_call = f"{self.API_BASE}/{board}/thread/{thread_number}.json"
        return self.execute_request(api_call)
    
    def get_catalog(self, board):
        """
        Get thread catalog for a specific board.
        """
        api_call = f"{self.API_BASE}/{board}/catalog.json"
        headers = {}
        if board in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[board]
        
        return self.execute_request(api_call, headers)

    def execute_request(self, api_call, headers={}, retries=3, backoff_factor=0.3):
        """
        Executes an HTTP request and returns the JSON response.
        Handles retries and uses If-Modified-Since to avoid redundant requests.
        """
        for attempt in range(retries):
            try:
                response = requests.get(api_call, headers=headers)
                if response.status_code == 304:
                    logger.info(f"No new data for API call: {api_call}")
                    return None  # No new data
                response.raise_for_status()  # Raise an exception for bad HTTP responses

                # Update last modified times if available
                if 'Last-Modified' in response.headers:
                    self.last_modified_times[api_call] = response.headers['Last-Modified']

                logger.info(f"Successful API call: {api_call}")
                return response.json()
            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor * (2 ** attempt)
                logger.error(f"Request failed: {e}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
        
        logger.error(f"Failed after {retries} retries: {api_call}")
        return None
