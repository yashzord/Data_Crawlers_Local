import logging
import requests
import time
from requests.exceptions import HTTPError, RequestException

# Configure logging to help with debugging and information output to the console.
logger = logging.getLogger("4chan client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

MAX_RETRIES = 5
RETRY_DELAY = 5

class ChanClient:
    API_BASE = "http://a.4cdn.org"

    def __init__(self):
        self.last_modified_times = {}

    def execute_request(self, api_call, headers={}, retries=MAX_RETRIES, backoff_factor=RETRY_DELAY):
        """
        Execute an HTTP GET request with retries and error handling for various HTTP status codes.
        """
        for attempt in range(retries):
            try:
                response = requests.get(api_call, headers=headers)
                if response.status_code == 304:
                    logger.info("No new data since last modified.")
                    return None
                
                response.raise_for_status()

                # Save the last modified time if present to use in subsequent requests.
                if 'Last-Modified' in response.headers:
                    self.last_modified_times[api_call] = response.headers['Last-Modified']

                return response.json()
            except HTTPError as http_err:
                status_code = response.status_code
                if status_code == 404:
                    logger.warning(f"Resource not found (404): {api_call}")
                    # Return None to signify that the resource is not found or deleted.
                    return None
                elif 400 <= status_code < 500:
                    logger.error(f"Client error occurred: {status_code} on {api_call}. Retrying in {backoff_factor} seconds...")
                    time.sleep(backoff_factor)
                elif 500 <= status_code < 600:
                    logger.error(f"Server error occurred: {status_code} on {api_call}. Retrying in {backoff_factor} seconds...")
                    time.sleep(backoff_factor)
                    backoff_factor *= 2
                else:
                    logger.error(f"Unexpected HTTP error: {http_err}")
                    return None
            except RequestException as req_err:
                logger.error(f"Network error: {req_err}. Retrying in {backoff_factor} seconds...")
                time.sleep(backoff_factor)
                backoff_factor *= 2

        logger.error(f"Max retries reached. Failed to execute request after {retries} attempts: {api_call}")
        return None

    def get_threads(self, board):
        api_call = f"{self.API_BASE}/{board}/threads.json"
        headers = {}
        if board in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[board]

        return self.execute_request(api_call, headers)

    def get_thread(self, board, thread_number):
        api_call = f"{self.API_BASE}/{board}/thread/{thread_number}.json"
        headers = {}
        if api_call in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[api_call]

        return self.execute_request(api_call, headers)

    def get_catalog(self, board):
        api_call = f"{self.API_BASE}/{board}/catalog.json"
        headers = {}
        if board in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[board]

        return self.execute_request(api_call, headers)
