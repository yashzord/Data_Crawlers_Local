import logging
import requests
import time


# Configure logging to help with debugging and information output to the console.
logger = logging.getLogger("4chan client")
logger.propagate = False  # Prevent log messages from propagating to the higher-level root logger.
logger.setLevel(logging.INFO)  # Set the log level to INFO to capture routine logging information.
sh = logging.StreamHandler()  # Create a stream handler to output logs to the console.
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)  # Set the format for log messages to include time, logger name, level, and message.
logger.addHandler(sh)  # Attach the stream handler to the logger.


class ChanClient:
    API_BASE = "http://a.4cdn.org"  # Base URL for accessing the 4chan API.


    def __init__(self):
        # Dictionary to store the last modified times of API responses to optimize request headers.
        self.last_modified_times = {}
   
    def get_threads(self, board):
        """
        Fetch all threads for a specified board using the 4chan API.
        Uses 'If-Modified-Since' to reduce unnecessary data transfer.
        """
        api_call = f"{self.API_BASE}/{board}/threads.json"  # Construct API endpoint URL for threads.
        headers = {}
        # Add 'If-Modified-Since' header if a last modified time exists for the board to avoid redundant data transfer.
        if board in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[board]
       
        return self.execute_request(api_call, headers)  # Perform the API request and return the JSON response.


    def get_thread(self, board, thread_number):
        """
        Retrieve all posts within a specific thread on a board.
        """
        api_call = f"{self.API_BASE}/{board}/thread/{thread_number}.json"  # Construct API endpoint URL for a specific thread.
        return self.execute_request(api_call)  # Perform the API request and return the JSON response.


    def get_catalog(self, board):
        """
        Fetch the catalog of threads for a specific board.
        Uses 'If-Modified-Since' to minimize data transfer.
        """
        api_call = f"{self.API_BASE}/{board}/catalog.json"  # Construct API endpoint URL for the catalog.
        headers = {}
        # Add 'If-Modified-Since' header if a last modified time exists for the board.
        if board in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[board]
       
        return self.execute_request(api_call, headers)  # Perform the API request and return the JSON response.


    def execute_request(self, api_call, headers={}, retries=3, backoff_factor=0.3):
        """
        Execute an HTTP GET request with retries and error handling for various HTTP status codes.
        """
        for attempt in range(retries):
            try:
                response = requests.get(api_call, headers=headers)  # Send the HTTP GET request.
                if response.status_code == 304:
                    logger.info("No new data since last modified.")
                    return None  # Return None if no updates (304 Not Modified).
                response.raise_for_status()  # Raise an exception if the response contains a HTTP error status.


                # Save the last modified time if present to use in subsequent requests.
                if 'Last-Modified' in response.headers:
                    self.last_modified_times[api_call] = response.headers['Last-Modified']


                return response.json()  # Return the parsed JSON data from the response.
            except requests.exceptions.HTTPError as e:
                if response.status_code == 404:
                    logger.error(f"Resource not found: {api_call}")
                    return None  # Return None if the resource is not found (404 Not Found).
                elif response.status_code == 429:
                    # Handle rate limiting by waiting before the next request attempt.
                    wait_time = backoff_factor * (2 ** attempt)
                    logger.error(f"Rate limit exceeded. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue  # Continue to retry after the wait.
                else:
                    logger.error(f"HTTP error occurred: {e}")  # Log other types of HTTP errors.
                    return None
            except requests.exceptions.RequestException as e:
                # Handle other types of requests exceptions like network problems.
                wait_time = backoff_factor * (2 ** attempt)
                logger.error(f"Request failed: {e}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)  # Wait before retrying.


        logger.error(f"Failed after {retries} retries: {api_call}")
        return None  # Return None if all retries fail.


