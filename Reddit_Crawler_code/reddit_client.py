import logging
import requests
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("Reddit client")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

class RedditClient:
    API_BASE = "https://oauth.reddit.com"

    def __init__(self):
        self.client_id = os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.access_token = self.get_access_token()

    def get_access_token(self):
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        data = {"grant_type": "client_credentials"}
        headers = {"User-Agent": "RedditClient/0.1"}
        response = requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=data, headers=headers)
        response.raise_for_status()
        return response.json()["access_token"]

    def execute_request(self, endpoint):
        headers = {"Authorization": f"bearer {self.access_token}", "User-Agent": "RedditClient/0.1"}
        url = f"{self.API_BASE}{endpoint}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Error fetching data: {response.status_code}")
            return None
        return response.json()

    def get_hot_posts(self, subreddit, limit=100):
        endpoint = f"/r/{subreddit}/hot?limit={limit}"
        logger.info(f"Fetching hot posts from {subreddit}")
        return self.execute_request(endpoint)

    def get_comments(self, subreddit, post_id):
        endpoint = f"/r/{subreddit}/comments/{post_id}"
        logger.info(f"Fetching comments for post {post_id} from {subreddit}")
        return self.execute_request(endpoint)
