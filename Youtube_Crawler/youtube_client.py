import os
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("YouTubeClient")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

class YouTubeClient:
    def __init__(self):
        self.api_key = os.getenv("YOUTUBE_API_KEY")
        self.base_url = "https://www.googleapis.com/youtube/v3"

    def get_channel_details(self, channel_id):
        logger.info(f"Fetching details for channel ID: {channel_id}")
        url = f"{self.base_url}/channels?part=snippet,statistics&id={channel_id}&key={self.api_key}"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error fetching channel details: {response.status_code}")
            return None
        
        data = response.json()
        if 'items' in data and data['items']:
            return data['items'][0]
        else:
            logger.warning(f"No channel details found for channel ID: {channel_id}")
            return None

    def get_channel_videos(self, channel_id, max_results=50):
        logger.info(f"Fetching videos for channel ID: {channel_id}")
        url = f"{self.base_url}/search?part=snippet&channelId={channel_id}&maxResults={max_results}&order=viewCount&type=video&key={self.api_key}"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error fetching videos: {response.status_code}")
            return None
        
        data = response.json()
        return data.get('items', [])

    def get_video_details(self, video_id):
        logger.info(f"Fetching details for video ID: {video_id}")
        url = f"{self.base_url}/videos?part=snippet,statistics&id={video_id}&key={self.api_key}"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error fetching video details: {response.status_code}")
            return None
        
        data = response.json()
        if 'items' in data and data['items']:
            return data['items'][0]
        else:
            logger.warning(f"No video details found for video ID: {video_id}")
            return None

    def get_video_comments(self, video_id, max_results=100):
        logger.info(f"Fetching comments for video ID: {video_id}")
        url = f"{self.base_url}/commentThreads?part=snippet,replies&videoId={video_id}&maxResults={max_results}&key={self.api_key}"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error fetching comments: {response.status_code}")
            return None
        
        data = response.json()
        return data.get('items', [])