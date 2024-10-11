import requests

class FourChanClient:
    BASE_URL = "https://a.4cdn.org"
    
    def fetch_threads(self, board):
        response = requests.get(f"{self.BASE_URL}/{board}/threads.json")
        response.raise_for_status()
        data = response.json()
        return data

    def fetch_thread(self, board, thread_id):
        response = requests.get(f"{self.BASE_URL}/{board}/thread/{thread_id}.json")
        response.raise_for_status()
        data = response.json()
        return data
