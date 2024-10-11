from fourchan_client import FourChanClient
import json
import faktory

client = FourChanClient()

boards = ['g', 'tv']  # 'g' for Technology, 'tv' for Movies & TV

def handle_thread(board, thread):
    data = client.fetch_thread(board, thread['no'])
    # Push each post to Faktory for further processing (e.g., storing in MongoDB)
    with faktory.connection() as f:
        for post in data.get('posts', []):
            f.queue('store_post', args=[json.dumps(post), board])

def crawl_board(board):
    threads_info = client.fetch_threads(board)
    for page in threads_info:
        for thread in page['threads']:
            handle_thread(board, thread)

if __name__ == '__main__':
    for board in boards:
        crawl_board(board)
