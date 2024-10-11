import faktory
import pymongo
import json

def store_post(post_data, board):
    post = json.loads(post_data)
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fourchan"]
    collection = db[board]
    collection.insert_one(post)

if __name__ == '__main__':
    with faktory.connection() as client:
        while True:
            with client.fetch(queue='default') as job:
                if job:
                    store_post(*job['args'])
                    job.ack()
