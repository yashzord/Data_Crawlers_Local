import matplotlib.pyplot as plt
import pymongo
from dotenv import load_dotenv
import os
from datetime import datetime
import random

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
client = pymongo.MongoClient(MONGO_DB_URL)
db = client['4chan_data']
threads_collection = db['threads']

PLOT_DIR = r"C:\Users\yashu\OneDrive\Desktop\Chan_Plots"

if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

def get_all_data():
    threads = list(threads_collection.find())
    if threads:
        print(f"Retrieved {len(threads)} threads from the database.")
    else:
        print("No data retrieved from the database.")
    return threads

def plot_documents_collected_over_time(threads):
    times = []
    doc_counts = []

    for i, thread in enumerate(threads):
        times.append(thread['crawled_at'])  
        doc_counts.append(i + 1)  

    times, doc_counts = zip(*sorted(zip(times, doc_counts)))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    plt.figure(figsize=(10, 6))
    plt.plot(times, doc_counts, marker='o', color='green', label='Documents / Threads Collected')
    plt.title('Cumulative Number of Documents / Threads (Active + Deleted) Collected Over Crawled Time Periods')
    plt.xlabel('Time (Crawled At)')
    plt.ylabel('Number of Documents')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()

    random_points = random.sample(range(len(times)), min(5, len(times))) 
    for i in random_points:
        plt.text(times[i], doc_counts[i], f"({times[i].strftime('%H:%M')}, {doc_counts[i]})", 
                 fontsize=9, ha='right', color='blue')

    doc_plot_path = os.path.join(PLOT_DIR, f'documents_collected_over_time_{timestamp}.png')
    plt.savefig(doc_plot_path)
    plt.show()

    print(f"Documents collected plot saved at {doc_plot_path}")

if __name__ == "__main__":
    threads = get_all_data()
    if threads:
        plot_documents_collected_over_time(threads)
