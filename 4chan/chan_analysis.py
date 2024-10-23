import matplotlib.pyplot as plt
import pymongo
import pandas as pd
from bson import BSON
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get MongoDB URL from environment variables
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

# Connect to MongoDB
client = pymongo.MongoClient(MONGO_DB_URL)
db = client['4chan_data']
threads_collection = db['threads']

# Directory to save all plots (e.g., C:\Users\yashu\OneDrive\Desktop\Chan_Plots)
PLOT_DIR = r"C:\Users\yashu\OneDrive\Desktop\Chan_Plots"

# Ensure the directory exists
if not os.path.exists(PLOT_DIR):
    os.makedirs(PLOT_DIR)

# Fetch all collected threads from the database
def get_all_data():
    threads = list(threads_collection.find())
    if threads:
        print(f"Retrieved {len(threads)} threads from the database.")
    else:
        print("No data retrieved from the database.")
    return threads

# Helper function to filter threads by board
def filter_by_board(threads, board):
    return [thread for thread in threads if thread['board'] == board]

# Function to annotate spikes in the plot
def annotate_spikes(ax, df, column, threshold=0.1):
    """
    Annotate spikes on the plot where the value jumps significantly.
    :param ax: The axes object for the plot.
    :param df: DataFrame with the data to plot.
    :param column: Column name to check for spikes.
    :param threshold: Percentage change threshold to consider as a spike.
    """
    prev_value = None
    for i, row in df.iterrows():
        current_value = row[column]
        if prev_value is not None and current_value > prev_value * (1 + threshold):
            ax.annotate(f'Spike: {current_value:.2f}', xy=(i, current_value),
                        xytext=(i, current_value + current_value * 0.05),
                        arrowprops=dict(facecolor='red', shrink=0.05),
                        fontsize=8, color='red')
        prev_value = current_value

# 1. Plot: Total Data Collected Over Time (in MB)
def plot_data_size_over_time():
    threads = get_all_data()
    sizes = []
    times = []

    for thread in threads:
        # Calculate size of each thread using BSON encode and len
        size_in_bytes = len(BSON.encode(thread))
        size_in_mb = size_in_bytes / (1024 * 1024)  # Convert to MB
        sizes.append(size_in_mb)
        times.append(thread['crawled_at'])

    df = pd.DataFrame({'crawled_at': times, 'size_in_mb': sizes})
    df['crawled_at'] = pd.to_datetime(df['crawled_at'])

    # Resample by minute ('T') to group the data
    df_resampled = df.resample('T', on='crawled_at').sum()

    # Plot
    plt.figure(figsize=(10, 6))
    ax = df_resampled['size_in_mb'].plot(kind='line', label='Data Size (MB)', color='blue')
    plt.title('Total Data Collected Over Time (in MB)')
    plt.xlabel('Time')
    plt.ylabel('Data Size (MB)')
    plt.grid(True)

    # Annotate spikes in the data
    annotate_spikes(ax, df_resampled, 'size_in_mb', threshold=0.5)

    # Add legend
    plt.legend()

    # Save the plot
    plt.savefig(f'{PLOT_DIR}/data_collected_over_time.png')
    plt.close()

    # Notify when plot is saved
    print("Plot 'Total Data Collected Over Time (in MB)' has been generated and saved.")

# 2. Plot: Number of Active Threads Over Time (Separate for each board)
def plot_active_threads_over_time():
    threads = get_all_data()

    # Filter threads by board and plot for each board
    for board in ['g', 'tv']:
        board_threads = filter_by_board(threads, board)
        df = pd.DataFrame(board_threads)
        df['crawled_at'] = pd.to_datetime(df['crawled_at'])

        # Filtering for active threads (not deleted)
        active_threads = df[df['is_deleted'] == False]
        active_counts = active_threads.resample('T', on='crawled_at').size()

        # Plot
        plt.figure(figsize=(10, 6))
        ax = active_counts.plot(kind='line', label=f'Active Threads on /{board}/', color='green')
        plt.title(f'Number of Active Threads Over Time - /{board}/')
        plt.xlabel('Time')
        plt.ylabel('Number of Active Threads')
        plt.grid(True)

        # Annotate spikes in active threads
        annotate_spikes(ax, active_counts.to_frame(), 0, threshold=0.5)

        # Add legend
        plt.legend()

        # Save the plot
        plt.savefig(f'{PLOT_DIR}/active_threads_over_time_{board}.png')
        plt.close()

        # Notify when plot is saved
        print(f"Plot 'Number of Active Threads Over Time - /{board}/' has been generated and saved.")

# Main block to run the plots
if __name__ == "__main__":
    plot_data_size_over_time()
    plot_active_threads_over_time()
