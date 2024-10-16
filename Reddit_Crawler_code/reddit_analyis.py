import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# MongoDB connection (adjust MONGO_DB_URL as necessary)
MONGO_DB_URL = "mongodb://localhost:27017/"
mongo_client = pymongo.MongoClient(MONGO_DB_URL)
db = mongo_client['reddit_data']
posts_collection = db['posts']

# Aggregating posts per hour for each subreddit
pipeline = [
    {
        "$group": {
            "_id": {
                "subreddit": "$subreddit",
                "hour": {"$dateToString": {"format": "%Y-%m-%d %H:00", "date": "$crawled_at"}}
            },
            "post_count": {"$sum": 1}
        }
    },
    {"$sort": {"_id.hour": 1}}  # Sorting by hour
]

result = list(posts_collection.aggregate(pipeline))

# Convert the result to a pandas DataFrame
data = []
for row in result:
    data.append({
        "subreddit": row["_id"]["subreddit"],
        "hour": row["_id"]["hour"],
        "post_count": row["post_count"]
    })

df = pd.DataFrame(data)

# Separate data by subreddit
movies_data = df[df['subreddit'] == 'movies']
technology_data = df[df['subreddit'] == 'technology']

# Convert 'hour' to datetime
movies_data['hour'] = pd.to_datetime(movies_data['hour'])
technology_data['hour'] = pd.to_datetime(technology_data['hour'])

# Set 'hour' as the index for better plotting
movies_data.set_index('hour', inplace=True)
technology_data.set_index('hour', inplace=True)

# Plot post count comparison between movies and technology per hour
plt.figure(figsize=(10, 6))
plt.plot(movies_data.index, movies_data['post_count'], label="Movies - Post Count", color='blue')
plt.plot(technology_data.index, technology_data['post_count'], label="Technology - Post Count", color='green')
plt.xlabel('Time')
plt.ylabel('Post Count')
plt.title('Post Count Comparison per Hour: Movies vs. Technology Subreddit')
plt.legend()
plt.grid(True)
plt.show()
