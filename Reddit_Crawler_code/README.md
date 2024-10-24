# Reddit Crawler with MongoDB and Faktory

This project contains a Python-based Reddit crawler that fetches hot posts and their comments from specified subreddits. It uses the Reddit API to gather data, stores it in a MongoDB database, and uses Faktory to manage job queuing and background workers.

## Prerequisites

### Setting Up Faktory

sudo docker run --rm -it \
-v ./data:/var/lib/faktory/db \
-e "FAKTORY_PASSWORD=raj123" \
-p 127.0.0.1:7419:7419 \
-p 127.0.0.1:7420:7420 \
contribsys/faktory:latest



## Setting Up MongoDB

sudo systemctl start mongod (to Run MongoDB)
sudo systemctl status mongod (to see Status)
sudo systemctl enable mongod 

sudo systemctl stop mongod (to stop connection)


## Setting Up Environment Variables


echo "MONGO_DB_URL=mongodb://localhost:27017/" > .env
echo "FAKTORY_SERVER_URL=tcp://:raj123@localhost:7419" >> .env
echo "REDDIT_CLIENT_ID=<your_reddit_client_id>" >> .env
echo "REDDIT_CLIENT_SECRET=<your_reddit_client_secret>" >> .env

