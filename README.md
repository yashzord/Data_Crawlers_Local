# Data_Crawlers_Local
1. Install Docker
2. Install MongoDB Compass and MongoDB using Docker
  - docker pull mongo
  - docker run --name mongodb -d -p 27017:27017 mongo
3. Use wsl (ubuntu) for Windows
4. Install Faktory using Docker (Windows)
  - sudo apt update
  - docker pull contribsys/faktory
5. Create a virtual Enviornment and activate it
  - python -m venv venv
  - source venv/Scripts/activate
  - deactivate
6. Install all the requirements after activating the virtual environment
  - pip install -r requirements.txt
7. Start MongoDB (if not running already)
  - docker start mongodb
8. Start Faktory Server. Run faktory in the background (Enter your own machine's username and password)
  - docker run --rm -it \
    -v ./data:/var/lib/faktory/db \
    -e "FAKTORY_PASSWORD=yash123" \
    -p 127.0.0.1:7419:7419 \
    -p 127.0.0.1:7420:7420 \
    contribsys/faktory:latest
Special Note - http://localhost:7420/
9. Run the Cold Start Script
  - python cold_start_board.py
10. Run the Crawler
  - python chan_crawler.py
11. Still Have to work on what to collect exactly and optimize the code, this is just a start. Have to work on reddit and youtube as well. Hvae to make sure we collect data in real time.
