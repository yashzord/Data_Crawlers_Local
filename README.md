# Data_Crawlers_Local
1. Install Docker
2. Install MongoDB Compass
3. Use wsl (ubuntu) for Windows
4. Install Faktory using Docker (Windows)
  - sudo apt update
  - docker pull contribsys/faktory
  - sudo docker run --rm -it \
    -v ./data:/var/lib/faktory/db \
    -e "FAKTORY_PASSWORD=yash123" \
    -p 127.0.0.1:7419:7419 \
    -p 127.0.0.1:7420:7420 \
    contribsys/faktory:latest

5.
