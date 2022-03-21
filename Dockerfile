# syntax=docker/dockerfile:1

FROM python:3.9-buster
WORKDIR /app

# Copy all allowed files and credentials
ENV HOME ~
COPY . .
# Old AWS credentials copy ADD aws ~/.aws

# Upgrade pip
RUN pip3 install --upgrade pip

# install python packages using pip
RUN pip3 install -r requirements.txt


ENV PATH="/usr/local/bin/:${PATH}"

# Get google's chrome signing key
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
# Adding Google Chrome to the repositories
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
# Updating apt to see and install Google Chrome
RUN apt-get -y update
# Install latest stable chrome release
RUN apt-get install -y google-chrome-stable

RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN apt-get install -yqq unzip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# Execute the scraping
CMD [ "python3", "web_scraper/scraper.py"]
