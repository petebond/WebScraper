# syntax=docker/dockerfile:1

FROM python:3.9-buster
WORKDIR /app


# Upgrade pip
RUN pip3 install --upgrade pip

# Copy AWS credentials
ENV region=eu-west-2
ENV aws_access_key_id=AKIA34QT5SJ5M5422NOT
ENV aws_secret_access_key=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

COPY . .

# install python packages using pip
RUN pip3 install -r requirements.txt

# Get google's chrome signing key
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
# Adding Google Chrome to the repositories
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
# Updating apt to see and install Google Chrome
RUN apt-get -y update
# Install latest stable chrome release
RUN apt-get install -y google-chrome-stable

# Execute the scraping
CMD [ "python3", "web_scraper/scraper.py"]