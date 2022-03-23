import requests
import numpy as np
from bs4 import BeautifulSoup
from selenium import webdriver
import urllib.request
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os
import uuid
import json
import boto3
import botocore
import shutil
import pandas as pd
from sqlalchemy import create_engine
from alive_progress import alive_bar
from dotenv import load_dotenv
load_dotenv()


class Scraper:
    def __init__(self):
        """Initialise the class object and set up the data structure

        Parameters:
        ----------

        url: str
            The URL of the site to be scraped

        Attributes:
        ----------

        player_links: list[str]
            The urls of the individual player profile pages on chess.com

        player_data: dict
            Information about each player, in a dictionary of data

        driver: webdriver object
            Selenium webdriver object using Chrome

        soup: the beautiful soup interpretation of the page content

        """
        # set up data structure
        self.changes = False
        self.player_links = []
        self.player_data = {
            "uuid": [],
            "name": [],
            "rank": [],
            "classical": [],
            "search_term": [],
            "links": [],
            "date_of_birth": [],
            "place_of_birth": [],
            "chess_federation": []
        }
        # selenium webdriver for wiki scraping
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        self.driver = webdriver.Chrome(options=options)
        self.s3 = boto3.client('s3')
        self.s3b = boto3.resource('s3')
        # set storage location
        self.data_store = "./raw_data"

    def connect_to_RDS_engine(self):
        DATABASE_TYPE = "postgresql"
        DBAPI = 'psycopg2'
        ENDPOINT = os.environ.get('ENDPOINT')
        DBUSER = os.environ.get('DBUSER')
        DBPASSWORD = os.environ.get('DBPASSWORD')
        PORT = 5432
        DATABASE = os.environ.get('DATABASE')
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{DBUSER}:"
                                    f"{DBPASSWORD}@{ENDPOINT}:"
                                    f"{PORT}/{DATABASE}")
        self.engine.connect()
        self.rds_player_data = pd.read_sql_table(
            'tbl_chess_players', self.engine,
            columns=["uuid", "name", "rank", "classical",
                     "search_term", "links", "date_of_birth",
                     "place_of_birth", "chess_federation"])
        self.rds_player_data = (self.rds_player_data.sort_values('name')
                                .reset_index(drop=True))
        print("Data from RDS below*****")
        print(self.rds_player_data)

    def page_grab(self, url):
        self.url = url
        self.driver.get(self.url)
        # requests and beautiful soup for getting the list of links
        page = requests.get(self.url)
        html = page.text
        self.soup = BeautifulSoup(html, 'html.parser')

    def store_UUIDs_and_links(self):
        """
        Generating unique IDs using UUID4 and accesing link data from the soup

        Parameters:
        ----------
        None

        Returns:
        -------
        None

        """
        print("Acquiring page of players")
        player_table = self.soup.find(name='tbody')
        with alive_bar(50, bar='smooth') as bar:
            for link in player_table.find_all('a'):
                self.player_data["links"].append(link.get('href'))
                self.player_data["uuid"].append(str(uuid.uuid4()))
                bar()

    def create_store(self, folder):
        """Checks to see if the raw_data folder exists, if not, creates it.

        Parameters:
        ----------
        Folder: str
            String value of the folder path for each player's data store

        Returns:
        -------
        None
        """
        if not os.path.exists(folder):
            os.makedirs(folder)

    # Get all player data into a dictionary - well formatted
    def get_player_data(self):
        """
        Get name, rank and classical ratings points from the ratings page

        Parameters:
        ----------
        Folder: str
            String value of the folder path for each player's data store

        Returns:
        -------
        None

        """
        rank_list = self.driver.find_elements(
            By.CLASS_NAME, "master-players-rating-rank")
        name_list = self.driver.find_elements(
            By.CLASS_NAME, "username")
        classical_list = self.driver.find_elements(
            By.CLASS_NAME, "master-players-rating-rank-active")
        self.append_player_data(rank_list, name_list, classical_list)

    def append_player_data(self, rank_list, name_list, classical_list):
        self.player_data["rank"] = (self.player_data["rank"] +
                                    [item.text for item in rank_list])
        self.player_data["name"] = (self.player_data["name"] +
                                    [item.text for item in name_list])
        self.player_data["classical"] = (self.player_data["classical"] +
                                         [item.text for item in classical_list]
                                         )
        self.player_data["search_term"] = (self.player_data["search_term"] + [(
                                            item.text + " chess player")
                                            for item in name_list])
        self.player_data["date_of_birth"] = (
            self.player_data["date_of_birth"]
            + [item.text for item in name_list])
        self.player_data["place_of_birth"] = (
            self.player_data["place_of_birth"]
            + [item.text for item in name_list])
        self.player_data["chess_federation"] = (
            self.player_data["chess_federation"]
            + [item.text for item in name_list])

    def sort_scraped_data(self):
        self.player_data = pd.DataFrame(self.player_data)
        self.player_data = self.player_data.sort_values(
            'name').reset_index(drop=True)

    def check_for_differences(self):
        if len(self.player_data) != len(self.rds_player_data):
            return True
        else:
            # self.player_data['matched'] = [(
            #     self.player_data['name'].reset_index(drop=True) ==
            #     self.rds_player_data['name'].reset_index(drop=True)
            #     )]
            # print(self.player_data['matched'])
            self.player_data['matched'] = (
                np.where(self.player_data['name']
                         == self.rds_player_data['name'], 1, 0))
            if (sum(self.player_data['matched']) == len(
                    self.player_data['matched'])):
                self.player_data['matched'] = (
                    np.where(self.player_data['rank']
                             == self.rds_player_data['rank'], 1, 0))
                if (sum(self.player_data['matched'])
                        == len(self.player_data['matched'])):
                    self.player_data['matched'] = (
                        np.where(self.player_data['classical']
                                 == self.rds_player_data['classical'], 1, 0))
                    if (sum(self.player_data['matched'])
                            == len(self.player_data['matched'])):
                        print("They're the same. No need to rescrape")
                        return False
        print("Differences found - Calculating rescrape necessity")
        return True

    # Follow previously downloaded links to get player data and download photo
    def player_search(self):
        """
        Scrapes info from each player profile page on chess.com

        Parameters:
        ----------
        None

        Returns:
        -------
        None
        """
        with alive_bar(359) as bar:
            # Get chess.com profile page data first
            for index in range(len(self.player_data["name"])):
                folder_name = self.player_data["name"][index].strip()
                self.create_store(f'raw_data/{folder_name}')
                upload = self.follow_links_more_data(
                            self.player_data["links"][index],
                            self.player_data["name"][index],
                            self.player_data["rank"][index],
                            self.player_data["classical"][index])
                data = [
                    {"uuid": self.player_data["uuid"][index],
                     "name": self.player_data["name"][index],
                     "rank": self.player_data["rank"][index],
                     "classical": self.player_data["classical"][index],
                     "search_term": self.player_data["search_term"][index],
                     "links": self.player_data["links"][index],
                     "date_of_birth": self.player_data["date_of_birth"][index],
                     "place_of_birth": (self.player_data["place_of_birth"]
                                        [index]),
                     "chess_federation":
                     self.player_data["chess_federation"][index]}
                ]
                # create the data.json from the above dictionary
                with open(f"raw_data/{folder_name}/data.json", "w") as f:
                    json.dump(data, f)
                # AWS S3 UPLOAD
                if upload:
                    self.data_dump(folder_name)
                bar()

    def data_dump(self, folder_name):
        """

        Parameters:
        ----------
        folder_name: str
            String value of the folder path for each player's data store

        Returns:
        -------
        None
        """
        try:
            pic_file = (f"raw_data/{folder_name}/{folder_name}.jpg")
        except Exception:
            pic_file = ""
        self.upload_to_aws((f"raw_data/{folder_name}/data.json"),
                           'chess-top-50', pic_file, folder_name)

    def follow_links_more_data(self, link, name, rank, classical):
        """Going to the individual page on chess.com and getting extra data

        This downloads the player date of birth and an image, as well
        as country of origin and chess federation that the player represents.
        """
        upload = False
        rds = self.player_data
        position = rds[rds['name'] == name].index[0]
        try:
            temp_rank = rds['rank'][position]
            temp_classical = rds['classical'][position]
        except ValueError:
            temp_rank = 0
            temp_classical = 0
        if not(str(rank) == str(temp_rank)) or not(
                            str(classical) == str(temp_classical)):
            upload = True
            self.driver.get(link)
            player_table = self.driver.find_elements(
                By.CLASS_NAME, "master-players-value")
            player_stats = []
            for row in player_table:
                player_stats.append(row.text)
            self.player_data.at[position, "name"] = name
            self.player_data.at[position, "rank"] = rank
            self.player_data.at[position, "classical"] = classical
            self.player_data.at[position, "date_of_birth"] = player_stats[1]
            self.player_data.at[position, "place_of_birth"] = player_stats[2]
            self.player_data.at[position, "chess_federation"] = player_stats[3]
            self.changes = True
        else:
            self.player_data.at[position, "date_of_birth"] = (
                rds['date_of_birth'][position])
            self.player_data.at[position, "place_of_birth"] = (
                rds['place_of_birth'][position])
            self.player_data.at[position, "chess_federation"] = (
                rds['chess_federation'][position])
        # reach out to s3 to see if there's a picture there.
        try:
            self.s3b.Object('chess-top-50',
                            f"raw_data/{name}/{name}.jpg").load()
            print("IMAGE EXISTS ON REMOTE STORAGE _ IGNORE")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("NO IMAGE FOUND ON S3 - SETTING TO RETRIEVE AND UPLOAD")
                upload = True
                self.driver.get(link)
                try:
                    image = self.driver.find_element(By.CLASS_NAME,
                                                     "post-view-thumbnail")
                    image = image.get_attribute("src")
                    urllib.request.urlretrieve(image,
                                               f"raw_data/{name}/{name}.jpg")
                except Exception:
                    urllib.request.urlretrieve("https://tinyurl.com/y7l4drrj",
                                               f"raw_data/{name}/{name}.jpg")
        return upload

    def upload_to_aws(self, filename, bucket, image, folder):
        """Gets the data.json file and the image.jpg and uploads them to AWS"""
        with open(filename, 'rb') as data:
            self.s3.upload_fileobj(data,
                                   bucket,
                                   f"raw_data/{folder}/data.json")
            try:
                self.s3b.meta.client.upload_file(
                    image, bucket, f"raw_data/{folder}/{folder}.jpg")
            except Exception:
                pass

    def upload_table_data(self):
        """uploads data.json from folders to AWS RDS

        Takes the data.json file from each player folder in raw_data
        Converts the data.json files into a pandas data frame
        Uploads the data frame as a table in AWS
        """
        self.engine.connect()
        file_list = []
        # Get the list of data files from the folder structure
        directory = 'raw_data'
        for player_folder in os.listdir(directory):
            f = os.path.join(directory, player_folder)
            for file in os.listdir(f):
                if ".json" in file:
                    file_list.append(os.path.join(f, file))
        # Iterate through the folders to get the data out of the json files
        # Convert all of the jsons to dataframes
        # Merge all of the dataframes into one frame
        df_list = []
        for file in file_list:
            temp_frame = pd.read_json(file, orient='records')
            df_list.append(temp_frame)
        data_set = pd.concat(df_list, axis=0, ignore_index=True)
        print(data_set)
        # Upload the dataframe as a table to AWS RDS
        if self.changes:
            data_set.to_sql('tbl_chess_players', self.engine,
                            if_exists='replace')

    def cleanup(self):
        if os.path.exists("./raw_data"):
            shutil.rmtree("./raw_data/")


if __name__ == "__main__":
    """
    Main program to trigger each function in order.
    """
    test = False
    upload_to_s3 = False
    upload_to_rds = False
    if test is False:
        chess_scrape = Scraper()
        chess_scrape.connect_to_RDS_engine()
        for i in range(1, 9):
            url = "http://chess.com/ratings" + "?page=" + str(i)
            chess_scrape.page_grab(url)
            chess_scrape.store_UUIDs_and_links()
            chess_scrape.create_store(chess_scrape.data_store)
            chess_scrape.get_player_data()
        chess_scrape.sort_scraped_data()
        print("Scraped Player Data")
        print(chess_scrape.player_data)
        if chess_scrape.check_for_differences():
            print("Differences found - rescraping")
            chess_scrape.player_search()
            chess_scrape.upload_table_data()
        else:
            print("They're the same. No need to rescrape")
        chess_scrape.cleanup()

    else:
        chess_scrape = Scraper("http://chess.com/ratings")
        chess_scrape.upload_table_data()
