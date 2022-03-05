import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import urllib.request
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os
import uuid
import json
import boto3
import pandas as pd
# import time
from sqlalchemy import create_engine
from sqlalchemy import inspect
from alive_progress import alive_bar


class Scraper:
    def __init__(self, url):
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
        self.driver = webdriver.Chrome(ChromeDriverManager().install(),
                                       options=options)
        # set storage location
        self.data_store = "./raw_data"

        # acquire existing data from RDS for comparison
        # TODO get RDS connection. Compare name and rank of existing
        # before downloading new.

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
        try:
            pic_file2 = (f"raw_data/{folder_name}/{folder_name} - wiki.jpg")
        except Exception:
            pic_file2 = ""
        self.upload_to_aws((f"raw_data/{folder_name}/data.json"),
                           'chess-top-50', pic_file, pic_file2, folder_name)

    def follow_links_more_data(self, link, name, rank, classical):
        """Going to the individual page on chess.com and getting extra data

        This downloads the player date of birth and a second image, as well
        as country of origin and chess federation that the player represents.
        """
        upload = False
        try:
            file = (f"raw_data/{name}/data.json")
            temp_stats = pd.read_json(file, orient='records')
            temp_rank = temp_stats['rank'][0]
            temp_classical = temp_stats['classical'][0]
        except ValueError:
            temp_rank = 0
            temp_classical = 0
        if rank != temp_rank and classical != temp_classical:
            upload = True
            self.driver.get(link)
            player_table = self.driver.find_elements(
                By.CLASS_NAME, "master-players-value")
            player_stats = []
            for row in player_table:
                player_stats.append(row.text)
            self.player_data["date_of_birth"].append(player_stats[1])
            self.player_data["place_of_birth"].append(player_stats[2])
            self.player_data["chess_federation"].append(player_stats[3])
            self.changes = True
        # Get chess.com image if not already acquired
        if not os.path.exists(f"raw_data/{name}/{name}.jpg"):
            upload = True
            try:
                image = self.driver.find_element(By.CLASS_NAME,
                                                 "post-view-thumbnail")
                image = image.get_attribute("src")
                urllib.request.urlretrieve(image,
                                           f"raw_data/{name}/{name}.jpg")
            except Exception:
                urllib.request.urlretrieve("https://tinyurl.com/y7l4drrj",
                                           f"raw_data/{name}/{name}.jpg")
        else:
            self.player_data["date_of_birth"].append(
                                            temp_stats['date_of_birth'][0])
            self.player_data["place_of_birth"].append(
                                            temp_stats['place_of_birth'][0])
            self.player_data["chess_federation"].append(
                                            temp_stats['chess_federation'][0])
        return upload

    def upload_to_aws(self, filename, bucket, image, image2, folder):
        """Gets the data.json file and the image.jpg and uploads them to AWS"""
        s3 = boto3.client('s3')
        s3b = boto3.resource('s3')
        with open(filename, 'rb') as data:
            s3.upload_fileobj(data, bucket, f"raw_data/{folder}/data.json")
            print("uploading json file")
            try:
                s3b.meta.client.upload_file(
                    image, bucket, f"raw_data/{folder}/{folder}.jpg")
                print("uploaded picture")
            except Exception:
                pass
            try:
                s3b.meta.client.upload_file(
                    image2, bucket, f"raw_data/{folder}/{folder} - wiki.jpg")
                print("uploaded second picture")
            except Exception:
                pass

    def upload_table_data(self):
        """uploads data.json from folders to AWS RDS

        Takes the data.json file from each player folder in raw_data
        Converts the data.json files into a pandas data frame
        Uploads the data frame as a table in AWS
        """
        DATABASE_TYPE = "postgresql"
        DBAPI = 'psycopg2'
        ENDPOINT = 'chess-db.cxwlqkybpl0p.eu-west-2.rds.amazonaws.com'
        USER = 'postgres'
        PASSWORD = 'chesspass'
        PORT = 5432
        DATABASE = 'chessdb'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:"
                               f"{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
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
            data_set.to_sql('tbl_chess_players', engine, if_exists='replace')
            # Have a look at the column names for each table
            insp = inspect(engine)
            print(insp)
            for table_name in insp.get_table_names():
                for column in insp.get_columns(table_name):
                    print(f"Column: {column['name']} of {table_name}")


if __name__ == "__main__":
    """
    Main program to trigger each function in order.
    """
    test = False
    upload_to_s3 = False
    upload_to_rds = False
    if test is False:
        chess_scrape = Scraper("http://chess.com/ratings")
        for i in range(1, 9):
            url = "http://chess.com/ratings" + "?page=" + str(i)
            chess_scrape.page_grab(url)
            chess_scrape.store_UUIDs_and_links()
            chess_scrape.create_store(chess_scrape.data_store)
            chess_scrape.get_player_data()
        chess_scrape.player_search()
        chess_scrape.upload_table_data()

    else:
        chess_scrape = Scraper("http://chess.com/ratings")
        chess_scrape.upload_table_data()
