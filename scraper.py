import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import urllib.request
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import os
import uuid
import json


class Scraper:
    
    def __init__(self, url):
        """Initialise the class object and set up the data structure 
        
        player_data is a dictionary with player attributes.
        each player is accessible by index of the player_data dictionary
        each player will have data output to the JSON dump later
        
        init then installs Chrome Driver and opens the page given by the url parameter
        the html of that page is then acquired using BS
        """
        # set up data structure
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
        self.driver = webdriver.Chrome(ChromeDriverManager().install())
        self.url = url
        self.driver.get(self.url)
        # requests and beautiful soup for getting the list of links
        page = requests.get(self.url)
        html = page.text
        self.soup = BeautifulSoup(html, 'html.parser')

    def store_UUIDs_and_links(self):
        """Generating unique IDs using UUID4 and accesing link data from the soup"""
        player_table = self.soup.find(name = 'tbody')
        for link in player_table.find_all('a'):
            self.player_data["links"].append(link.get('href'))
            self.player_data["uuid"].append(str(uuid.uuid4()))
        # set storage location
        self.data_store = "./raw_data"

    def create_store(self, folder):
        """Checks to see if the raw_data folder exists, if not, creates it."""
        if not os.path.exists(folder):
            os.makedirs(folder)

    #Get all player data into a dictionary - well formatted
    def get_player_data(self):
        """Uses selenium to get name, rank and classical ratings points information from the ratings page
        Generates a search term from the name, for use in wikipedia later
        """
        rank_list = self.driver.find_elements(By.CLASS_NAME, "master-players-rating-rank")
        name_list = self.driver.find_elements(By.CLASS_NAME, "username")
        classical_list = self.driver.find_elements(By.CLASS_NAME, "master-players-rating-rank-active")
        
        self.player_data["rank"] = [item.text for item in rank_list]
        self.player_data["name"] = [item.text for item in name_list]
        self.player_data["classical"] = [item.text for item in classical_list]
        self.player_data["search_term"] = [(item.text + " chess player") for item in name_list]
    
    # Search Wikipedia for each chess player and download photo
    def wiki_player_search(self):
        """Using wikipedia - the function searches for the player using their name and "chess player"
        which forms the search_term. This brings up a search result page, of which the first link is 
        accessed (unless a "did you mean confusion needs dealing with). 
        Then accessed right hand vBox, grabs the image url and then pulls the image into the data folder.
        
        Not my finest bit of code, this. Lots of very specific xpaths used, with some workarounds for wiki inconsistencies.
        """
        for index in range(len(self.player_data["name"])):
            search_term = self.player_data["search_term"][index]
            folder_name = self.player_data["name"][index].strip()
            self.driver.get("http://wikipedia.org")
            # choose search box
            search = self.driver.find_element_by_id("searchInput")
            search.click()
            # magnus is famous enough to have his own Wiki URL so I need to fudge his search.
            # if any other player gets their own web direct link it'll break this scraper.
            if self.player_data["name"][index] == "Magnus Carlsen":
                search.send_keys("Magnus Carlsen Norwegian")
            else:
                search.send_keys(search_term)
            confirm = self.driver.find_element_by_xpath("/html/body/div[3]/form/fieldset/button")
            confirm.click()
            # try the top result
            print("SEARCH COMPLETE - CLICKING LINK")
            try:
                first_result = self.driver.find_element_by_xpath("/html/body/div[3]/div[3]/div[4]/div[3]/ul/li[1]/div[1]/a")
            except:
                # some pages have a "did you mean" to ignore
                first_result = self.driver.find_element_by_xpath("/html/body/div[3]/div[3]/div[4]/div[4]/ul/li[1]/div[1]/a")
            print("LINK CLICKED")
            link = first_result.get_attribute("href")
            print(link)
            self.driver.get(link)
            self.create_store(f'raw_data/{folder_name}')
            self.pull_image(folder_name, search_term)
            # data_dump() and pull_image() both need to run on every iteration of a person search
            self.follow_links_more_data(folder_name, self.player_data["links"][index])
            data = {
                "uuid": self.player_data["uuid"][index],
                "name": self.player_data["name"][index], 
                "rank": self.player_data["rank"][index], 
                "classical": self.player_data["classical"][index],
                "search_term": self.player_data["search_term"][index],
                "links": self.player_data["links"][index], 
                "date_of_birth": self.player_data["date_of_birth"][index],
                "place_of_birth": self.player_data["place_of_birth"][index],
                "chess_federation": self.player_data["chess_federation"][index]    
            }
            
            self.data_dump(folder_name, data)

    def pull_image(self, folder_name, search_term):
        """Downloads the image from the specific location on the wikipedia page."""
        try:
            image = self.driver.find_element_by_xpath('//*[@id="mw-content-text"]/div[1]/table[1]/tbody/tr[2]/td/a/img')
        except:
            try:
                image = self.driver.find_element_by_xpath('//*[@id="mw-content-text"]/div[1]/table[1]/tbody/tr[3]/td/a/img')
            except:
                image = self.driver.find_element_by_xpath('//*[@id="mw-content-text"]/div[1]/table[2]/tbody/tr[2]/td/a/img')
        image = image.get_attribute("src")
        urllib.request.urlretrieve(image, f"raw_data/{folder_name}/{folder_name}.jpg")
        print(f"Search complete for {search_term} - Moving on!")
            
    def data_dump(self, folder_name, data):
        """Converts the appropriate indexed player_data information into a json file and stores it in the player's folder"""
        with open(f"raw_data/{folder_name}/data.json", "w") as f:
            json.dump(data, f)

    def follow_links_more_data(self, name, link):
        self.driver.get(link)
        print(f"going to {link}")
        player_table = self.driver.find_elements(By.CLASS_NAME, "master-players-value")
        player_stats = []
        for row in player_table:
            player_stats.append(row.text)
        print(player_stats)
        self.player_data["date_of_birth"].append(player_stats[1])
        self.player_data["place_of_birth"].append(player_stats[2])
        self.player_data["chess_federation"].append(player_stats[3])
        #Get second image
        image = self.driver.find_element(By.CLASS_NAME, "post-view-thumbnail")
        image = image.get_attribute("src")
        urllib.request.urlretrieve(image, f"raw_data/{name}/{name}2.jpg")



  

    ## 
if __name__ == "__main__":
    """Main program to trigger each function in order.
    
    Paramter for the Scraper class is the URL which is passed to the constructor for the class
    """
    chess_scrape = Scraper("http://chess.com/ratings")
    chess_scrape.store_UUIDs_and_links()
    chess_scrape.create_store(chess_scrape.data_store)
    chess_scrape.get_player_data()
    chess_scrape.wiki_player_search()
    print(chess_scrape.wiki_player_search.__doc__)
