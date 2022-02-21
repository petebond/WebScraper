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
        # set up data structure
        self.player_links = []
        self.player_num = 0
        self.player_data = {
            "uuid": [], 
            "name": [], 
            "rank": [], 
            "classical": [], 
            "search_term": [],
            "links": []
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
        player_table = self.soup.find(name = 'tbody')
        for link in player_table.find_all('a'):
            self.player_data["links"].append(link.get('href'))
            self.player_data["uuid"].append(str(uuid.uuid4()))
        # set storage location
        self.data_store = "./raw_data"

    def create_store(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    #Get all player data into a dictionary - well formatted
    def get_player_data(self):
        rank_list = self.driver.find_elements(By.CLASS_NAME, "master-players-rating-rank")
        name_list = self.driver.find_elements(By.CLASS_NAME, "username")
        classical_list = self.driver.find_elements(By.CLASS_NAME, "master-players-rating-rank-active")
        
        self.player_data["rank"] = [item.text for item in rank_list]
        self.player_data["name"] = [item.text for item in name_list]
        self.player_data["classical"] = [item.text for item in classical_list]
        self.player_data["search_term"] = [(item.text + " chess player") for item in name_list]
    
    # Search Wikipedia for each chess player and download photo
    def wiki_player_search(self):
        for index in range(len(self.player_data["name"])):
            search_term = self.player_data["search_term"][index]
            folder_name = self.player_data["name"][index].strip()
            self.create_store(f'raw_data/{folder_name}')
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
            # data_dump() and pull_image need to run on every iteration of a person search
            self.pull_image(folder_name, search_term)
            data = {
                "uuid": self.player_data["uuid"][index],
                "name": self.player_data["name"][index], 
                "rank": self.player_data["rank"][index], 
                "classical": self.player_data["classical"][index],
                "search_term": self.player_data["search_term"][index],
                "links": self.player_data["links"][index]        
            }
            self.data_dump(folder_name, data)

    def pull_image(self, folder_name, search_term):
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
        with open(f"raw_data/{folder_name}/data.json", "w") as f:
            json.dump(data, f)
  

    ## 
if __name__ == "__main__":
    chess_scrape = Scraper("http://chess.com/ratings")
    chess_scrape.store_UUIDs_and_links()
    chess_scrape.create_store(chess_scrape.data_store)
    chess_scrape.get_player_data()
    chess_scrape.wiki_player_search()

