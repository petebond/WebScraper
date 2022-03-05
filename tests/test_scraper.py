import unittest
import web_scraper.scraper as ws


class TestScraper(unittest.TestCase):
    def setUp(self):
        self.bot = ws.Scraper("http://chess.com/ratings")

    def test_ratings_page_loads(self):
        """
        Test that the chess.com ratings page loads on init
        """
        actual_value = self.bot.driver.current_url
        print(actual_value)
        expected_value = "https://www.chess.com/ratings"
        self.assertEqual(actual_value, expected_value)

    def test_extra_player_data(self):
        self.bot.follow_links_more_data(
            "Magnus Carlsen", "https://www.chess.com/players/magnus-carlsen")
        actual_value = self.bot.driver.current_url
        expected_value = "https://www.chess.com/players/magnus-carlsen"
        self.assertEqual(actual_value, expected_value)

    def tearDown(self):
        pass


# Integration test is where you test a method that calls other methods.
# We want to test as granularly as possible.
if __name__ == '__main__':
    unittest.main(verbosity=2)
