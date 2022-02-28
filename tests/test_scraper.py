from types import NoneType
from scraper import Scraper
import unittest

class ScraperTestCase(unittest.TestCase):
    def test_load_webpage(self):
        open_chess = Scraper("https://chess.com/ratings")
        expected_value = scraper.Scraper
        actual_value = Scraper("https://chess.com/ratings")
        self.assertEqual(expected_value, actual_value)

unittest.main(argv=[''], verbosity=3, exit=False)