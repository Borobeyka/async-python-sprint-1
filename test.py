import unittest

from patterns import *
from tasks import *


class Test(unittest.TestCase):
    def test_fetch_forecast(self):
        self.assertIsInstance(DataFetchingTask.fetch("MOSCOW"), CityForecastPattern)
        self.assertIsInstance(DataFetchingTask.fetch("PARIS"), CityForecastPattern)

    def test_fetch_forecast_failed(self):
        self.assertIsInstance(DataFetchingTask.fetch("TVER"), CityForecastPattern)
    

if __name__ == "__main__":
    unittest.main()