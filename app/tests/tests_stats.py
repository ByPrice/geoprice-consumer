# -*- coding: utf-8 -*-
import app
import config
import unittest
import json
import time
import sys
from pprint import pprint
from app.scripts.create_stats import get_daily_data, aggregate_daily
import datetime

# Vars
test_date = datetime.date(2018,5,10)


class GeopriceStatsTestCase(unittest.TestCase):
    """ Test Case for Geoprice Stats
    """ 

    daily_data = None

    @classmethod
    def setUpClass(cls):
        """ Initializes the database
        """
        # Define test database
        print("Setting up Stats tests")

    @classmethod
    def tearDownClass(cls):
        """ Drops database
        """
        print("Teardown class")

    def setUp(self):
        """ Set up
        """
        # Init Flask ctx
        self.ctx = app.app.app_context()
        self.ctx.push()
        app.get_db()
        #pass

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()
        #pass

    def test_01_stats_query(self):
        """ Testing Stats query
        """ 
        self.daily_data = get_daily_data(test_date)
        print(self.daily_data)
        self.assertGreater(len(self.daily_data), 0)

    def test_02_stats_generation(self):
        """ Testing Stats generation
        """ 
        if self.daily_data is None:
            self.daily_data = get_daily_data(test_date)
        try:
            aggregate_daily(self.daily_data )
            self.assertTrue(True)
        except Exception as e:
            print(e)
            self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()