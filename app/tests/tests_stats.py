# -*- coding: utf-8 -*-
import app
import config
import unittest
import json
import threading
import time
import sys
from pprint import pprint
from app.scripts.create_stats import daily_stats
import datetime

# Vars
test_date = datetime.date(2018,5,10)


class GeopriceStatsTestCase(unittest.TestCase):
    """ Test Case for Geoprice Stats
    """ 

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
        #self.ctx = app.app.app_context()
        #self.ctx.push()
        #app.get_db()
        pass

    def tearDown(self):
        # Dropping flask ctx
        #self.ctx.pop()
        pass

    def test_01_stats_generation(self):
        """ Testing Stats generation
        """ 
        daily_stats(test_date)
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()