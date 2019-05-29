#-*- coding: utf-8 -*-
import app
from app.models.task import Task
import unittest
import config
import uuid
import sys
import time
import json

task_id = None

class GeopriceGeoServicesTestCase(unittest.TestCase):
    """ Test Case for Geoprice Geo Services

        TODO: Set up all the tests of the 
            - alert
            - check
            - dump
        modules in here, normal methods and also tasks
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the database
        """
        # Define test database
        print("Setting up tests for CELERY TASKS")
        if config.TESTING:
            with app.app.app_context():
                app.get_redis()
                print("Connected to Redis")

    @classmethod
    def tearDownClass(cls):
        """ Drops database
        """
        print(">>>> Teardown class")
        return

    def setUp(self):
        """ Set up
        """
        # Init Flask ctx
        self.ctx = app.app.app_context()
        self.ctx.push()
        app.get_redis()
        # Testing client
        self.app = app.app.test_client()
        print("\n"+"_"*50+"\n")

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()

    def test_00_geo_alert_root_path(self):
        """ Test price Geo Alert Root path
        """
        print(">>>>>", "Test price Geo Alert Root path")
        _res = self.app.get("/geo/alert/")
        print(_res.status_code)
        print(_res.data)
    
    @unittest.skip("TO DO ")
    def test_01_geo_check_root_path(self):
        """ Test price Geo Check Root path
        """
        print(">>>>>", "Test price Geo Check Root path")
        _res = self.app.get("/geo/check/")
        print(_res.status_code)
        print(_res.data)
    
    @unittest.skip("TO DO ")
    def test_02_geo_dump_root_path(self):
        """ Test price Geo Dump Root path
        """
        print(">>>>>", "Test price Geo Dump Root path")
        _res = self.app.get("/geo/dump/")
        print(_res.status_code)
        print(_res.data)

    def test_03_geo_alert_prices_method(self):
        """ Test Geo Alert Prices Method
        """
        print(">>>>>", "Test Geo Alert Prices Method")

        # Filters for the task
        params = {
            "uuids" : ["478c624b-bf0d-4540-bee9-c870ff0e69fd",
                    "c34742b1-9ed7-451c-b0aa-c965e146675b",
                    "6267ab2f-bf96-4c4c-8c12-acac370aebf3"],
            "retailers" : ["walmart","chedraui", "san_pablo"],
            "today" : "2019-05-20"
        }
        _res = self.app.post('/geo/alert/prices',
                    data=json.dumps(params),
                    headers={'content-type': 'application/json'}
        )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)

          

if __name__ == '__main__':
    unittest.main()


