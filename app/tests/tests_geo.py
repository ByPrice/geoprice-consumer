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
    
    def test_04_geo_alert_direct_compare_prices_method(self):
        """ Test Geo Alert Prices Compare Method
        """
        print(">>>>>", "Test Geo Alert Prices Compare Method")

        # Filters for the task
        params = {
            "alerts": [
                {
                    "variation": 10,
                    "item_uuid_compare": "078d7bd8-f447-4913-87a4-f6c82860403d",
                    "type": "price",
                    "item_uuid": "b50a332c-9d54-4df7-83ce-f856db0c1f51"
                },
                {
                    "variation": 10,
                    "item_uuid_compare": "b50a332c-9d54-4df7-83ce-f856db0c1f51",
                    "type": "price",
                    "item_uuid": "d4cae6a5-ea9e-40c9-b1d5-6a2ed6656fc5"
                }
            ],
            "retailers": [
                "chedraui",
                "fresko",
                "la_comer",
                "soriana",
                "superama",
                "walmart"
            ],
            "stores": [ "9bca8952-7b04-11e7-855a-0242ac110005", "976a313c-7b04-11e7-855a-0242ac110005", "a36c064a-7b04-11e7-855a-0242ac110005", "9c9e654c-7b04-11e7-855a-0242ac110005", "99211a2c-7b04-11e7-855a-0242ac110005", "9d6119a2-7b04-11e7-855a-0242ac110005", "96c3ed9a-7b04-11e7-855a-0242ac110005", "9a48de30-7b04-11e7-855a-0242ac110005", "95ec22de-7b04-11e7-855a-0242ac110005", "88798f92-7b04-11e7-855a-0242ac110005", "a03f8398-7b04-11e7-855a-0242ac110005", "a2989062-7b04-11e7-855a-0242ac110005", "a41bda02-7b04-11e7-855a-0242ac110005", "acdd8db6-7b04-11e7-855a-0242ac110005", "b6a82810-7b04-11e7-855a-0242ac110005", "b7fe5e64-7b04-11e7-855a-0242ac110005", "b5c7ff92-7b04-11e7-855a-0242ac110005", "a607e374-7b04-11e7-855a-0242ac110005", "b775d774-7b04-11e7-855a-0242ac110005","e523e86e-7afa-11e7-8dda-0242ac110005", "e499dd86-7afa-11e7-8dda-0242ac110005"]
        }
        _res = self.app.post('/geo/alert/price_compare',
                    data=json.dumps(params),
                    headers={'content-type': 'application/json'}
        )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)

    def test_05_geo_alert_geolocated_prices_method(self):
        """ Test Geo Alert Geolocated Method
        """
        print(">>>>>", "Test Geo Alert Geolocated Method")

        # Filters for the task
        params = {
            "items": [
                ["b50a332c-9d54-4df7-83ce-f856db0c1f51", 32.90],
                ["fff85e4c-404d-4c86-8ff0-6e764da130ea", 19.90],
                ["d4cae6a5-ea9e-40c9-b1d5-6a2ed6656fc5", 2.20],
                ["b50a332c-9d54-4df7-83ce-f856db0c1f51", 4.50]
            ], 
            "stores": [
                ["17128984-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["20a7c5d6-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["1743a104-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["175c49a2-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["172a7274-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["2d7b15b0-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["283aeb0c-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["299359b2-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["293270d4-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["2560acf0-7ace-11e7-9b9f-0242ac110003", "walmart"], 
                ["25a731b6-7ace-11e7-9b9f-0242ac110003", "walmart"],
                ["3f4b9de4-ecf9-4445-96a7-1dc778d0f9ea", "walmart"],
                ["e02a5370-7b09-11e7-855a-0242ac110005", "chedraui"],
                ["e0128880-7b09-11e7-855a-0242ac110005", "chedraui"],
                ["5ba8393e-7ae9-11e7-a394-0242ac110003", "san_pablo"],
                ["d11e139e-7afa-11e7-8dda-0242ac110005", "superama"]
            ], 
            "retailers": ["walmart", "san_pablo","chedraui", "superama"], 
            "variation": 0.1, 
            "variation_type": "percent",
            "date":"2019-05-29"
        }
        _res = self.app.post('/geo/alert/geolocated',
                    data=json.dumps(params),
                    headers={'content-type': 'application/json'}
        )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)

    def test_06_geo_check_stores_method(self):
        """ Test Geo Check Stores Method
        """
        print(">>>>>", "Test Geo Check Stores Method")
        retailer = "la_comer"
        _res = self.app.get('/geo/check/stores/'+retailer)
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)
          

if __name__ == '__main__':
    unittest.main()


