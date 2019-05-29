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

class GeopriceHistoryTasksTestCase(unittest.TestCase):
    """ Test Case for Geoprice Async Tasks  (History)

        TODO: Set up all the tests of the 
            -  alarm
            - product
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

    @unittest.skip('TODO')
    def test_00_history_alarm_root_path(self):
        """ Test price History Alarm Root path
        """
        print(">>>>>", "Test price History Alarm Root path")
        _res = self.app.get("/history/alarm/")
        print(_res.status_code)
        print(_res.data)
    
    @unittest.skip("Already tested")
    def test_01_history_product_root_path(self):
        """ Test price History Product Root path
        """
        print(">>>>>", "Test price History Product Root path")
        _res = self.app.get("/history/product/")
        print(_res.status_code)
        print(_res.data)

    #@unittest.skip("Already tested")
    def test_02_history_alarm_method(self):
        """ Test price History Alarm Method
        """
        print(">>>>>", "Test price History Alarm Method")

        # Filters for the task
        params = params = {
            "uuids" : ["478c624b-bf0d-4540-bee9-c870ff0e69fd",
                    "c34742b1-9ed7-451c-b0aa-c965e146675b",
                    "6267ab2f-bf96-4c4c-8c12-acac370aebf3"],
            "retailers" : ["walmart","chedraui", "san_pablo"],
            "today" : "2019-05-20"
        }
        _res = self.app.post('/history/alarm/prices',
                    data=json.dumps(params),
                    headers={'content-type': 'application/json'}
        )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)

    # @unittest.skip("Already tested")
    def test_03_complete_task_history_alarm(self):
        """ Test price Alarm
        """
        print(">>>>>", "Test Alarm")
        # Import celery task
        from app.models.history_alarm import Alarm

        # Filters for the task
        params = {
            "uuids" : [
                "abebb3a7-e6d2-4235-8cfa-66b63f80c3e2"
            ],
            "retailers" : ["walmart","superama"],
            "today" : "2019-05-24"
        }   

        resp = Alarm.start_task(params)     
        print("Submitted Task: ")
        print("Result keys: {} ".format(list(resp.keys())))

        self.assertIsInstance(resp, dict)

    #@unittest.skip("Already tested")
    def test_04_history_product_bystore(self):
        """ Test price History Product bystore
        """
        print(">>>>>", "Test price History Product bystore")
        _res = self.app.get("/history/product/bystore?uuid=fd960578-71ae-463e-84d5-0e451d184597")
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass

    #@unittest.skip("Already tested")
    def test_05_history_product_bystore_history(self):
        """ Test price History Product bystore history
        """
        print(">>>>>", "Test price History Product bystore history")
        _res = self.app.get("/history/product/bystore/history?uuid=fd960578-71ae-463e-84d5-0e451d184597")
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
            

    #@unittest.skip("Already tested")
    def test_06_history_product_ticket(self):
        """ Test price History Product ticket
        """
        print(">>>>>", "Test price History Product ticket")
        _res = self.app.post("/history/product/ticket",
            data=json.dumps(
                {'uuids': ['fd960578-71ae-463e-84d5-0e451d184597'] }
                ),
            headers={'content-type': 'application/json'}
        )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass


if __name__ == '__main__':
    unittest.main()


