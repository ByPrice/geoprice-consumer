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

    def test_00_history_alarm_root_path(self):
        """ Test price History Alarm Root path
        """
        print(">>>>>", "Test price History Alarm Root path")
        _res = self.app.get("/history/alarm/")
        print(_res.status_code)
        print(_res.data)
    
    def test_01_history_product_root_path(self):
        """ Test price History Product Root path
        """
        print(">>>>>", "Test price History Product Root path")
        _res = self.app.get("/history/product/")
        print(_res.status_code)
        print(_res.data)

    @unittest.skip('TODO')
    def test_02_history_alarm_method(self):
        """ Test price History Alarm Method

            TODO:  Verify this test before running
        """
        print(">>>>>", "Test price History Alarm Method")
        from app.controllers.history_alarm import check_prices_today

        # Filters for the task
        params = {
			'uuids' : ['2h354iu23h5423i5uh23i5', '30748123412057g1085h5oh3'],
			'retailers' : ['walmart','chedraui'],
			'today' : '2017-09-20'
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

if __name__ == '__main__':
    unittest.main()


