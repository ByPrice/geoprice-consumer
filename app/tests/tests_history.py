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

    @unittest.skip('TODO')
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

    @unittest.skip("Already tested")
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

    @unittest.skip("Already tested")
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

    @unittest.skip("Already tested")
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
          
    @unittest.skip("Already tested")
    def test_06_history_product_catalogue(self):
        """ Test price History Product Catalogue
        """
        print(">>>>>", "Test price History Product Catalogue")
        _res = self.app.get("/history/product/catalogue?r=walmart&sid=1e3d5b76-7ace-11e7-9b9f-0242ac110003")
        print('Got Response')
            
    @unittest.skip("Already tested")
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

    @unittest.skip("Already tested")
    def test_07_complete_task_count_by_source(self):
        """ Test count by source task
        """
        print(">>>>>", "Test Count by Source")
        # Import celery task
        from app.celery_app import main_task
        from app.models.history_product import Product

        # Filters for the task
        params = {
            "store_id" : "1e3d5b76-7ace-11e7-9b9f-0242ac110003",
            "retailer" : "walmart",
            "date_start" : "2019-05-24",
            "date_end" : "2019-05-25"
        }

        celery_task = main_task.apply_async(args=(Product.count_by_store_task, params))        
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        time.sleep(2)
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            print(task.task_id)
            print(task.progress)
            print(task.status)
            time.sleep(1)

        prog = task.status['progress']
        print("Final progress: {}".format(prog))
        print("Result keys: {} ".format(list(task.result.keys())))
        self.assertEqual(prog,100)
        
    @unittest.skip("Already tested")
    def test_08_history_product_count_st_hours(self):
        """ Test price History Product Count by Store hours
        """
        print(">>>>>", "Test price History Product Count by Store hours")
        _res = self.app.get("/history/product/count_by_store_hours?r=walmart&sid=1e3d5b76-7ace-11e7-9b9f-0242ac110003&last_hours=168")
        print('Got Response')

    @unittest.skip("Already tested")
    def test_07_history_product_stats(self):
        """ Test price History Product stats
        """
        print(">>>>>", "Test price History Product stats")
        _res = self.app.get("/history/product/stats?item_uuid=fd960578-71ae-463e-84d5-0e451d184597")
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass

    @unittest.skip("Already tested")
    def test_09_complete_task_compare_store_item(self):
        """ Test compare store item
        """
        print(">>>>>", "Test Compare store item")
        # Import celery task
        from app.celery_app import main_task
        from app.models.history_product import Product

        # Filters for the task
        params = {
            "fixed_segment" : {
                "store_uuid": "1e3d5b76-7ace-11e7-9b9f-0242ac110003",
                "item_uuid" : "12e4f953-0595-4d3f-8024-aa04c2ec60eb",
                "retailer":"walmart",
                "name": "Test Name"
            },
            "added_segments" : [
                {
                "store_uuid": "1e3d5b76-7ace-11e7-9b9f-0242ac110003",
                    "item_uuid" : "12e4f953-0595-4d3f-8024-aa04c2ec60eb",
                    "retailer":"walmart",
                    "name": "Test Name"
                }
            ],
            "date_ini": "2019-05-24",
            "date_fin": "2019-05-25"
        }

        celery_task = main_task.apply_async(args=(Product.compare_store_item_task, params))        
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        time.sleep(2)
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            print(task.task_id)
            print(task.progress)
            print(task.status)
            time.sleep(1)

        prog = task.status['progress']
        print("Final progress: {}".format(prog))
        print("Result keys: {} ".format(list(task.result.keys())))
        self.assertEqual(prog,100)


    @unittest.skip("Already tested")
    def test_10_complete_task_count_by_store_engine(self):
        """ Test count by store engine
        """
        print(">>>>>", "Test Count by store engine")
        # Import celery task
        from app.celery_app import main_task
        from app.models.history_product import Product

        # Filters for the task
        params = {
            "store_uuid" : "1e3d5b76-7ace-11e7-9b9f-0242ac110003",
            "date" : "2019-05-24",
            "retailer" : "walmart"
        }

        celery_task = main_task.apply_async(args=(Product.count_by_store_engine_task, params))        
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        time.sleep(2)
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            print(task.task_id)
            print(task.progress)
            print(task.status)
            time.sleep(1)

        prog = task.status['progress']
        print("Final progress: {}".format(prog))
        print("Result keys: {} ".format(list(task.result.keys())))
        self.assertEqual(prog,100)
 

    @unittest.skip("Already tested")
    def test_11_complete_task_count_by_retailer_engine(self):
        """ Test count by retailer engine
        """
        print(">>>>>", "Test Count by retailer engine")
        # Import celery task
        from app.celery_app import main_task
        from app.models.history_product import Product

        # Filters for the task
        params = {
            "date" : "2019-05-24 19:17:06",
            "retailer" : "walmart"
        }

        celery_task = main_task.apply_async(args=(Product.count_by_retailer_engine_task, params))        
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        time.sleep(2)
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            print(task.task_id)
            print(task.progress)
            print(task.status)
            time.sleep(1)

        prog = task.status['progress']
        print("Final progress: {}".format(prog))
        print("Result keys: {} ".format(list(task.result.keys())))
        self.assertEqual(prog,100)


    @unittest.skip("Already tested")
    def test_12_complete_byfile(self):
        """ Test price History Product byfile
        """
        # Filters for the task
        params = {
            "sid" : "1e3d5b76-7ace-11e7-9b9f-0242ac110003",
            "date" : "2019-05-24",
            "ret" : "walmart",
            "stn" : "Walmart Test"
        }

        print(">>>>>", "Test price History Product byfile")
        _res = self.app.get("/history/product/byfile?ret=walmart&sid=1e3d5b76-7ace-11e7-9b9f-0242ac110003&stn=Walmart%20Test")
        print('Got Response')
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        try:
            head = _res.headers
            print(head)
        except:
            pass

    # @unittest.skip("Already tested")
    def test_13_complete_task_history_product_retailer(self):
        """ Test Price History Product Retailer
        """
        print(">>>>>", "Test Price History Product Retailer")
        # Import celery task
        from app.celery_app import main_task
        from app.models.history_product import Product

        # Filters for the task
        params = {
            "item_uuid": "fd960578-71ae-463e-84d5-0e451d184597",
            "retailer": "walmart"
        }

        celery_task = main_task.apply_async(args=(Product.start_retailer_task, params))        
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        time.sleep(2)
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            print(task.task_id)
            print(task.progress)
            print(task.status)
            time.sleep(1)

        prog = task.status['progress']
        print("Final progress: {}".format(prog))
        print("Result keys: {} ".format(list(task.result.keys())))
        self.assertEqual(prog,100)


if __name__ == '__main__':
    unittest.main()


