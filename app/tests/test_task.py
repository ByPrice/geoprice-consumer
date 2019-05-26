#-*- coding: utf-8 -*-
import app
from app.models.task import Task
import unittest
import config
import uuid
import sys
import time
 
task_id = None


class GeopriceTaskTestCase(unittest.TestCase):
    """ Test Case for Geoprice Async Tasks
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
        print("\n"+"_"*50+"\n")

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()

    @unittest.skip("To be tested later")
    def test_01_save_task_status(self):
        """ Testing DB prices i
        """ 
        print(">>>> Testing create new task and save status")
        global task_id
        task = Task()
        task.status = dict(
            stage='STARTING', 
            progress=1, 
            msg='All set'
        )
        # Save global task_id
        task_id = task.task_id
        self.assertTrue(task_id)

    @unittest.skip("To be tested later")
    def test_02_get_task_status(self):
        """ Get task's status 
        """
        print(">>>> Testing get task status")
        global task_id
        task = Task(task_id)
        status = task.status
        print(status)
        self.assertTrue(type(status) == dict)
    
    @unittest.skip("To be tested later")
    def test_03_set_task_result(self):
        """ Delete task
        """
        print(">>>> Testing setting task result")
        global task_id
        task = Task(task_id)
        # Set last task status
        task.status = {
            "stage" : "COMPLETED",
            "progress" : 100,
            "msg" : "Task completed successfully"
        }
        # Set and save task result
        
        result = {
            "msg" : "OK",
            "data" : { "field{}".format(i) : str(uuid.uuid4()) for i in range(100000) }
        }
        print("Size of saved result: {}".format(sys.getsizeof(str(result))))
        task.result = result
        self.assertTrue(True)

    @unittest.skip("To be tested later")
    def test_04_get_task_result(self):
        """ Change task status
        """
        print(">>>> Testing getting task result")
        global task_id
        task = Task(task_id)
        result = task.result
        print("Size of queried result: {}".format(sys.getsizeof(str(result))))
        self.assertEqual(type(result), dict)

    @unittest.skip("To be tested later")
    def test_05_complete_task_status(self):
        """ Testing task with celery
        """
        print(">>>> Testing complete task result")
        # Import celery task
        from app.celery_tasks import test_task 
        params = {"test_param" : "Hello World!"}
        c_task = test_task.apply_async(args=(params,))

        # Get the task from the celery task
        time.sleep(5)
        task = Task(c_task.id)

        # Check result of task
        while task.status['progress'] < 100:
            print("Waiting for task to finish")
            print(task.status)
            time.sleep(2)

        prog = task.status['progress']
        print("Final progress: {}".format(prog))

        self.assertEqual(prog,100)

    @unittest.skip("DEPRECATED")
    def test_06_complete_task_price_map(self):
        """ Test price
        """
        raise Exception("DEPRECATED TEST!")
        # Import celery task
        from app.celery_tasks import test_task 
        # Filters for the task
        filters = [
            {"item_uuid" : ""},
            {"item_uuid" : ""},
            {"retailer" : ""}
        ]
        params = {
            "filters" : filters,
            "retailers" : ["walmart","superama"],
            "date_start" : "2018-05-25",
            "date_end" : "2018-05-29",
            "interval" : "day"
        }
        c_task = price_map.apply_async(args=(params,))

        # Get the task from the celery task
        task = Task(c_task.id)

        # Check result of task
        while task.status['progress'] < 100:
            print("Waiting for task to finish")
            print(task.status)
            time.sleep(2)

        prog = task.status['progress']
        print("Final progress: {}".format(prog))

        self.assertEqual(prog,100)

    def test_07_complete_task_price_map_decorator(self):
        """ Test price Map Decorator
        """
        print(">>>>>", "Test price Map Decorator")
        # Import celery task
        from app.celery_app import main_task
        from app.models.map import Map

        # Filters for the task
        params = {
            "filters" : [
                {"item_uuid" : "b50a332c-9d54-4df7-83ce-f856db0c1f51"},
                {"item_uuid" : "7f177768-cd76-45e4-92ac-9bab4ec8d8b3"},
                {"item_uuid" : "63aa59b6-04a7-45ed-99df-8f6d1403c4de"},
                {"item_uuid" : "facdc537-d80f-447e-9d6e-0266e0e9d082"},
                {"retailer" : "walmart"}
            ],
            "retailers" : {
                "walmart" : "Walmart",
                "superama" : "Superama"
            },
            "date_start" : "2019-04-10",
            "date_end" : "2019-04-13",
            "interval" : "day"
        }

        celery_task = main_task.apply_async(args=(Map.start_task, params))        
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
    
    
    def test_08_fail_task_price_map_decorator(self):
        """ Test Map task without filters
        """
        print(">>>>", "Test Map task without filters")
        # Import celery task
        from app.celery_app import main_task
        from app.models.map import Map

        # Filters for the task -> missing filters
        params = {
            "retailers" : {
                "walmart" : "Walmart",
                "superama" : "Superama"
            },
            "date_start" : "2019-01-10",
            "date_end" : "2019-01-13",
            "interval" : "day"
        }

        celery_task = main_task.apply_async(args=(Map.start_task,params))        
        task = Task(celery_task.id)
        print("Submitted Task: ", celery_task.id) 
        # Check result of task
        while task.is_running():
            time.sleep(1)

        prog = task.status['progress']
        print("Final progress: {}".format(prog))
        print("Result keys: {} ".format(list(task.result.keys())))
        
        self.assertEqual(prog,-1)


if __name__ == '__main__':
    unittest.main()


