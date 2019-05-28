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

class GeopriceStatsTasksTestCase(unittest.TestCase):
    """ Test Case for Geoprice Async Tasks  (Stats)
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

    def test_01_complete_task_stats_current(self):
        """ Test price Stats Current Task
        """
        print(">>>>>", "Test price Stats Current Task")
        # Import celery task
        from app.celery_app import main_task
        from app.models.stats import Stats

        # Filters for the task
        params = {
            "filters" : [
                { "category" : "9406" },
                { "retailer" : "superama" },
                { "retailer" : "ims" },
                { "item" : "08cdcbaf-0101-440f-aab3-533e042afdc7" }  
            ],
            "export": True
		}
        celery_task = main_task.apply_async(args=(Stats.start_task, params))        
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


