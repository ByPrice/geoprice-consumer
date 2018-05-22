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
    """ Test Case for Geoprice Consumer
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
                app.get_consumer(queue='test_queue')
                print("Connected to RabbitMQ")

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

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()

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

    def test_02_get_task_status(self):
        """ Get task's status 
        """
        print(">>>> Testing get task status")
        global task_id
        task = Task(task_id)
        status = task.status
        print(status)
        self.assertTrue(type(status) == dict)
    
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

    def test_04_get_task_result(self):
        """ Change task status
        """
        print(">>>> Testing getting task result")
        global task_id
        task = Task(task_id)
        result = task.result
        print("Size of queried result: {}".format(sys.getsizeof(str(result))))
        self.assertEqual(type(result), dict)

    def test_05_complete_task_status(self):
        """ Testing task with celery
        """
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

    def test_06_start_async_task(self):
        """ Start async_task with celery
        """
        pass


if __name__ == '__main__':
    unittest.main()