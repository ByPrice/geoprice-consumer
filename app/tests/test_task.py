#-*- coding: utf-8 -*-
import app
from app.models.task import Task
import unittest
import config

task_uuid = None


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
        print("Teardown class")
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
        print("Testing create new task and save status....")
        global task_uuid
        task = Task()
        task.status = dict(
            text='STARTING', 
            progress=1, 
            msg='All set'
        )
        # Save global task_id
        task_uuid = task.task_uuid
        self.assertTrue(task_uuid)

    def test_02_get_task_status(self):
        """ Get task's status 
        """
        print("Testing get task status....")
        global task_uuid
        task = Task(task_uuid)
        status = task.status
        print(status)
        self.assertTrue(type(status) == dict)
    

    def test_03_delete_task(self):
        """ Delete task
        """
        #print("Testing delete task....")
        pass

    def test_04_change_task_status(self):
        """ Change task status
        """
        #print("Testing change task status....")
        pass

    def test_05_complete_task_status(self):
        """ Complete task progress and status
        """
        pass

if __name__ == '__main__':
    unittest.main()