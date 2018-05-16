#-*- coding: utf-8 -*-
from app.models.task import Task
import unittest



class GeopriceTaskTestCase(unittest.TestCase):
    """ Test Case for Geoprice Consumer
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the database
        """
        # Define test database
        print("Setting up tests")
        if config.TESTING:
            with app.app.app_context():
                app.get_redis()
                print("Connected to Redis")
                app.get_consumer()
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

    def test_create_new_task(self):
        """ Testing DB prices i
        """ 
        global new_price
        print("Testing price validation")
        validate = Price.validate(new_price)
        self.assertTrue(validate)

    def test_delete_task(self):
        """ Delete task
        """
        pass

    def test_change_task_status(self):
        """ Change task status
        """
        pass

    def test_complete_task_status(self):
        """ Complete task progress and status
        """
        pass