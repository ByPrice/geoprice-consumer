# -*- coding: utf-8 -*-
import unittest
import config
import time
import json
import app
from app.models.task import Task
from app.celery_app import main_task
from app.models.stats import Stats

task_id = None


class GeopriceStatsTasksTestCase(unittest.TestCase):
    """ Test Case for Geoprice Async Tasks  (Stats)

        TODO: Set up all the tests of the `stats` module, for all the async tasks
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
        self.app = app.app.test_client()
        print("\n" + "_" * 50 + "\n")

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()

    #@unittest.skip('TODO')
    def test_01_retailer_current_submit(self):
        """ Test /stats/current/submit endpoint
        """
        print(">>>>>", "Test retailer current stats")

        # Filters for the task
        params = {
            "filters":
                [
                    {"item_uuid": "98440d28-64be-4994-8244-2b2aa57b0c1a"},
                    {"item": "3a8b8a6f-82df-4bbd-84bf-3d291f0a3b29"},
                    {"item": "decd74df-6a9d-4614-a0e3-e02fe13d1542"},
                    {"retailer": "san_pablo"},
                    {"retailer": "chedraui"},
                    {"retailer": "walmart"},
                    {"retailer": "superama"}
                ],
            "export": False
        }
        celery_task = main_task.apply_async(args=(Stats.get_actual_by_retailer_task, params))
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            time.sleep(1)
            print(task.task_id, task.progress, task.status['stage'])

        progress = task.status['progress']
        print("Final progress: {}".format(progress))
        self.assertEqual(progress, 100)
        self.assertNotIn('error', task.result['data'])
        self.assertIsInstance(task.result['data'], list)

    #@unittest.skip('TODO')
    def test_03_stats_history(self):
        """ Test /stats/history endpoint
        """
        print(">>>>>", "Test history stats")

        # Filters for the task
        params = {
            "filters":
                [
                    {"product": "3c9beac2-a944-4f05-871c-7cee6afd47d6"},
                    {"item": "930d055d-d781-40bd-8f9c-93e9722046bd"},
                    {"retailer": "superama"},
                    {"retailer": "walmart"},
                    {"retailer": "san_pablo"},
                    {"retailer": "chedraui"}
                ],
            "client": "chedraui",
            "date_start": "2019-05-27",
            "date_end": "2019-06-30",
            "interval": "day"
        }
        celery_task = main_task.apply_async(args=(Stats.get_historics, params))
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            time.sleep(1)
            print(task.task_id, task.progress, task.status['stage'])

        progress = task.status['progress']
        print("Final progress: {}".format(progress))

        self.assertEqual(progress, 100)
        self.assertIn('metrics', task.result['data'])
        self.assertNotIn('error', task.result['data'])

    #@unittest.skip('TODO')
    def test_04_stats_category(self):
        """ Test /stats/category endpoint
        """
        print(">>>>>", "Test category stats")

        # Filters for the task
        params = {
            "filters": [
                {"item": "98440d28-64be-4994-8244-2b2aa57b0c1a"},
                {"item": "56e67b35-d27e-4cac-9e91-533e0578b59c"},
                {"item": "3a8b8a6f-82df-4bbd-84bf-3d291f0a3b29"},
                {"item": "decd74df-6a9d-4614-a0e3-e02fe13d1542"},
                {"item": "62ec9ad5-2c26-483e-8413-83499d5eef04"},
                {"retailer": "f_ahorro"},
                {"retailer": "la_comer"}
            ]
        }

        celery_task = main_task.apply_async(args=(Stats.get_count_by_cat, params))
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            time.sleep(1)
            print(task.task_id, task.progress, task.status['stage'])

        progress = task.status['progress']
        print("Final progress: {}".format(progress))
        self.assertEqual(progress, 100)
        self.assertIn('name', task.result['data'][0])

    #@unittest.skip('TODO')
    def test_05_stats_direct_compare(self):
        """ Test /stats/direct_compare endpoint
        """
        print(">>>>>", "Test direct compare stats")

        # Filters for the task
        params = {
            "client": " ",
            "export": False,
            "filters": [
                {
                    "retailer": "san_pablo"
                },
                {
                    "retailer": "soriana"
                },
                {"item": "98440d28-64be-4994-8244-2b2aa57b0c1a"},
                {"item": "56e67b35-d27e-4cac-9e91-533e0578b59c"},
                {"item": "3a8b8a6f-82df-4bbd-84bf-3d291f0a3b29"},
                {"item": "decd74df-6a9d-4614-a0e3-e02fe13d1542"},
                {"item": "62ec9ad5-2c26-483e-8413-83499d5eef04"}
            ],
            "date_start": "2019-05-30",
            "date_end": "2019-06-05",
            "ends": False,
            "interval": "day"
        }
        celery_task = main_task.apply_async(args=(Stats.get_matched_items_task, params))
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            time.sleep(1)
            print(task.task_id, task.progress, task.status['stage'])

        progress = task.status['progress']
        print("Final progress: {}".format(progress))
        print(task.result)
        self.assertEqual(progress, 100)
        self.assertNotIn('error', task.result)
        self.assertIn('items', task.result['data'])

    #@unittest.skip('TODO')
    def test_06_stats_compare(self):
        """ Test /stats/compare endpoint
        """
        print(">>>>>", "Test compare stats")

        # Filters for the task
        params = {
            "client": " ",
            "export": False,
            "filters": [
                {"retailer": "san_pablo"},
                {"retailer": "soriana"},
                {"retailer": "san_pablo"},
                {"retailer": "chedraui"},
                {"retailer": "walmart"},
                {"retailer": "superama"},
                {"item": "98440d28-64be-4994-8244-2b2aa57b0c1a"},
                {"item": "56e67b35-d27e-4cac-9e91-533e0578b59c"},
                {"item": "3a8b8a6f-82df-4bbd-84bf-3d291f0a3b29"},
                {"item": "decd74df-6a9d-4614-a0e3-e02fe13d1542"}
            ],
            "date_start": "2019-05-31",
            "date_end": "2019-06-05",
            "ends": False,
            "interval": "day"
        }
        celery_task = main_task.apply_async(args=(Stats.get_comparison_task, params))
        print("Submitted Task: ", celery_task.id)
        # Get the task from the celery task
        task = Task(celery_task.id)
        print('Created task instance!')

        # Check result of task
        while task.is_running():
            print("Waiting for task to finish")
            time.sleep(1)
            print(task.task_id, task.progress, task.status['stage'])

        progress = task.status['progress']
        print("Final progress: {}".format(progress))
        self.assertEqual(progress, 100)
        self.assertNotIn('error', task.result['data'])
        self.assertIsInstance(task.result['data'], list)
        self.assertIn('gtin', task.result['data'][0])
        print(task.result['data'])


if __name__ == '__main__':
    unittest.main()
