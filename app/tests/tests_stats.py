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

    @unittest.skip('TODO')
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
        _res = self.app.post('/stats/history',
                             data=json.dumps(params),
                             headers={'content-type': 'application/json'}
                             )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)
        self.assertIn('metrics', _jr)
        self.assertNotIn('error', _jr)

    @unittest.skip('TODO')
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
                {"retailer": "san_pablo"},
                {"retailer": "comercial_mexicana"},
                {"retailer": "farmatodo"},
                {"retailer": "f_ahorro"},
                {"retailer": "soriana"},
                {"retailer": "superama"},
                {"retailer": "la_comer"},
                {"retailer": "walmart"}
            ]
        }
        _res = self.app.post('/stats/category',
                             data=json.dumps(params),
                             headers={'content-type': 'application/json'}
                             )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)

        self.assertNotIn('error', _jr)
        self.assertIn('name', _jr[0])

    @unittest.skip('TODO')
    def test_05_stats_direct_compare(self):
        """ Test /stats/direct_compare endpoint
        """
        print(">>>>>", "Test direct compare stats")

        # Filters for the task
        params = {
            "client": " ",
            "export": True,
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
        _res = self.app.post('/stats/direct_compare',
                             data=json.dumps(params),
                             headers={'content-type': 'application/json'}
                             )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)

        self.assertNotIn('error', _jr)
        self.assertIn('items', _jr)

    @unittest.skip('TODO')
    def test_06_stats_compare(self):
        """ Test /stats/compare endpoint
        """
        print(">>>>>", "Test compare stats")

        # Filters for the task
        params = {
            "filters":
                [
                    {"item": "98440d28-64be-4994-8244-2b2aa57b0c1a"},
                    {"item": "56e67b35-d27e-4cac-9e91-533e0578b59c"},
                    {"category": "2388"},
                    {"retailer": "superama"},
                    {"retailer": "walmart"},
                    {"retailer": "san_pablo"},
                    {"retailer": "soriana"},
                    {"retailer": "city_market"},
                    {"retailer": "f_ahorro"},
                    {"retailer": "la_comer"}
                ],
            "client": "",
            "date_start": "2019-05-28",
            "date_end": "2019-05-30",
            "ends": False,
            "interval": "day",
            "export": False
        }
        _res = self.app.post('/stats/compare',
                             data=json.dumps(params),
                             headers={'content-type': 'application/json'}
                             )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)

        self.assertNotIn('error', _jr)
        self.assertIn('gtin', _jr[0])


if __name__ == '__main__':
    unittest.main()
