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

class GeopriceGeoServicesTestCase(unittest.TestCase):
    """ Test Case for Geoprice Geo Services

        TODO: Set up all the tests of the 
            - alert
            - check
            - dump
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

    def test_00_geo_alert_root_path(self):
        """ Test price Geo Alert Root path
        """
        print(">>>>>", "Test price Geo Alert Root path")
        _res = self.app.get("/geo/alert/")
        print(_res.status_code)
        print(_res.data)
    
    @unittest.skip("TO DO ")
    def test_01_geo_check_root_path(self):
        """ Test price Geo Check Root path
        """
        print(">>>>>", "Test price Geo Check Root path")
        _res = self.app.get("/geo/check/")
        print(_res.status_code)
        print(_res.data)
    
    @unittest.skip("TO DO ")
    def test_02_geo_dump_root_path(self):
        """ Test price Geo Dump Root path
        """
        print(">>>>>", "Test price Geo Dump Root path")
        _res = self.app.get("/geo/dump/")
        print(_res.status_code)
        print(_res.data)

    def test_03_geo_alert_prices_method(self):
        """ Test Geo Alert Prices Method
        """
        print(">>>>>", "Test Geo Alert Prices Method")

        # Filters for the task
        params = {
            "uuids" : ["478c624b-bf0d-4540-bee9-c870ff0e69fd",
                    "c34742b1-9ed7-451c-b0aa-c965e146675b",
                    "6267ab2f-bf96-4c4c-8c12-acac370aebf3"],
            "retailers" : ["walmart","chedraui", "san_pablo"],
            "today" : "2019-05-20"
        }
        _res = self.app.post('/geo/alert/prices',
                    data=json.dumps(params),
                    headers={'content-type': 'application/json'}
        )
        try:
            _jr = json.loads(_res.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_res.status_code, 200)
    
    def test_04_geo_alert_direct_compare_prices_method(self):
        """ Test Geo Alert Prices Compare Method
        """
        print(">>>>>", "Test Geo Alert Prices Compare Method")

        # Filters for the task
        params = {
            "alerts": [
                {
                    "variation": 10,
                    "item_uuid_compare": "078d7bd8-f447-4913-87a4-f6c82860403d",
                    "type": "price",
                    "item_uuid": "b50a332c-9d54-4df7-83ce-f856db0c1f51"
                },
                {
                    "variation": 10,
                    "item_uuid_compare": "b50a332c-9d54-4df7-83ce-f856db0c1f51",
                    "type": "price",
                    "item_uuid": "d4cae6a5-ea9e-40c9-b1d5-6a2ed6656fc5"
                }
            ],
            "retailers": [
                "chedraui",
                "fresko",
                "la_comer",
                "soriana",
                "superama",
                "walmart"
            ],
            "stores": [  "1d69fe8e-7ace-11e7-9b9f-0242ac110003", "1fda295a-7ace-11e7-9b9f-0242ac110003", "1854f516-7ace-11e7-9b9f-0242ac110003", "25d69b9a-7ace-11e7-9b9f-0242ac110003", "24fa3452-7ace-11e7-9b9f-0242ac110003", "26abc004-7ace-11e7-9b9f-0242ac110003", "25bda4c8-7ace-11e7-9b9f-0242ac110003", "2606b6ae-7ace-11e7-9b9f-0242ac110003", "26de097e-7ace-11e7-9b9f-0242ac110003", "2397bcd8-7ace-11e7-9b9f-0242ac110003", "235a75e4-7ace-11e7-9b9f-0242ac110003", "2766ecda-7ace-11e7-9b9f-0242ac110003", "24256984-7ace-11e7-9b9f-0242ac110003", "20da8368-7ace-11e7-9b9f-0242ac110003", "2197d940-7ace-11e7-9b9f-0242ac110003", "1fef5fd2-7ace-11e7-9b9f-0242ac110003", "216b15e0-7ace-11e7-9b9f-0242ac110003", "1bd3e4b8-7ace-11e7-9b9f-0242ac110003", "148e85c8-7ace-11e7-9b9f-0242ac110003", "1471ddd8-7ace-11e7-9b9f-0242ac110003", "1399c6e6-7ace-11e7-9b9f-0242ac110003", "13ccf9bc-7ace-11e7-9b9f-0242ac110003", "14579220-7ace-11e7-9b9f-0242ac110003", "2b2bf360-7ace-11e7-9b9f-0242ac110003", "2e92b78c-7ace-11e7-9b9f-0242ac110003", "2b209d1c-7ace-11e7-9b9f-0242ac110003", "2dc4b3d2-7ace-11e7-9b9f-0242ac110003", "2c2f0c0c-7ace-11e7-9b9f-0242ac110003", "2cdd7ae4-7ace-11e7-9b9f-0242ac110003", "2c11e870-7ace-11e7-9b9f-0242ac110003", "29c33c54-7ace-11e7-9b9f-0242ac110003", "27c3d63e-7ace-11e7-9b9f-0242ac110003", "297d368c-7ace-11e7-9b9f-0242ac110003", "28227824-7ace-11e7-9b9f-0242ac110003", "28525f9e-7ace-11e7-9b9f-0242ac110003", "29d4fb7e-7ace-11e7-9b9f-0242ac110003", "2796bbd6-7ace-11e7-9b9f-0242ac110003", "28a2b890-7ace-11e7-9b9f-0242ac110003", "291a2722-7ace-11e7-9b9f-0242ac110003", "28fff5c8-7ace-11e7-9b9f-0242ac110003", "28e8fec2-7ace-11e7-9b9f-0242ac110003", "28d14e76-7ace-11e7-9b9f-0242ac110003", "27db954e-7ace-11e7-9b9f-0242ac110003", "163f55d2-7ace-11e7-9b9f-0242ac110003", "24728a52-7ace-11e7-9b9f-0242ac110003", "1ecca34e-7ace-11e7-9b9f-0242ac110003", "20c00380-7ace-11e7-9b9f-0242ac110003", "141a3ca4-7ace-11e7-9b9f-0242ac110003", "15ef41f0-7ace-11e7-9b9f-0242ac110003", "143194d0-7ace-11e7-9b9f-0242ac110003", "428759d9-4547-4a7c-9c43-2dd38c29c855", "7c3e6447-4f3e-49de-9728-8a56eac622d4", "41eb4230-3544-4772-9e3c-500bf5343efd", "998f4d92-6ba7-4bd0-8c68-d7781d8a56b6", "bfc3c142-c87a-4e7b-bd46-ff96b68e470e", "f57cecb6-9224-41d0-8690-90fde703bc98", "005e6f0f-4a8b-4d4d-8e48-dd75bb98506d", "689bf7b3-548a-4462-878f-5b6d093b3d87", "e5a01b8d-3666-47f6-935b-9940b2b45ef3", "19f8fb42-4b5b-4579-b2db-4fb9ca6aed9e", "8db44406-84ff-47dc-acc4-ed9a9aa40e0d", "4fea9775-8525-4e53-b8bb-66206c7bb2a3", "022aab9d-946f-4985-a11d-3b775dfd23db", "75646d40-6426-4d87-83b0-a35acbf3433d", "95244d6f-3ffd-4169-b3d2-ca7040b0ee6c", "e76e9a4a-900c-45e8-8121-0afe3748fe89", "271d65ce-7ace-11e7-9b9f-0242ac110003", "29404fa6-7ace-11e7-9b9f-0242ac110003", "228e5a22-7ace-11e7-9b9f-0242ac110003", "5167ad04-d6ea-4c4e-a609-56703ccfb7c3", "07ecc6f8-ad30-4ffe-95ed-1711a8956216", "a12c3017-9022-48a5-8dd1-b107775723c0", "8602eda8-d29e-415e-b0e5-1668ba483b52", "01c39a6b-fc01-4257-a06f-39df1e590763", "7fc17b20-a1ca-4240-90c8-a50f2db5415e", "fe8024d1-e1a7-4d7d-b192-d426189758d5", "3f4b9de4-ecf9-4445-96a7-1dc778d0f9ea", "6923880e-5d84-4f51-8514-206d4dbea722", "f98bb235-02f2-4ff5-af91-d41f6da69e41", "3888ccdc-8373-46a0-b2ce-1e7ede1aa9e0", "e186f873-77a0-4937-a8c6-64f7137c01dc", "4029aae4-5402-4237-95c6-3182ae198500", "4513d5b2-1fc2-4d51-829d-88f8dca90cea", "eee5550f-f608-46f0-b9b8-ae00fa101e75", "ac4cdb92-0488-49c5-86cd-e0a0297202ea", "0d7bf084-e1da-454c-b689-c113c18cd176", "e74b592a-f6c3-4373-92ff-d57f2bd78289", "9ce8da97-2bc1-4f19-b383-0be63114161d", "5516e2e0-8b52-45ae-81bf-91f961e1aa71", "c1ea3464-8768-11e7-8ca7-80e6500152aa", "c2aef61e-8768-11e7-bbe7-80e6500152aa", "c34c3a8c-8768-11e7-a892-80e6500152aa", "61105926-86cc-11e7-87b4-0242ac110002", "60ea4bfa-86cc-11e7-87b4-0242ac110002", "30ddaf14-298d-11e9-884f-0242ac110002", "304b0308-298d-11e9-990c-0242ac110003", "30ad754c-298d-11e9-b29d-0242ac110004", "311b5562-298d-11e9-a5be-0242ac110003", "31d3c0f2-298d-11e9-a5be-0242ac110003", "2fbc36f0-298d-11e9-b29d-0242ac110004", "b727136e-8768-11e7-be8c-80e6500152aa", "b5e152ec-8768-11e7-a41b-80e6500152aa", "b8716788-8768-11e7-a458-80e6500152aa", "b9a95adc-8768-11e7-abcc-80e6500152aa", "371d7d5a-298d-11e9-85fe-0242ac110004", "60749450-86cc-11e7-87b4-0242ac110002", "60413a88-86cc-11e7-87b4-0242ac110002", "6097aeae-86cc-11e7-87b4-0242ac110002", "5fd3c55c-86cc-11e7-87b4-0242ac110002", "325cd57c-298d-11e9-884f-0242ac110002", "35ffc658-298d-11e9-884f-0242ac110002", "a703cae0-7b04-11e7-855a-0242ac110005", "aa6e2374-7b04-11e7-855a-0242ac110005", "87bb3916-7b04-11e7-855a-0242ac110005", "af8787ce-7b04-11e7-855a-0242ac110005", "75be104e-7b04-11e7-855a-0242ac110005", "85712dc8-7b04-11e7-855a-0242ac110005", "b90f1938-7b04-11e7-855a-0242ac110005", "982ae5a8-7b04-11e7-855a-0242ac110005", "7f3277b4-7b04-11e7-855a-0242ac110005", "8033c4f6-7b04-11e7-855a-0242ac110005", "a50f1726-7b04-11e7-855a-0242ac110005", "a9440068-7b04-11e7-855a-0242ac110005", "b36956d8-7b04-11e7-855a-0242ac110005", "b9b00be0-7b04-11e7-855a-0242ac110005", "8f58a19a-7b04-11e7-855a-0242ac110005", "79829f74-7b04-11e7-855a-0242ac110005", "946edadc-7b04-11e7-855a-0242ac110005", "ae93979a-7b04-11e7-855a-0242ac110005", "910e323e-7b04-11e7-855a-0242ac110005", "9536cace-7b04-11e7-855a-0242ac110005", "b0329146-7b04-11e7-855a-0242ac110005", "8a4d4e80-7b04-11e7-855a-0242ac110005", "92a105f4-7b04-11e7-855a-0242ac110005", "93712522-7b04-11e7-855a-0242ac110005", "9f882cac-7b04-11e7-855a-0242ac110005", "871a348a-7b04-11e7-855a-0242ac110005", "a9ab6866-7b04-11e7-855a-0242ac110005", "89883384-7b04-11e7-855a-0242ac110005", "76ba0ef8-7b04-11e7-855a-0242ac110005", "a11e20f8-7b04-11e7-855a-0242ac110005", "a208ff7e-7b04-11e7-855a-0242ac110005", "9bca8952-7b04-11e7-855a-0242ac110005", "976a313c-7b04-11e7-855a-0242ac110005", "a36c064a-7b04-11e7-855a-0242ac110005", "9c9e654c-7b04-11e7-855a-0242ac110005", "99211a2c-7b04-11e7-855a-0242ac110005", "9d6119a2-7b04-11e7-855a-0242ac110005", "96c3ed9a-7b04-11e7-855a-0242ac110005", "9a48de30-7b04-11e7-855a-0242ac110005", "95ec22de-7b04-11e7-855a-0242ac110005", "88798f92-7b04-11e7-855a-0242ac110005", "a03f8398-7b04-11e7-855a-0242ac110005", "a2989062-7b04-11e7-855a-0242ac110005", "a41bda02-7b04-11e7-855a-0242ac110005", "acdd8db6-7b04-11e7-855a-0242ac110005", "b6a82810-7b04-11e7-855a-0242ac110005", "b7fe5e64-7b04-11e7-855a-0242ac110005", "b5c7ff92-7b04-11e7-855a-0242ac110005", "a607e374-7b04-11e7-855a-0242ac110005", "b775d774-7b04-11e7-855a-0242ac110005", "b8bdc466-7b04-11e7-855a-0242ac110005", "adf8a53c-7b04-11e7-855a-0242ac110005", "b28fb28e-7b04-11e7-855a-0242ac110005", "b7bdc188-7b04-11e7-855a-0242ac110005", "b8778b04-7b04-11e7-855a-0242ac110005", "ab410438-7b04-11e7-855a-0242ac110005", "b0eb6b94-7b04-11e7-855a-0242ac110005", "b18934fa-7b04-11e7-855a-0242ac110005", "b422af02-7b04-11e7-855a-0242ac110005", "ac2b23d8-7b04-11e7-855a-0242ac110005", "7759734e-7b04-11e7-855a-0242ac110005", "7a39e468-7b04-11e7-855a-0242ac110005", "82e887cc-7b04-11e7-855a-0242ac110005", "8d193a02-7b04-11e7-855a-0242ac110005", "864329cc-7b04-11e7-855a-0242ac110005", "8b375c0a-7b04-11e7-855a-0242ac110005", "837d26d4-7b04-11e7-855a-0242ac110005", "8e9541b4-7b04-11e7-855a-0242ac110005", "7879786e-7b04-11e7-855a-0242ac110005", "81e2c7e8-7b04-11e7-855a-0242ac110005", "7ca38646-7b04-11e7-855a-0242ac110005", "ba540542-7b04-11e7-855a-0242ac110005", "b9e07fa0-7b04-11e7-855a-0242ac110005", "8de09886-7b04-11e7-855a-0242ac110005", "847e0f9e-7b04-11e7-855a-0242ac110005", "7d446336-7b04-11e7-855a-0242ac110005", "bad978d0-7b04-11e7-855a-0242ac110005", "91e5120e-7b04-11e7-855a-0242ac110005", "90b2e960-7b04-11e7-855a-0242ac110005", "a82b7940-7b04-11e7-855a-0242ac110005", "7b014706-7b04-11e7-855a-0242ac110005", "7bdff258-7b04-11e7-855a-0242ac110005", "ca7a6808-7afa-11e7-8dda-0242ac110005", "d615e34a-7afa-11e7-8dda-0242ac110005", "cdbca35a-7afa-11e7-8dda-0242ac110005", "cf94289c-7afa-11e7-8dda-0242ac110005", "cc5c9484-7afa-11e7-8dda-0242ac110005", "d7647f0e-7afa-11e7-8dda-0242ac110005", "d91c1fd2-7afa-11e7-8dda-0242ac110005", "dc4a22a8-7afa-11e7-8dda-0242ac110005", "dbddb01e-7afa-11e7-8dda-0242ac110005", "db42e02a-7afa-11e7-8dda-0242ac110005", "d11e139e-7afa-11e7-8dda-0242ac110005", "da3fe3a8-7afa-11e7-8dda-0242ac110005", "db0813c8-7afa-11e7-8dda-0242ac110005", "d6bcd5ce-7afa-11e7-8dda-0242ac110005", "d56165b4-7afa-11e7-8dda-0242ac110005", "d26427e8-7afa-11e7-8dda-0242ac110005", "d048a3b2-7afa-11e7-8dda-0242ac110005", "c9837214-7afa-11e7-8dda-0242ac110005", "cd8be21a-7afa-11e7-8dda-0242ac110005", "cb2f10f0-7afa-11e7-8dda-0242ac110005", "d4b678ac-7afa-11e7-8dda-0242ac110005", "d897741c-7afa-11e7-8dda-0242ac110005", "dac9852c-7afa-11e7-8dda-0242ac110005", "dceaee0e-7afa-11e7-8dda-0242ac110005", "dd725cd6-7afa-11e7-8dda-0242ac110005", "d9827674-7afa-11e7-8dda-0242ac110005", "e42cf270-7afa-11e7-8dda-0242ac110005", "e3c1adee-7afa-11e7-8dda-0242ac110005", "f6371806-7afa-11e7-8dda-0242ac110005", "f35528b2-7afa-11e7-8dda-0242ac110005", "e2f09bbe-7afa-11e7-8dda-0242ac110005", "e2bf0bda-7afa-11e7-8dda-0242ac110005", "e1ec4434-7afa-11e7-8dda-0242ac110005", "e1486a1c-7afa-11e7-8dda-0242ac110005", "e0cbe91a-7afa-11e7-8dda-0242ac110005", "e00112c6-7afa-11e7-8dda-0242ac110005", "df1f139e-7afa-11e7-8dda-0242ac110005", "e523e86e-7afa-11e7-8dda-0242ac110005", "e499dd86-7afa-11e7-8dda-0242ac110005", "e5a42358-7afa-11e7-8dda-0242ac110005", "e6b5c828-7afa-11e7-8dda-0242ac110005", "e64dd13c-7afa-11e7-8dda-0242ac110005", "ec58ac8c-7afa-11e7-8dda-0242ac110005", "e7aa210c-7afa-11e7-8dda-0242ac110005", "de5571b0-7afa-11e7-8dda-0242ac110005", "f0d33f2a-7afa-11e7-8dda-0242ac110005", "f5ba2dc8-7afa-11e7-8dda-0242ac110005", "f420159a-7afa-11e7-8dda-0242ac110005", "f5f8aefe-7afa-11e7-8dda-0242ac110005", "f4c9fd1c-7afa-11e7-8dda-0242ac110005", "f532ceb4-7afa-11e7-8dda-0242ac110005", "f2e71fde-7afa-11e7-8dda-0242ac110005", "f2113216-7afa-11e7-8dda-0242ac110005", "f1eb6194-7afa-11e7-8dda-0242ac110005", "ed88d032-7afa-11e7-8dda-0242ac110005", "ee1d5446-7afa-11e7-8dda-0242ac110005", "edd98dce-7afa-11e7-8dda-0242ac110005", "f0821f32-7afa-11e7-8dda-0242ac110005", "ecd632b0-7afa-11e7-8dda-0242ac110005", "e602511c-7afa-11e7-8dda-0242ac110005", "f6cf76a0-7afa-11e7-8dda-0242ac110005", "f6ee63e4-7afa-11e7-8dda-0242ac110005", "eba37cfe-7afa-11e7-8dda-0242ac110005", "ea5e770e-7afa-11e7-8dda-0242ac110005", "e9f1fd18-7afa-11e7-8dda-0242ac110005", "f14fcef0-7afa-11e7-8dda-0242ac110005", "ec0fa1a4-7afa-11e7-8dda-0242ac110005", "e9a71988-7afa-11e7-8dda-0242ac110005", "f73367b4-7afa-11e7-8dda-0242ac110005", "f8750952-7afa-11e7-8dda-0242ac110005", "f9393aca-7afa-11e7-8dda-0242ac110005", "f89ae0be-7afa-11e7-8dda-0242ac110005", "fecfdeb2-7afa-11e7-8dda-0242ac110005", "fd6c65cc-7afa-11e7-8dda-0242ac110005", "fca4981c-7afa-11e7-8dda-0242ac110005", "fde61dd6-7afa-11e7-8dda-0242ac110005", "fd084c5e-7afa-11e7-8dda-0242ac110005", "f99d1dce-7afa-11e7-8dda-0242ac110005", "fe416862-7afa-11e7-8dda-0242ac110005", "fb8110d2-7afa-11e7-8dda-0242ac110005", "f8d3a408-7afa-11e7-8dda-0242ac110005", "fa2b81f4-7afa-11e7-8dda-0242ac110005", "cf3daefe-7afa-11e7-8dda-0242ac110005", "d2d8217a-7afa-11e7-8dda-0242ac110005", "e1af948a-7afa-11e7-8dda-0242ac110005", "e777c004-7afa-11e7-8dda-0242ac110005", "ef508a2c-7afa-11e7-8dda-0242ac110005", "e33533a0-7afa-11e7-8dda-0242ac110005", "d803d824-7afa-11e7-8dda-0242ac110005", "eb3faf8a-7afa-11e7-8dda-0242ac110005", "d39380fa-7afa-11e7-8dda-0242ac110005", "eaadb42c-7afa-11e7-8dda-0242ac110005", "faace122-7afa-11e7-8dda-0242ac110005"]
        }
        _res = self.app.post('/geo/alert/price_compare',
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


