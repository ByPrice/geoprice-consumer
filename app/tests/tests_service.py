# -*- coding: utf-8 -*-
import app
import config
import unittest
import json
from pprint import pprint
import io
import pandas as pd


_testing_item = '5630f780-1952-465b-b38c-9f02f2b0e24d'
_test_ret = 'walmart'
_test_store = '16faeaf4-7ace-11e7-9b9f-0242ac110003'
_test_start_date = '2018-05-10'
_test_end_date = '2018-05-11'
_test_time = '2018-05-11 10:00:00'


class GeoPriceServiceTestCase(unittest.TestCase):
    """ Test Case for GeoPrice Service
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the database
        """
        # Define test database
        if config.TESTING:
            with app.app.app_context():
                app.initdb()

    @classmethod
    def tearDownClass(cls):
        """ Drops database
        """
        if config.TESTING:
            with app.app.app_context():
                app.dropdb()

    def setUp(self):
        """ Generating Flask App Client 
            context for testing
        """
        print("\n***************************\n")
        self.app = app.app.test_client()

    def tearDown(self):
        pass

    ##
    # ----------- PRODUCT TESTS -----------------
    ##

    @unittest.skip('Already tested')
    def test_00_geoprice_connection(self):
        """ Testing GeoPrice DB connection
        """ 
        print("Testing GeoPrice DB connection")
        _r =  self.app.get('/product/one')
        print(_r.status_code)
        try:
            print(json.loads(_r.data.decode('utf-8')))
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Already tested')
    def test_01_by_store_with_item(self):
        """ Test By Store endpoint with Item UUID
        """ 
        print("Test By Store endpoint with Item UUID")
        _r = self.app.get("/product/bystore?uuid={}"\
            .format(_testing_item))
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    @unittest.skip('Not yet tested')
    def test_02_by_store_with_prod(self):
        """ Test By Store endpoint with Product UUID
        """
        print("Test By Store endpoint with Product UUID")
        pass

    @unittest.skip('Already tested')
    def test_03_by_store_history_with_item(self):
        """ Test By Store History endpoint with Item UUID
        """ 
        print("Test By Store History endpoint with Item UUID")
        _r = self.app.get("/product/bystore/history?uuid={}"\
            .format(_testing_item))
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Not yet tested')
    def test_04_by_store_history_with_prod(self):
        """ Test By Store History endpoint with Product UUID
        """
        print("Test By Store History endpoint with Product UUID")
        pass

    @unittest.skip('Tested already')
    def test_05_ticket_with_item(self):
        """ Test Ticket endpoint with Item UUIDs
        """ 
        print("Test Ticket endpoint with Item UUIDs")
        _r = self.app.post("/product/ticket",
            data=json.dumps(
                {'uuids':[_testing_item]}
                ),
            headers={'content-type': 'application/json'}
            )
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Not yet tested')
    def test_06_ticket_with_prod(self):
        """ Test Ticket endpoint with Product UUIDs
        """ 
        print("Test Ticket endpoint with Product UUIDs")
        pass
    
    @unittest.skip('Tested already')
    def test_07_store_catalogue(self):
        """ Test store prices catalogue endpoint
        """ 
        print("Test store prices catalogue endpoint")
        _r = self.app.get("/product/catalogue?r={}&sid={}"\
            .format(_test_ret, _test_store))
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    @unittest.skip('Tested already')
    def test_08_count_by_store(self):
        """ Test Count by Store
        """ 
        print("Test Count by Store")
        _r = self.app\
            .get("/product/count_by_store?r={}&sid={}&date_start={}&date_end={}"\
            .format(_test_ret, _test_store,
                _test_start_date, _test_end_date))
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    @unittest.skip('Tested already')
    def test_09_count_by_store_hours(self):
        """ Test Count by Store over last X hours
        """ 
        print("Test Count by Store over last X hours")
        _r = self.app\
            .get("/product/count_by_store_hours?r={}&sid={}&last_hours={}"\
            .format(_test_ret, _test_store, 24))
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Tested already')
    def test_10_byfile(self):
        """ Test Store by File
        """ 
        print("Test Store by File")
        _r = self.app\
            .get("/product/byfile?ret={}&sid={}&stn={}"\
            .format(_test_ret, _test_store,
                    'Universidad'))
        print('Status code', _r.status_code)
        try:
            print(_r.data)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    @unittest.skip('Tested already')
    def test_11_retailer_with_item(self):
        """ Test Retailer prices by Item
        """ 
        print("Test Retailer prices by Item")
        _r = self.app\
            .get("/product/retailer?retailer={}&item_uuid={}&export={}"\
                .format(_test_ret, _testing_item, True))
        print('Status code', _r.status_code)
        try:
            print(_r.data)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    @unittest.skip('Not yet tested')
    def test_12_retailer_with_prod(self):
        """ Test Retailer prices by Product
        """ 
        print("Test Retailer prices by Product")
    
    @unittest.skip('Tested already')
    def test_13_compare_retailer_item(self):
        """ -Test Compare retailer-price pairs
        """ 
        print("Test Compare retailer-price pairs")
        _r = self.app.post(
            "/product/compare/details",
            headers={'content-type': 'application/json'},
            data=json.dumps({
                "date": "2018-05-10",
                "fixed_segment" : {
                    "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                    "retailer": "chedraui"
                    },
                "added_segments": [
                    { 
                        "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                        "retailer": "walmart"
                    },
                    {
                        "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                        "retailer": "soriana"
                    }
                ]
            })
        )
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Tested already')
    def test_13_compare_store_item(self):
        """ Test Compare retailer-price pairs
        """ 
        print("Test Compare retailer-price pairs")
        _r = self.app.post(
            "/product/compare/history",
            headers={'content-type': 'application/json'},
            data=json.dumps({
                "date_ini": "2017-12-01",
                "date_fin": "2017-12-07",
                "interval": "day",
                "fixed_segment" : {
                    "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                    "retailer": "chedraui",
                    "store_uuid": "e02a5370-7b09-11e7-855a-0242ac110005",
                    "name": "CHEDRAUI SELECTO UNIVERSIDAD"
                },
                "added_segments": [
                    {
                        "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                        "retailer": "walmart",
                        "store_uuid": "16faeaf4-7ace-11e7-9b9f-0242ac110003",
                        "name": "Walmart Universidad"
                    },
                    {
                        "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                        "retailer": "walmart",
                        "store_uuid": "16faeaf4-7ace-11e7-9b9f-0242ac110003",
                        "name": "Walmart Universidad"
                    }
                ]
            })
        )
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Tested already')
    def test_14_stats(self):
        """ Test Stats per product
        """ 
        print("Test Stats per product")
        _r = self.app.get(
            "/product/stats?item_uuid={}"\
                .format(_testing_item))
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Tested already')
    def test_15_count_engine(self):
        """ Test Count by retailer (Engine)
        """ 
        print("Test Count by retailer (Engine)")
        _r = self.app.get(
            "/product/count_by_retailer_engine?retailer={}&date={}"\
                .format(_test_ret, _test_time))
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    ##
    # ----------- STATS TESTS -----------------
    ##
    #@unittest.skip('Tested already')
    def test_16_stats_blueprint(self):
        """ Test stats blueprint
        """ 
        print("Test stats blueprint")
        _r = self.app.get("/stats/")
        print('Status code', _r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

if __name__ == '__main__':
    unittest.main()