# -*- coding: utf-8 -*-
import app
import config
import unittest
import json
import threading
import time
import sys
from pprint import pprint
from app.models.price import Price

new_price = {
    "route_key" : "price",
    "source" : "walmart",
    "item_uuid" : "605e179d-6ede-48e3-a4fd-820b89718589",
    "product_uuid" : "f0191eed-cae2-4128-b101-3f7a6ed6ec92",
    "gtin" : "07501043100137",
    "id" : "001920001",
    "price" : 80.00,
    "price_original" : 100.00,
    "promo" : "2x10",
    "currency" : "MXN",
    "date" : "2018-05-10 20:54:55",
    "url" : "https://super.walmart.com.mx/Leche/Leche-Alpura-deslactosada-1-l/00750105590142",
    "location" : {
        "store" : [
            "17128984-7ace-11e7-9b9f-0242ac110003",
            "244023aa-7ace-11e7-9b9f-0242ac110003",
            "62dd3a78-4bdd-11e7-a958-0242ac110002",
            "6221c95a-4bdd-11e7-a958-0242ac110002"
        ],
        "zip" : ["14140","01900","14140","01900"],
        "city" : ["México","México","México","México"],
        "state" : ["México","México","México","México"],
        "country" : ["México","México","México","México"],
        "coords" : [
            {
                "lat" : 19.4968732,
                "lng" : -99.72326729999999
            },
            {
                "lat" : 18.9732,
                "lng" : -97.772999
            },
            {
                "lat" : 19.4968732,
                "lng" : -99.72326729999999
            },
            {
                "lat" : 18.9732,
                "lng" : -97.772999
            }
        ]
    }
}

class GeopriceConsumerTestCase(unittest.TestCase):
    """ Test Case for Geoprice Consumer
    """ 

    @classmethod
    def setUpClass(cls):
        """ Initializes the database
        """
        # Define test database
        print("Setting up tests")
        # if config.TESTING:
        #    with app.app.app_context():
        #        app.initdb_cmd()

    @classmethod
    def tearDownClass(cls):
        """ Drops database
        """
        print("Teardown class")
        # if config.TESTING:
        #     with app.app.app_context():
        #         app.dropdb_cmd()

    def setUp(self):
        """ Set up
        """
        # Init Flask ctx
        self.ctx = app.app.app_context()
        self.ctx.push()
        app.get_db()

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()

    def test_price_validation(self):
        """ Testing DB prices i
        """ 
        global new_price
        print("Testing price validation")
        validate = Price.validate(new_price)
        print("------------")
        print(validate)
        self.assertTrue(validate)

    def test_price_save_success(self):
        print("Validating save price success")
        global new_price
        pr = Price(new_price)
        pr.part = 1
        result = pr.save_all()
        self.assertEqual(result, True)

if __name__ == '__main__':
    unittest.main()