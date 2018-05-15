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
from app.norm import map_product_keys as mpk

new_price = {
    "route_key" : "price",
    "retailer" : "walmart",
    "item_uuid" : "f0191eed-cae2-4128-b101-3f7a6ed6ec92",
    "product_uuid" : "4730c33-7a98-48dd-bbbd-1165f8058916",
    "gtin" : "07501043100137",
    "id" : "001920001",
    "price" : 80.00,
    "price_original" : 100.00,
    "promo" : "2x10",
    "date" : "2018-05-09 20:54:55",
    "location" : {
        "store" : [
            "3b005588-be71-11e7-bb47-0242ac110002",
            "39fd55b4-be71-11e7-bb47-0242ac110002"
        ],
        "zip" : ["14140","01900"],
        "city" : ["México","México"],
        "state" : ["México","México"],
        "country" : ["México","México"],
        "coords" : [
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


class TestStreamer(threading.Thread):
    """ Testing Streamer Thread
    """

    def __init__(self):
        threading.Thread.__init__(self)      

    def run(self):
        print('Async Streaming Test Price!')
        ## Run consumer
        i = 0
        for i in range(1000000):
            print(i)

class GeopriceConsumerTestCase(unittest.TestCase):
    """ Test Case for Geoprice Consumer
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
        """ Set up
        """
        # Init Flask ctx
        self.ctx = app.app.app_context()
        self.ctx.push()
        app.get_db()

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()

    def test_price_validation_succes(self):
        """ Testing DB prices i
        """ 
        global new_price
        print("Testing price validation")
        
        validate = Price.validate(new_price)
        self.assertTrue(validate)

    def test_price_validation_fail(self):
        global new_price
        print("Testing price validation failure!")

    def test_price_save_success(self):
        print("Validating save price success")
        global new_price
        pr = Price(new_price)
        result = pr.save()
        self.assertEqual(result, True)

