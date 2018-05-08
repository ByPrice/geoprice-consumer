from flask import g
import requests
import json


class Source(object):

    def __init__(self):
        pass

    def get_stores(self):
        """ Given a data_source, if it is a retailer
            get all its stores.
        """
        stores = []
        return stores