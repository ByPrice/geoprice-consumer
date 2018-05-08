from flask import g
import json
import requests



class Item(object):
    """ Class to get the equivalences between 
        the products and the items
    """

    def __int__(self):
        self.products = []
        pass


    @staticmethod
    def get_by_product(product_uuid):
        """ Get list products from an item,
            given a product_uuid
        """
        pass 

    @staticmethod
    def get_by_item(item_uuid):
        """ Get list of products given an 
            item_uuid
        """
        pass 
