import json
from flask import g
import requests
from app import logger
from config import SRV_CATALOGUE



class Item(object):
    """ Class to get the equivalences between 
        the products and the items
    """

    def __int__(self):
        self.products = []
        pass


    @staticmethod
    def get_by_product(product_uuid, cols=['item_uuid']):
        """ Get list products from an item,
            given a product_uuid
        """
        pass 

    @staticmethod
    def get_by_item(item_uuid, cols=['product_uuid']):
        """ Get list of products given an 
            item_uuid, with specified table columns.

            Params:
            -----
            item_uuid : str
                Item UUID
            cols : list
                List of requested table columns
            
            Returns: 
            -----
            products : list
                List requested of products
        """
        url = SRV_CATALOGUE + \
            '/product/by/iuuid?keys={item}&ipp=50&cols={cols}'\
            .format(item=item_uuid, cols=','.join(cols))
        logger.debug(url)
        try:
            r = requests.get(url)
            logger.debug(r.status_code)
            # In case of error
            if r.status_code != 200:
                raise Exception('Issues requesting Catalogue')
        except Exception as e:
            logger.error(e)
            return []
        # Format response
        products = []
        for p in r.json()['products']:
            products.append(
                {j:x for j, x in p.items() \
                    if j in cols}
            )
        return products