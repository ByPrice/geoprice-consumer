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
    def get_by_product(p_uuids, cols=['item_uuid']):
        """ Get list item_uuids from an item,
            given a product_uuids

            Params:
            -----
            p_uuids : list
                List of Product UUIDs
            cols : list
                List of additional columns to call
            
            Returns: 
            -----
            items : list
                List of product with respective cols
        """
        _k, items = 100, []
        # Iterate over batches of lenght: _k
        for i in range(0, len(p_uuids), _k):
            _pbatch = p_uuids[i: i+_k]
            url = SRV_CATALOGUE + \
                '/product/by/puuid?keys={item}&ipp={ipp}&cols={cols}'\
                .format(item=','.join(_pbatch),
                        ipp=_k,
                        cols=','.join(cols))
            logger.debug(url)
            try:
                r = requests.get(url)
                logger.debug(r.status_code)
                # In case of error
                if r.status_code != 200:
                    raise Exception('Issues requesting Catalogue')
                items += r.json()['products']
            except Exception as e:
                logger.error(e)
        logger.info("Found {} products from Catalogue"\
            .format(len(items)))
        return items

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
            .format(item=item_uuid,
                    cols=','.join(cols))
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

    
    @staticmethod
    def get_item_details(filters):
        """ Get item details from catalogue service
            
            Params:
            -----
            - filters: (list) Item Filters

            Returns: 
            -----
            (list) List of Items with Name and GTIN
        """
        # Fetch uuids from filters in ITEM
        payload  = json.dumps(filters)
        url = 'http://'+config.SRV_CATALOUGUE+'/item/filtered'
        headers = {'content-type':'application/json'}
        logger.debug(url)
        try:
            resp = requests.request("POST", url, data=payload, headers=headers)
            logger.debug(resp.status_code) 
            return resp.json()
        except Exception as e:
            logger.error(e)
            g.error = {'code': 10001,'message':'Issues fetching info...'}
            return False