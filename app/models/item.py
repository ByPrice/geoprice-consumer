import json
from flask import g
import requests
from app import logger
from config import SRV_CATALOGUE, SRV_PROTOCOL


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
            url = SRV_PROTOCOL + '://' + SRV_CATALOGUE + \
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
        logger.info("Found {} products from Catalogue"
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

        url = SRV_PROTOCOL + '://' + SRV_CATALOGUE + \
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
                {j: x for j, x in p.items()
                    if j in cols}
            )
        return products

    @staticmethod
    def get_by_items_and_retailers(items, retailers):
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

        chunk_size = 100

        chunk_items = Item.divide_chunks(items, chunk_size)

        products = []

        for items in chunk_items:
            logger.debug('chunk ')
            url = SRV_PROTOCOL + '://' + SRV_CATALOGUE + \
                '/product/by/items_and_retailers?items={items}&retailers={retailers}'\
                .format(items=','.join(items),
                        retailers=','.join(retailers))
            logger.debug(url)
            try:
                r = requests.get(url)
                logger.debug(r.status_code)
                # In case of error
                if r.status_code != 200:
                    raise Exception('Issues requesting Catalogue')
                products += r.json()['products']
                logger.debug('prods')
                logger.debug(products)
            except Exception as e:
                logger.error(e)
                return []
        
        logger.debug('Finished get_by_items_and_retailers')
        logger.debug(products)
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
        payload = json.dumps(filters)
        url = SRV_PROTOCOL + '://'+SRV_CATALOGUE+'/item/filtered'
        headers = {'content-type': 'application/json'}
        logger.debug(url)
        try:
            resp = requests.request("POST", url, data=payload, headers=headers)
            logger.debug(resp.status_code)
            return resp.json()
        except Exception as e:
            logger.error(e)
            g.error = {'code': 10001, 'message': 'Issues fetching info...'}
            return False

    @staticmethod
    def get_all_items(ip=1, _ipp=500):
        """ Get the number of items from the catalogue

            Params:
            -----
            ip :  int
                page number
            ipp : int
                number of results

            Returns: 
            -----
            items : list
                List of product with respective cols
        """

        items = []

        url = SRV_PROTOCOL + "://" + SRV_CATALOGUE + \
            '/product/by/puuid?keys=''&p={p}&ipp={ipp}&orderby=name'\
            .format(p=ip,
                    ipp=_ipp)
        logger.debug(url)
        try:
            r = requests.get(url)
            logger.debug(r.status_code)
            # In case of error
            if r.status_code != 200:
                raise Exception('Issues requesting Catalogue')
            items = r.json()['products']
        except Exception as e:
            logger.error(e)

        logger.info("Found {} products from Catalogue"
                    .format(len(items)))
        return items

    def divide_chunks(l, n): 
    
    # looping till length l 
    for i in range(0, len(l), n):
        if len(l[i:i + n]) > 1:
            yield l[i:i + n]
        else:
            yield (l[i-1:i+1])