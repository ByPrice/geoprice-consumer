import requests

class Catalogue(object):

    def __init__(self, uri="", protocol="http"):
        """ Constructor method
        """
        self.uri = uri or "geolocation"
        self.protocol = protocol or "http"
        self.base_url = "{}://{}".format(self.protocol, self.uri)
        self.auth = None
    
    def get_item_details(
        self, 
        values=None, 
        by='item_uuid', 
        cols=['item_uuid','gtin','name'], 
        loop_size=10
        ):
        """ Get item details by a given field 
            that equals a given value.
        """
        if not values:
            logger.error("Set the values to obtain")
            return False
        qry_cols = ','.join(cols)
        # Get chunks of n size for the values
        chunks = [values[i:i + loop_size] for i in range(0, len(values), loop_size)]
        # Loop through chunks
        result = []
        for chunk in chunks:
            qry_items=','.join(chunk)
            # Make the request
            resp = requests.get(self.base_url+'/item/details').json()
            logger.info("Result from catalogue /item/details: {}".format(resp.status_code))
            items = resp.json()
            result += items

        return result
        

    def get_products_by_item(self, item_uuid, cols=['product_uuid']):
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


    def get_intersection(self, **kwargs):
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

        if 'cols' in kwargs:
            cols = kwargs['cols'].join(",")
        else:
            cols = 'product_uuid,retailer'

        # Build the query
        q_arr = [ "{}={}".format(k, ",".join(vals) for k,vals in kwargs ) ]
        q =  "&".join(q_arr)
        q = "?"+q

        # Pagination
        p=1
        ipp=50

        try:
            nxt = True
            while nxt:  
                r = requests.get(self.base_url+q+"&p={}&ipp={}".format(p,ipp))
                if r.status_code != 200:
                    raise Exception("Could not fetch product intersection")
                page_prods = r.json()['products'] 
                if not page_prods:
                    nxt = False
                prods += page_prods
                p+=1
                
        except Exception as e:
            logger.error(e)
            return False

        return products

    
