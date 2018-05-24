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
            items = requests.get(self.base_url+'/item/details').json()
            result += items

        return result
        
