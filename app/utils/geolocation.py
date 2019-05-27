import requests
from ByHelpers import applogger

# Logger
logger = applogger.get_logger()

class Geolocation(object):

    def __init__(self, uri="", protocol="http"):
        """ Init method
        """
        self.uri = uri or "geolocation"
        self.protocol = protocol or "http"
        self.base_url = "{}://{}".format(self.protocol, self.uri)

    def get_retailers(self):
        """ Request list of retailers and set them
            in retailers variables
        """
        self.retailers = requests.get(
            self.base_url\
            +"/retailer/all"
        ).json()
        self.retailers_dict = { i['key'] : i for i in self.retailers }
        return self.retailers

    def get_stores(self, rets=None, active=None):
        """ Static method to retrieve stores by retailer keys

            Params:
            -----
            - rets: (list) List of Retailer keys

            Returns:
            -----
            (list) List of stores

        """
        stores = []
        qry_active = '&active={}'.format(active) if active else ''
        for retailer in rets:
            # Fetch Stores by retailer
            try:
                stores_j = requests\
                    .get(self.base_url+"/store/retailer?key="+retailer+qry_active)\
                    .json()
                logger.debug("Fetched {} stores!".format(retailer))
            except Exception as e:
                logger.error(e)
                return None
            stores += stores_j
        logger.info("Found {} stores".format(len(stores)))
        return stores