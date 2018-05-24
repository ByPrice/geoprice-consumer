import requests


class Geolocation(obj):

    def __init__(self, uri="", protocol="http"):
        """ Init method
        """
        self.uri = uri or "geolocation"
        self.protocol = protocol or "http"
        self.base_url = "{}://{}".format(self.protocol, self.uri)

    @staticmethod
    def get_retailers(self):
        """ Get list of all retailers
        """
        pass

    @staticmethod
    def stores(rets):
        """ Static method to retrieve stores by retailer keys

            Params:
            -----
            - rets: (list) List of Retailer keys

            Returns:
            -----
            (list) List of stores

        """
        stores = []
        for retailer in rets:
            # Fetch Stores by retailer
            try:
                stores_j = requests\
                    .get(self.base_url+"/store/retailer?key="+retailer)\
                    .json()
                logger.debug("Fetched {} stores!".format(retailer))
            except Exception as e:
                logger.error(e)
                return None
            stores += stores_j
        logger.info("Found {} stores".format(len(stores)))
        return stores