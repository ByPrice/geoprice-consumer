from flask import g
import datetime
from uuid import UUID
from app.utils.helpers import tupleize_date
from app import logger, errors

class Check(object):
    """ Class to check validation of prices
    """

    @staticmethod
    def valid_stores(retailer):
        """ Verify with are valid stores from
            a given retailer

            Params:
            -----
            retailer: str
                Retailer key

            Returrns:
            list
                List of valid stores
        """
        # Get the list of active stores from geolocation
        stores = g._geolocation.get_stores([retailer])
        logger.debug("Got {} total stores".format(len(stores)))
        # Time
        _now = datetime.datetime.utcnow()
        then = _now - datetime.timedelta(days=3)
        if not stores:
            logger.warning("No stores for given retailer : %s" % retailer)
            raise errors.AppError(
                "price_geo_error",
                "Could not get stores from geolocation service"
            )
        # For every store, get at least one record for the day
        valid_stores = []
        _dates = tupleize_date(_now.date(), 3)
        for store in stores:
            # Get one store
            try:
                rows = g._db.query("""
                    SELECT date FROM price_by_store
                    WHERE store_uuid = {}
                    AND date IN {}
                    LIMIT 1
                """.format(
                    store['uuid'], 
                    _dates  
                ))
                if rows:
                    valid_stores.append(store)
            except Exception as e:
                logger.error(e)
        return valid_stores