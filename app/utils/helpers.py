import datetime
import pandas as pd
import requests
from config import *
from app import logger

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'
geo_rets_url = 'http://'+SRV_GEOLOCATION+'/retailer/all'

def tupleize_date(date, periods):
    """ Generate date tuples from a certain date 
        and a number of prior periods (days)

        Params:
        -----
        date : datetime.date
            Date to tupleize from
        periods : int
            Number of prior periods(days)
        
        Returns:
        -----
        tupdate : tuple
            Tupled dates in Cassandra needed format
    """
    tupdate = []
    for i in range(periods):
        tmp = date - datetime.timedelta(days=i)
        tupdate.append(
            int(tmp.__str__().replace('-',''))
        )
    return tuple(tupdate)


def get_all_stores():
    """ Fetch all stores from Geolocation service.

        Returns:
        -----
        stores_df : pd.DataFrame
            Stores DF
    """
    rets = requests.get(geo_rets_url).json()
    stores = []
    for r in rets:
        try:
            xr = requests.get(geo_stores_url % r['key']).json()
            for i, x in enumerate(xr):
                xr[i].update({'source': r['key']})
            stores += xr
        except Exception as e:
            logger.error(e)
            logger.warning('Issues retrieving %s stores' % str(key))
            continue
    stores_df = pd.DataFrame(stores)
    stores_df['store_uuid'] = stores_df['uuid'].astype(str)
    del stores_df['uuid']
    return stores_df