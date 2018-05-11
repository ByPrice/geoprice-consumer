import datetime
import pandas as pd
import numpy as np
import requests
from config import *
from app import logger
import calendar

geo_stores_url = SRV_GEOLOCATION+'/store/retailer?key=%s'
geo_rets_url = SRV_GEOLOCATION+'/retailer/all'

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


def fetch_store(rkey):
    """ Fetch a store from Geolocation Service

        Params:
        -----
        rkey : str
            Source key

        Returns
        -----
        xr : list
            List of stores of certain source/retailer
    """
    try:
        logger.debug('Querying %s' %(geo_stores_url % rkey))
        xr = requests.get(geo_stores_url % rkey).json()
        for i, x in enumerate(xr):
            xr[i].update({'source': rkey})
        return xr
    except Exception as e:
        logger.error(e)
        logger.warning('Issues retrieving %s stores' % str(rkey))
    return None


def get_all_stores(rets=[]):
    """ Fetch all stores from Geolocation service.

        Params:
        -----
        rets : list
            List of retailer dicts

        Returns:
        -----
        stores_df : pd.DataFrame
            Stores DF
    """
    # Verify rets
    if not rets:
        rets = requests.get(geo_rets_url).json()
    stores = []
    for r in rets:
        tmp = fetch_store(r['key'])
        if tmp:
            stores += tmp
    stores_df = pd.DataFrame(stores)
    stores_df['store_uuid'] = stores_df['uuid'].astype(str)
    del stores_df['uuid']
    return stores_df

def obtain_distances(fixed, added, rets):
    """ Obtain distances from all stores

        Params:
        -----
        fixed : str
            Store UUID from Fixed 
        added : list
            Store UUIDs from Added
        - rets : List
            Retailers 

        Returns:
        -----
        _added_d : dict
            Hash table mapping fixed store to added store
    """
    # Fetch all stores
    _st_list = []
    _geo_df = get_all_stores([{'key': r} for r in rets])
    # Get fixed store values
    _f =  _geo_df[_geo_df['store_uuid'] == fixed]
    f_lat, f_lng = _f['lat'].values[0], _f['lng'].values[0]
    # Compute distances from fixed against added
    _geo_df = _geo_df[_geo_df.store_uuid.isin(added)]
    _geo_df['fdist'] = np\
        .arccos(np.sin(np.deg2rad(_geo_df.lat))
            * np.sin(np.deg2rad(f_lat))
            + np.cos(np.deg2rad(_geo_df.lat))
            * np.cos(np.deg2rad(f_lat))
            * np.cos(np.deg2rad(f_lng)
                        - (np.deg2rad(_geo_df.lng))
                        )
            ) * 6371
    _geo_df.fdist = _geo_df.fdist.apply(lambda x: round(x,2))
    # Reformat values
    _added_d = _geo_df[['store_uuid', 'fdist']]\
                .set_index('store_uuid')\
                .to_dict()['fdist']
    logger.info('Got distances')
    return _added_d

def date_js():
    """ Convert datetime object into 
        JS timestamp.

        Returns
        -----
        f : lambda
            Converter function to JS timestamp
    """
    return lambda djs: int((djs - 
                        datetime.datetime(1970, 1, 1,0,0))\
                        /datetime.timedelta(seconds=1)*1000)

def grouping_periods(params):
    """ Method the receives a dict params with the following keys:
         
        Params:
        -----
        params : dict
            Dates info and interval
        >>> { 
            "date_ini": "(str) ISO format Date",
            "date_fin": "(str) ISO format Date",
            "interval": "(str) day | week | month"
        }

        Returns:
        -----
        groups : list
            Group of valid date ranges
    """
    groups = []
    di = datetime.datetime(*tuple(int(d) for d in params['date_ini'].split('-')))
    df = datetime.datetime(*tuple(int(d) for d in params['date_fin'].split('-')))
    # Day intervals
    if params['interval'] == 'day':
        groups.append([di])
        while True:
            di += datetime.timedelta(days=1)
            groups.append([di])
            if di >= df:
                break
    # Week intervals
    elif params['interval'] == 'week':
        groups.append([di,di+datetime.timedelta(days=7-di.isoweekday())])
        di += datetime.timedelta(days=7-di.weekday())
        while True:
            if di >= df:
                break
            dv = di + datetime.timedelta(days=6)
            groups.append([di,dv])
            di = dv + datetime.timedelta(days=1)
    # Monthly intervals
    else:
        lmd = calendar.monthrange(di.year,di.month)[1]
        groups.append([di,di+datetime.timedelta(days=lmd-di.day)])
        di += datetime.timedelta(days=lmd-di.day)
        while True:
            if di >= df:
                break
            _di_month = di.month + 1 if di.month != 12 else 1
            _di_year = di.year + 1 if di.month == 12 else di.year
            lmd = calendar.monthrange(_di_year,_di_month)[1]
            dv = di + datetime.timedelta(days=lmd)
            groups.append([di+ datetime.timedelta(days=1),dv])
            di = dv
    return groups