from flask import g
import datetime
import app
from app.utils import applogger, errors
from app.models.task import Task
import config
import requests
import json
import pandas as pd
import calendar

# Logger
logger = applogger.get_logger()

# Task global variable
task = None

grouping_cols = {
    'day': ['year', 'month', 'day'],
    'month': ['year', 'month'],
    'week': ['year', 'week']
}

def validate_params(params):
    """ Params validation method
        @Params:
            - params (dict): params to validate

        @Returns:
            - params: (dict): Validated params
    """
    if not params:
        raise errors.AppError(40002, "Params Missing!", 400)
    if 'filters' not in params:
        raise errors.AppError(40003, "Filters param Missing!", 400)
    if 'retailers' not in params:
        raise errors.AppError(40003, "Retailers param Missing!", 400)
    if 'date_start' not in params:
        raise errors.AppError(40003, "Start Date param Missing!", 400)
    if 'date_end' not in params:
        raise errors.AppError(40003, "End Date param Missing!", 400)
    if 'interval' not in params:
        # In case interval is not explicit, set to day
        params['interval'] = 'day' 

    return params



def add_dates(df):
    """ Method to add dates to DF

        Params:
        ------
        - df : (pandas.DataFrame) Data Set

        Returns:
        -----
        (pandas.DataFrame)  Data Set with Date Columns
    """        
    df['day'] = df['time'].apply(lambda x: x.day)
    df['month'] = df['time'].apply(lambda x: x.month)
    df['year'] = df['time'].apply(lambda x: x.year)
    df['week'] = df['time'].apply(lambda x: x.isocalendar()[1])
    return df


def tuplize(_list, is_uuid=False):
    """ Static method to convert into a tuple-like string

        Params: 
        -----
        - _list: (list) Elements to convert
        - is_uuid: (bool) UUID flag to remove or not single-quotes

        Returns:
        -----
        (str) Tuple-like Converted String
    """
    lstr = str(tuple(_list))
    if len(_list) == 1:
        lstr = lstr.replace(',','')
    if is_uuid:
        lstr = lstr.replace("'","")
    return lstr


def fetch_prices(rets, stores, items, _t, _t1=None):
    """ Method to query detail prices from C*

        Params:
        -----
        - rets : (list) List of Retailer keys
        - stores : (list) List Stores UUIDs
        - items : (list) List of Item UUIDs
        - _t : (str) Time formatted string (lower bound)
        - _t1 : (str) Time formatted string (upper bound)

        Returns:
        -----
        - (list) : Query response with Store_uuid, retailer and price
    """
    stores_str = tuplize(stores, True)
    rets_str = tuplize(rets)
    items_str = tuplize(items, True)
    cass_query = """
                SELECT item_uuid, retailer,
                store_uuid, price, lat,
                lng, time, date
                FROM price_details 
                WHERE item_uuid IN {}
                AND retailer IN {}
                AND store_uuid IN {}
                AND time > '{}'
                """.format(items_str,
                    rets_str,
                    stores_str,
                    _t)
    if _t1:
        cass_query += " AND time < '{}'".format(_t1)
    logger.debug(cass_query)
    # Query item price details
    try:
        c_resp = g._db.query(cass_query, timeout=200)
        logger.info('C* Prices fetched!')
        logger.debug(len(c_resp))
        if not c_resp:
            raise Exception('No prices!!')
        return c_resp
    except Exception as e:
        logger.error(e)
        return []


def fetch_from_item(filters):
    """ Static Method to retrieve info from asked items.
        
        Params:
        -----
        - filters: (list) Item Filters

        Returns: 
        -----
        (list) List of Items with Name and GTIN
    """
    # Fetch uuids from filters in ITEM
    payload  = json.dumps(filters)
    url = 'http://'+config.SRV_ITEM+'/item/filtered'
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


def fetch_stores(rets):
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
                .get("http://"+config.SRV_GEOLOCATION+"/store/retailer?key="+retailer)\
                .json()
            logger.debug("Fetched {} stores!".format(retailer))
        except Exception as e:
            logger.error(e)
            return None
        stores += stores_j
    logger.info("Found {} stores".format(len(stores)))
    return stores


def grouped_by_store(task_id, filters, rets, _ini, _fin, _inter):
    """ Static method to retrieve passed prices by all 
        available stores given UUIDS, retailer keys
        and date range . 
        Result is temporarly stored in dumps/<task_id>

        Params:
        -----
        - task_id: (str) Celery Task ID
        - filters: (list) Item Filters
        - rets: (list) Needed Retailers
        - _ini: (str) ISO format Start Date
        - _fin: (str) ISO format End Date 
        - _inter: (str) Time interval            
    """
    global task
    task.task_id = task_id

    # Start status file in 0
    task.progress = 0 

    # Given the filters (list of item_uuids), get the prices
    # for the given dates and time interval (day, week, month)
    # for






    # Validate Params
    filters += [{'retailer': _ret} for _ret in rets.keys()]


    # Fetch Items by filters
    items = fetch_from_item(filters)
    # Save status
    task.progress = 10
    # Fetch Stores by retailers
    stores = fetch_stores([r for r in rets.keys()])

    # Create Stores DF 
    stores_df = pd.DataFrame(stores)
    stores_df.rename(columns={'name': 'store'}, inplace=True)
    stores_df['store_uuid'] = stores_df.uuid.apply(lambda x: str(x))

    # And filter stores by filters
    if ('store' in [list(_fil.keys())[0] for _fil in filters]):
        logger.info('Filtering stores!')
        _stuids = [_su['store'] for _su in filters \
                    if 'store' in _su]
        logger.debug(_stuids)
        stores_df = stores_df[stores_df.store_uuid\
                            .apply(lambda x: True \
                                    if x in _stuids \
                                    else False) 
                            == True]
    logger.info('Filtering over {} stores'.format(len(stores_df)))
    task.progress = 20
    # Start fetching prices
    map_prices = fetch_prices(
        [r for r in rets.keys()],
        stores_df.uuid.tolist(),
        [i['item_uuid'] for i in items],
        _ini,
        _fin)

    if not map_prices:
        # Save status
        taks.error("Something happened")
        raise Exception('Could not fetch Prices!')
    # Update loading status
    task.progress = 60

    # Load DF and filter results
    mapa_df = pd.DataFrame(map_prices)
    mapa_df.item_uuid = mapa_df.item_uuid.apply(lambda x: str(x))
    mapa_df.store_uuid = mapa_df.store_uuid.apply(lambda x: str(x))
    # Remove Duplicates by day, take more recent
    mapa_df.sort_values(['time'], ascending=False, inplace=True)
    mapa_df.drop_duplicates(['item_uuid', 'store_uuid', 'date'],
                            keep='first',
                            inplace=True)
    logger.info('Droping duplicates and starting to format..')
    # Merge with item and stores DF
    items_df = pd.DataFrame(items)
    items_df.item_uuid = items_df.item_uuid.apply(lambda x: str(x))
    mapa_df = pd.merge(mapa_df,
                    items_df,
                    on='item_uuid',
                    how='left')
    mapa_df = pd.merge(mapa_df,
                    stores_df[['store_uuid','store']],
                    on='store_uuid',
                    how='left')
    mapa_df.retailer = mapa_df.retailer.apply(lambda x: rets[x])
    # Add Grouping dates
    mapa_df = add_dates(mapa_df)
    ### Verify if **grouping_periods** is needed
    logger.info('Finished format, writing result..')
    task.progress = 80

    table_result, map_result = {}, {}
    # Format Table Result
    # Group by retailer
    for temp_ret,ret_df in mapa_df.groupby(['retailer']):
        table_result[temp_ret] = []
        # Group by intervals, item and store
        for temp_int, i_df in ret_df.groupby(Mapa\
                        .grouping_cols[_inter] \
                        + ['item_uuid', 'store_uuid']):
            table_result[temp_ret].append({
                'gtin': i_df.gtin.tolist()[0],
                'name': i_df.name.tolist()[0],
                'store': i_df.store.tolist()[0],
                'price': round(i_df.price.mean(), 2),
                'date': i_df.time.tolist()[0].date().isoformat()
            })

    task.progress = 90
    logger.debug('Formatted Table result')
    # Format Map Result
    # Group by intervals
    for temp_int, i_df in mapa_df.groupby(Mapa\
                            .grouping_cols[_inter]):
        # first Date in teh interval
        _tdate = i_df\
                .sort_values(['date'], ascending=True)\
                .time.tolist()[0]\
                .date().isoformat()
        map_result[_tdate] = []
        # Group by item and store
        for temp_stit, si_df in i_df.groupby(['store_uuid']):
            map_result[_tdate].append({
                'store': si_df.store.tolist()[0],
                'retailer': si_df.retailer.tolist()[0],
                'lat': round(si_df.lat.tolist()[0], 6),
                'lng': round(si_df.lng.tolist()[0], 6),
                'price': round(si_df.price.mean(), 2),
                'max': round(si_df.price.max(), 2),
                'min': round(si_df.price.min(), 2)
            })
    # Final result
    result = {'mapa': map_result, 'tabla': table_result}
    task.progress = 100
    task.result = {
        'data' : result,
        'msg' : 'Task completed!'
    }
    logger.info('Finished computing {}!'.format(task_id))
    

