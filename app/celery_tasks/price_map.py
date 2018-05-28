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


def grouped_by_store(task_id, filters, rets, date_start, date_end, interval):
    """ Static method to retrieve passed prices by all 
        available stores given UUIDS, retailer keys
        and date range . 
        Result is temporarly stored in dumps/<task_id>

        Params:
        -----
            - task_id: (str) Celery Task ID
            - filters: (list) Item Filters 
                [item_uuids, store_uuids, retailers]
            - rets: (list) Needed Retailers
            - date_start: (str) ISO format Start Date
            - date_end: (str) ISO format End Date 
            - interval: (str) Time interval  

        @Returns:
            - map: {
                "date" : [ {
                    "lat" : "",
                    "lng" : "",
                    "value" :
                } ]
            }
            - table:
                  
    """
    global task
    task.task_id = task_id

    # Filters
    f_retailers = [ f['retailer'] for f in filters if 'retailer' in f ]
    f_stores = [ f['store_uuid'] for f in filters if 'store_uuid' in f ]
    f_items = [ f['item_uuid'] for f in filters if 'item_uuid' in f ]

    logger.info("Filters for the task: {}".format(filters))

    # Dates
    dates = []
    date_start = moment = datetime.datetime.strptime(date_start, "%Y-%m-%d") 
    date_end = datetime.datetime.strptime(date_end, "%Y-%m-%d") 
    while moment <= date_end:
        dates.append(moment.strftime("%Y-%m-%d"))
        moment = moment + datetime.timedelta(days=1)

    logger.info("Dates list: {}".format(dates))

    # Get retailers and its details
    retailers = { r['key'] : r for r in g._geolocation.get_retailers() }
    if f_retailers:
        retailers_by_key = { k : v for k,v in retailers.items() if k in f_retailers }
    else:
        retailers_by_key = retailers 

    # Get all stores
    stores = g._geolocation.get_stores(retailers_by_key.keys())
    # Stores by their uuid
    if f_stores:
        stores_by_uuid = { s['uuid'] : s for s in stores if s['uuid'] in f_stores }
    else:
        stores_by_uuid = { s['uuid'] : s for s in stores }

    # Get item details
    items = g._catalogue.get_items(
        values=f_items, 
        by='item_uuid', 
        cols=['item_uuid','gtin','name']
    )
    items_by_uuid = { i['item_uuid'] : i for i in items }
    
    # Get product_uuids per item
    table = []
    prices = []
    for i,it in enumerate(items):

        # Get product of the item
        prods = g._catalogue.get_intersection(
            item_uuid=[it['item_uuid']], 
            retailer=list(retailers_by_key.keys()),
            cols=['product_uuid','retailer']
        )
        prods_by_uuid = { p['product_uuid'] : p for p in prods }
        # Get prices of the products
        prices = Price.query_by_product_store(
            'table',
            stores=stores,
            products=list(prods.keys()),
            dates=dates
        )

        # Append data to create dataframe
        row = {}
        for pr in prices:
            row = {
                "item_uuid" : it['item_uuid'],
                "gtin" : it['gtin'],
                "name" : it['name'],
                "store_uuid" : pr['store_uuid'],
                "store" : stores_by_uuid[pr['store_uuid']]['name'],
                "retailer" : prods_by_uuid[pr['product_uuid']]['retailer'],
                "retailer_name" : retailers_by_key[\
                    prods_by_uuid[\
                        pr['product_uuid']\
                    ]['retailer']\
                ]['name'],
                "price" : pr['price'],
                "price_original" : pr['price_original'],
                "promo" : pr['promo'],
                "currency" : pr['currency'],
                "time" : pr['time'],
                "lat" : pr['lat'],
                "lng" : pr['lng'],
                "product_uuid" : pr['product_uuid'],
                "day" : pr['time'].day,
                "month" : pr['time'].month,
                "year" : pr['time'].year,
                "week" : pr['time'].isocalendar()[1],
                "date" :  pr['time'].date().isoformat()
            }
            table.append(row)
            
    logger.info("Got the info to build the df: ")
    print(table)

    # Create the dataframe
    df = pd.DataFrame(table)
    df.sort_values(by=['date'], ascending=True, inplace=True)

    # Group by intervals and product_uuid
    interval = 'day' if not interval else interval
    columns_map = grouping_cols[interval]+['store_uuid']
    columns_table = grouping_cols[interval]+['product_uuid']

    # Get stats for interval and store
    result['map'] = {}
    grouped_by_interval = df.groupby(columns_map)
    for key, prod_interval in grouped_by_interval:
        # Sorting by dates
        prod_interval.sort_values(by=['date'], ascending=True, inplace=True)
        tmp_prod_interval = dict(grouped.iloc[0])
        interval_date = prod_interval.time.tolist()[0].date().isoformat()
        # Build the response
        result['map'][interval_date].append({
            "gtin" : tmp_prod_interval['gtin'],
            "lat" : tmp_prod_interval['lat'],
            "lng" : tmp_prod_interval['lng'],
            "retailer" : tmp_prod_interval['retailer'],
            "store" : tmp_prod_interval['store'],
            "date" : tmp_prod_interval['date'],
            "avg" : round(prod_interval.price.avg(),2)
            "max" : round(prod_interval.price.max(),2),
            "min" : round(prod_interval.price.min(),2),
            "original" : round(prod_interval.price_original.mean(),2)
        })
        # Result
        result['map'][interval_date] = tmp_prod_interval

    # Groped stats for the table
    result['table'] = {}
    df.sort_values(by=['retailer'], ascending=True, inplace=True)
    # Order by retailer
    grouped_by_retailer = df.groupby(columns_table)
    for key, prod_retailer in grouped_by_retailer:
        prod_retailer.sort_values(by=['retailer','date'], ascending=True, inplace=True)
        tmp_prod_retailer = dict(grouped.iloc[0])
        result['table'][tmp_prod_retailer['retailer_name']] = {
            "date" : tmp_prod_retailer['date'],
            "gtin" : tmp_prod_retailer['gtin'],
            "name" : tmp_prod_retailer['name'],
            "store" : tmp_prod_retailer['store'],
            "price" : round(prod_retailer.price.min(),2),
            "price_original" : round(prod_retailer.price_original.min(),2)
        }

    # Save task result
    task.progress = 100
    task.result = {
        'data' : result,
        'msg' : 'Task completed in {} seconds'
    }
    logger.info('Finished computing {}!'.format(task_id))
    

