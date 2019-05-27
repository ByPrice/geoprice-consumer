import datetime
import app
from flask import g
import config
import requests
import json
import pandas as pd
from app.models.task import Task
from app.models.price import Price
from app import errors
from ByHelpers import applogger
import calendar
import math

# Logger
logger = applogger.get_logger()

grouping_cols = {
    'day': ['year', 'month', 'day'],
    'month': ['year', 'month'],
    'week': ['year', 'week']
}

class Map(object):
    """ Class to perform Map needed routines for Geo located prices.
    """
    

    @staticmethod        
    def validate_params(params):
        """ Params validation method
            
            Params:
            -----
            params : dict
                params to validate

            Returns:
            -----
            dict
                Validated params
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

    
    @staticmethod
    def start_task(task_id, params):
        """ Start Map info task, first it validates parameters
            and then it builds a response upon filters

            Params:
            -----
            task_id:  str
                Task ID 
            params: dict
                Request Params
            
            Returns:
            -----
            flask.Response
                Map Response
        """
        print(task_id, params)
        # Validate params
        Map.validate_params(params)
        # Parse and start task
        response = Map.build_response(
            task_id,
            params['filters'],
            params['retailers'],
            params['date_start'],
            params['date_end'],
            params['interval']
        )
        return response

    @staticmethod
    def build_response(task_id, filters, rets, date_start, date_end, interval):
        """ Static method to retrieve passed prices by all 
            available stores given UUIDS, retailer keys
            and date range . 
            Result is temporarly stored in dumps/<task_id>

            Params:
            -----
                - task_id: (str) Celery Task ID to keep track of the progress
                - filters: (list) Item Filters 
                    [item_uuids, store_uuids, retailers]
                - rets: (list) List of posible retailers to query depending
                        on the client configurations
                - date_start: (str) ISO format Start Date
                - date_end: (str) ISO format End Date 
                - interval: (str) Time interval  

            Returns:
            -----
                - map: {
                    "date" : [ {
                        "lat" : "",
                        "lng" : "",
                        "value" :
                    } ]
                }
                - table: all table data
                - history: history chart data
                    
        """
        ## Delete this later
        print(task_id)
        print(filters)
        print(rets)
        print(date_start)
        print(date_end)
        print(interval)
        ##

        # Task initialization
        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

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
            dates.append(int(moment.strftime("%Y%m%d")))
            moment = moment + datetime.timedelta(days=1)
        logger.info("Dates list: {}".format(dates))

        # Get retailers and its details
        retailers = rets if type(rets) == dict else { r['key'] : r for r in rets }
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

        # Get details of all items requested
        items = g._catalogue.get_items_details(
            values=f_items, 
            cols=['item_uuid','gtin','name']
        )
        items_by_uuid = { i['item_uuid'] : i for i in items }
        task.progress = 20

        # Get products per item and then prices to build the main DF
        table = []
        prices = []
        prods = []
        for i,it in enumerate(items):
            # Get product of the item
            prods += g._catalogue.get_intersection(
                item_uuid=[it['item_uuid']], 
                source=list(retailers_by_key.keys()),
                cols=['item_uuid','product_uuid','source','gtin','name'],
            )
        # All prods by uuid
        prods_by_uuid = { p['product_uuid'] : p for p in prods }
        logger.info("Got products from filters")
        # Get prices of all the products
        prices = Price.query_by_product_store(
            stores=list(stores_by_uuid.keys()),
            products=list(prods_by_uuid.keys()),
            dates=dates
        )
        task.progress = 50
        # If no prices, end task...
        if not prices:
            logger.warning("No prices found!")
            raise Exception("No se encontraron precios, intenta nuevamente")
        logger.info("Got prices from DB !")

        # Append data to create dataframe
        df = pd.DataFrame(prices)
        df['product_uuid'] = df['product_uuid'].astype(str)
        df['store_uuid'] = df['store_uuid'].astype(str)
        df['item_uuid'] = df.product_uuid.apply(lambda z: prods_by_uuid[z]['item_uuid'])
        df['gtin'] = df.item_uuid.apply(lambda z: items_by_uuid[z]['gtin'])
        df['name'] = df.product_uuid.apply(lambda z: prods_by_uuid[z]['name'])
        df['store'] = df.store_uuid.apply(lambda z: stores_by_uuid[z]['name'])
        df['retailer_name'] = df.retailer.apply(lambda z: retailers_by_key[z])
        df['promo'] = df.promo.fillna('')
        df['currency'] = 'MXN'
        df['day'] = df.time.apply(lambda z: z.day)
        df['month'] = df.time.apply(lambda z: z.month)
        df['year'] = df.time.apply(lambda z: z.year)
        df['week'] = df.time.apply(lambda z: z.isocalendar()[1])
        df['date'] = df.time.apply(lambda z: z.date().isoformat())
        task.progress = 60
        logger.info("Got the info to build the DF ")

        # Create the dataframe
        df.sort_values(by=['date'], ascending=True, inplace=True)
        df.drop_duplicates(
            ['item_uuid', 'store_uuid', 'date'],
            keep='first',
            inplace=True
        )

        # Group by intervals and product_uuid
        interval = 'day' if not interval else interval
        columns_map = grouping_cols[interval]+['store_uuid']
        columns_table = grouping_cols[interval]+['product_uuid']
        columns_history = grouping_cols[interval]+['retailer']
        columns_interval = grouping_cols[interval]

        # Get stats for interval and store
        result = {}
        result['map'] = {}
        grouped_by_interval = df.groupby(grouping_cols[interval])
        for key, df_interval in grouped_by_interval:
            # First date in the interval
            interval_date = df_interval.sort_values(
                ['time'], ascending=True
            ).date.tolist()[0]
            result['map'][interval_date] = []
            for store, df_store in df_interval.groupby(['store']):
                
                result['map'][interval_date].append({
                    "gtin" : df_store['gtin'].tolist()[0],
                    "lat" : df_store['lat'].tolist()[0],
                    "lng" : df_store['lng'].tolist()[0],
                    "retailer" : df_store['retailer'].tolist()[0],
                    "store" : df_store['store'].tolist()[0],
                    "date" : df_store['date'].tolist()[0],
                    "avg" : round(df_store.price.mean(),2),
                    "max" : round(df_store.price.max(),2),
                    "min" : round(df_store.price.min(),2),
                    "original" : round(df_store.price_original.mean(),2)
                })
        
        task.progress = 90
        logger.info("Built map result: ")

        # Groped stats for the table
        result['table'] = {}
        df.sort_values(by=['retailer'], ascending=True, inplace=True)
        # Order by retailer
        grouped_by_retailer = df.groupby(['retailer'])
        for retailer, df_retailer in grouped_by_retailer:
            # New list value
            result['table'][retailer] = []
            # Interval
            for interval, df_interval in df_retailer.groupby(grouping_cols[interval] + ['item_uuid','store_uuid']): 
                result['table'][retailer].append({
                    "gtin" : df_interval['gtin'].tolist()[0],
                    "name" : df_interval['name'].tolist()[0],
                    "promo" : df_interval['promo'].tolist()[0],
                    "store" : df_interval['store'].tolist()[0],
                    "price" : df_interval['price'].tolist()[0],
                    "date" : df_interval['date'].tolist()[0],
                    "price" : round(df_interval.price.mean(),2),
                    "price_original" : round(df_interval.price_original.mean(),2),
                    "max" : round(df_interval.price.max(),2),
                    "min" : round(df_interval.price.min(),2)
                })

        # Save task result
        print("Before saving result... ")
        task.progress = 100
        resp = {
            'data' : result,
            'msg' : 'Task completed'
        }
        logger.info('Finished computing {}!'.format(task_id))
        return resp
        
