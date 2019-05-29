import datetime
import app
from ByHelpers import applogger
from flask import g, request
import config
import requests
import json
import pandas as pd
from app.models.geo_mapa import Map, grouping_cols
from app.models.task import Task
from app.models.price import Price

# Logger
logger = applogger.get_logger()


class Historia(Map):
    """ Class to perform Historic needed routines for Geo located prices,
        inheriting methods from Mapa
    """

    def __init__(self):
        """ Constructor
        """
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        """ Destructor
        """
        pass

    @staticmethod
    def filters_task(task_id, params):
        """ Start Historia info task, first it validates parameters
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
                Historia Response
        """
        # Validate params
        Historia.validate_params(params)
        # Parse and start task
        response = Historia.grouped_by_retailer(
            task_id,
            params['filters'],
            params['retailers'],
            params['date_start'],
            params['date_end'],
            params['interval']
        )
        return response

    @staticmethod
    def fetch_prices(items, _t, _t1=None):
        """ Method to query detail prices from C*

            @Params:
            - items : (list) List of Item UUIDs
            - _t : (str) Time formatted string (lower bound)
            - _t1 : (str) Time formatted string (upper bound)
            @Returns:
            - (list) : Query response with Store_uuid, retailer and price
        """
        items_str = Historia.tuplize(items, True)
        cass_query = """
                    SELECT item_uuid, retailer,
                    store_uuid, price, time
                    FROM price_item
                    WHERE item_uuid IN {}
                    AND time > '{}'
                    """.format(items_str,
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
    
    @staticmethod
    def decompose_filters(task, filters, rets, date_start, date_end):
        """ Given filters with time intervals, stores, retailers and items,
            return decomposed elements for querying ing C*

            Params:
            -----
            - task: (app.models.Task)  Task instance
            - filters: (list) Item Filters
            - rets: (list) Needed Retailers
            - date_start: (str) Date Lower Bound
            - date_from: (str) Date Upper Bound

            Returns:
            -----
            (tuple): Tuple containing the following
                (dates, retailers, retailers_by_key, stores, 
                stores_by_uuid, items, items_by_uuid,
                prods, prods_by_uuid)
        """
        # Filters
        f_retailers = [ f['retailer'] for f in filters if 'retailer' in f ]
        f_stores = [ f['store_uuid'] for f in filters if 'store_uuid' in f ]
        f_items = [ f['item_uuid'] for f in filters if 'item_uuid' in f ]
        logger.debug("Filters for the task: {}".format(filters))
        # Dates
        dates = []
        date_start = moment = datetime.datetime.strptime(date_start, "%Y-%m-%d") 
        date_end = datetime.datetime.strptime(date_end, "%Y-%m-%d") 
        while moment <= date_end:
            dates.append(int(moment.strftime("%Y%m%d")))
            moment = moment + datetime.timedelta(days=1)
        logger.debug("Dates list: {}".format(dates))

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
        task.progress = 35
        # Return elements
        return dates, retailers, retailers_by_key, \
            stores, stores_by_uuid, items, \
            items_by_uuid, prods, prods_by_uuid
    
    @staticmethod
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

    @staticmethod
    def grouped_by_retailer(task_id, filters, rets, _from, until, interval):
        """ Static method to retrieve passed prices by all 
            available stores given UUIDS and retailer keys
            from previous 24 hrs. 
            Result is temporarly stored in Redis

            Params:
            -----
            - task_id: (str) Celery Task ID
            - filters: (list) Item Filters
            - rets: (list) Needed Retailers
            - _from: (str) Date Lower Bound
            - until: (str) Date Upper Bound
            - interval: (str) Interval Type (day, month, week)
        """
        # Task initialization
        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1
        
        # Decompose filters
        dates, retailers, \
            retailers_by_key, \
            stores, stores_by_uuid, \
            items, items_by_uuid, \
            prods, prods_by_uuid = Historia.decompose_filters(
                                task,
                                filters, 
                                rets, 
                                _from,
                                until
                            )
        logger.info("Got products from filters")
        # Get prices of all the products
        prices = Price.query_by_product_store(
            stores=list(stores_by_uuid.keys()),
            products=list(prods_by_uuid.keys()),
            dates=dates
        )
        task.progress = 60
        # If no prices, end task...
        if not prices:
            logger.warning("No prices found!")
            raise Exception("No se encontraron precios, intenta nuevamente")
        # Format prices
        hist_df = pd.DataFrame(prices)
        hist_df['product_uuid'] = hist_df.product_uuid.apply(lambda x: str(x))
        hist_df['store_uuid'] = hist_df.store_uuid.apply(lambda x: str(x))
        hist_df['item_uuid'] = hist_df.product_uuid.apply(lambda z: prods_by_uuid[z]['item_uuid'])
        # Add JS-like timestamp
        hist_df.sort_values(['time'], ascending=False, inplace=True)
        hist_df['time_js'] = hist_df.time\
                                    .apply(lambda djs: \
                                            (djs - \
                                            datetime.datetime(1970, 1, 1,0,0))\
                                            /datetime.timedelta(seconds=1)\
                                            * 1000)
        hist_df['date'] = hist_df.time.apply(lambda d: d.date().isoformat())
        # Remove Duplicates, take more recent
        hist_df.drop_duplicates(['item_uuid', 'store_uuid', 'date'],
                                keep='first',
                                inplace=True)
        task.progress = 70
        # Add name and gtin
        hist_df['gtin'] = hist_df.item_uuid.apply(lambda z: items_by_uuid[z]['gtin'])
        hist_df['name'] = hist_df.product_uuid.apply(lambda z: prods_by_uuid[z]['name'])
        hist_df['store'] = hist_df.store_uuid.apply(lambda z: stores_by_uuid[z]['name'])
        hist_df['retailer'] = hist_df.retailer.apply(lambda z: retailers_by_key[z])
        # Add Grouping dates
        hist_df = Historia.add_dates(hist_df)
        task.progress = 80
        # Add Retailer Format
        ret_res = []
        for _gk, _gdf in hist_df.groupby(['retailer']):
            _tmp = {
                "name": _gk, "data": []
            }
            # Once grouped by retailer aggregate by interval
            for _dk, _ddf in _gdf.groupby(grouping_cols[interval]):
                _tmp['data'].append([
                        int(_ddf.time_js.min()),
                        round(_ddf.price.mean(), 2)
                    ])
            # Append to result
            ret_res.append(_tmp)
        task.progress = 90
        logger.info('Created Retailer result')
        # Add Metrics Format
        met_res = {'avg': [], 'min': [], 'max': []}
        for mj, mrow in hist_df.groupby(grouping_cols[interval]):
            # Take JS Timestamp
            _tmpjs = int(mrow.time_js.min())
            met_res['avg'].append([_tmpjs, round(mrow.price.mean(), 2)])
            met_res['min'].append([_tmpjs, round(mrow.price.min(), 2)])
            met_res['max'].append([_tmpjs, round(mrow.price.max(), 2)])
        logger.info('Created Metrics result')
        # Acumulated result
        result = {
            "metrics": met_res,
            "retailers": ret_res,
            "title": "Tendencia de Precios Regionalizados",
            "subtitle": ("<b> Periodo: </b> {} - {} <br>"\
                    + "<b> Retailers: </b> {}").format(
                        hist_df.time.min().date().isoformat(),
                        hist_df.time.max().date().isoformat(),
                        ', '.join([z['name'] for z in ret_res])
                    )
        }
        task.progress = 100
        return {
            'data': result,
            'msg': 'Task test OK'
        }
        


