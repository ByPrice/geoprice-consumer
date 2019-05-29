import datetime
import app
from ByHelpers import applogger
from flask import g, request
import config
import requests
import json
import pandas as pd
from app.models.geo_mapa import Map
from app.models.task import Task

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
    def grouped_by_retailer(task_id, filters, rets, _from, until, interval):
        """ Static method to retrieve passed prices by all 
            available stores given UUIDS and retailer keys
            from previous 24 hrs. 
            Result is temporarly stored in dumps/<task_id>

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
        
        # Validate Params
        filters += [{'retailer': _ret} for _ret in rets.keys()]
        print(filters)

        task.progress = 100
        return {
            'data': [],
            'msg': 'Task test OK'
        }
        ##
        ##

        # Fetch Items by filters
        items = Mapa.fetch_from_item(filters)
        Historia.write_file('states', task_id, '10')
        # Fetch stores by retailers
        stores = Mapa.fetch_stores([r for r in rets.keys()])
        # Create Stores DF
        stores_df = pd.DataFrame(stores)
        stores_df.rename(columns={'name': 'store'}, inplace=True)
        stores_df['store_uuid'] = stores_df.uuid.apply(lambda x: str(x))
        Historia.write_file('states', task_id, '20')
        # Start fetching prices
        hist_prices = Historia.fetch_prices([i['item_uuid'] for i in items],
                                        _from,
                                        until)
        if not hist_prices:
            # Put an empty result
            Historia.write_file('states', task_id, '100')
            Historia.write_file('dumps', task_id, '')
            raise Exception('Could not fetch Prices!')
        # Update loading status
        Historia.write_file('states', task_id, '50')
        # Load DF
        hist_df = pd.DataFrame(hist_prices)
        hist_df.item_uuid = hist_df.item_uuid.apply(lambda x: str(x))
        hist_df.store_uuid = hist_df.store_uuid.apply(lambda x: str(x))
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
        logger.info('Droping duplicates and starting to format..')
        # Update loading status
        Historia.write_file('states', task_id, '60')
        # Filter only Valid Retailers
        hist_df = hist_df[hist_df['retailer']\
                .apply(lambda x: True if x in rets \
                                      else False) == True]
        # Filter only Valid Stores
        if ('store' in [list(_fil.keys())[0] for _fil in filters]):
            _stuids = [_su['store'] for _su in filters \
                        if 'store' in _su]
            hist_df = hist_df[hist_df.store_uuid\
                        .apply(lambda x: True \
                                if x in _stuids \
                                else False) 
                        == True]
        # Complete Item names and Store Names
        items_df = pd.DataFrame(items)
        items_df.item_uuid = items_df.item_uuid.apply(lambda x: str(x))
        hist_df = pd.merge(hist_df,
                        items_df,
                        on='item_uuid',
                        how='left')
        hist_df = pd.merge(hist_df,
                        stores_df[['store_uuid','store']],
                        on='store_uuid',
                        how='left')
        # Add Correct Retailer names
        hist_df.retailer = hist_df.retailer.apply(lambda x: rets[x])
        # Add Grouping dates
        hist_df = Historia.add_dates(hist_df)
        logger.info('Finished format, writing result..')
        Historia.write_file('states', task_id, '80')
        # Format Result
        # Add Retailer Format
        ret_res = []
        for _gk, _gdf in hist_df.groupby(['retailer']):
            _tmp = {
                "name": _gk, "data": []
            }
            # Once grouped by retailer aggregate by interval
            for _dk, _ddf in _gdf.groupby(Historia\
                                    .grouping_cols[interval]):
                _tmp['data'].append([
                        int(_ddf.time_js.min()),
                        round(_ddf.price.mean(), 2)
                    ])
            # Append to result
            ret_res.append(_tmp)
        logger.info('Created Retailer result')
        # Add Metrics Format
        met_res = {'avg': [], 'min': [], 'max': []}
        for mj, mrow in hist_df.groupby(Historia\
                                    .grouping_cols[interval]):
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
        Historia.write_file('states', task_id, '100')
        Historia.write_file('dumps', task_id, json.dumps(result))
        logger.info('Finished computing {}!'.format(task_id))
        


