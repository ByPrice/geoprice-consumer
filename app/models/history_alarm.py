import datetime
from uuid import UUID
from io import StringIO
import math
import json
import itertools
from collections import OrderedDict
from flask import g
import pandas as pd
import numpy as np
import requests
from app import errors, logger
from config import *
from app.models.task import Task
from app.models.item import Item
from app.models.stats import Stats
from app.utils.helpers import *


class Alarm(object):
    """ Class perform query methods 
        on Cassandra products over elements
        to verify change.

        # TODO: Reference for additional needed methods:
        https://github.com/ByPrice/byprice-price-history/blob/dev/app/alarm/models.py
    """

    def __init__(self):
        pass

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
        if 'uuids' not in params:
            raise errors.AppError(40003, "Uuids param Missing!", 400)
        elif not isinstance(params['uuids'], list):
            raise errors.AppError(40003, "Uuids param must be a list!", 400)                
        if 'retailers' not in params:
            raise errors.AppError(40003, "Retailers param Missing!", 400)
        elif not isinstance(params['retailers'], list):
            raise errors.AppError(40003, "Retailers param must be a list!", 400)
        if 'today' not in params:
            params['today'] = datetime.datetime.utcnow()
        else:
            params['today'] = params['today']
        return params


    @staticmethod
    def start_task(params):
        """ Start History price alert task, first it validates parameters
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
                Alarm Response
        """
        
        # Validate params
        Alarm.validate_params(params)
        # Parse and start task
        response = Alarm.prices_vs_prior(
            params
        )
        return response
    

    @staticmethod
    def get_cassandra_prices(prods, _date, period):
        """ Query prices of product by date
            
            Params:
            -----
            prods : list
                List of products
            dates : list
                List of dates
            period : int
                Period of prev days
            
            Returns
            -----
            df : pandas.DataFrame
                Product prices
        """

        # Generate dates
        _days = tupleize_date(_date.date(), period)

        chunk_size = int(30 / len(_days))
        logger.debug('chunk size')
        logger.debug(chunk_size)

        # Fetch prod uuids
        puuids = [p['product_uuid'] for p in prods]
        chunk_puuids = Alarm.divide_chunks(puuids, chunk_size)

        cass_query = """SELECT product_uuid, price,
                store_uuid, time
                FROM price_by_product_date
                WHERE product_uuid in ({})
                AND date in {}"""

        qs = []

        for puuids in chunk_puuids:
            cass_query_text = cass_query.format(', '.join(puuids), str(_days))
            logger.debug(cass_query_text)

            try:
                q = g._db.query(cass_query_text,
                                timeout=2000)
                if q:
                    qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))

        logger.info("Fetched {} prices".format(len(qs)))
        # logger.debug(qs[:1] if len(qs) > 1 else [])
        # Empty validation
        if len(qs) == 0:
            return pd.DataFrame({'date':[], 'product_uuid':[]})
        # Load Response into a DF
        df = pd.DataFrame(qs)
        df['product_uuid'] = df.product_uuid.astype(str)
        return df

    @staticmethod
    def prices_vs_prior(params):
        """ Compute prices for alarms.
            
            Params:
            -----
            params : dict
                Request params: uuids, retailers and date.

            Returns:
            -----
            prices : dict
                Dict containing list of current and prior prices
        """
        logger.debug('Fetching Alarm prices...')
        # Format params
        params['filters'] = [{'item_uuid': i} for i in params['uuids']]
        params['filters'] += [{'retailer': i} for i in params['retailers']]
        rets = Stats.fetch_rets(params['filters'])

        if not rets:
            raise errors.AppError(80011, "No retailers found.")
        # Items from service
        filt_items = Stats\
            .fetch_from_catalogue(params['filters'], rets)
        if not filt_items:
            logger.warning("No Products found!")
            return {'today':[], 'prevday':[]}
        
        # Date
        if isinstance(params['today'], datetime.datetime):
            _now = params['today']
        else:
            _now = datetime.datetime\
                .strptime(str(params['today']), '%Y-%m-%d')
        try:
            # Today prices
            today_df = Alarm.get_cassandra_prices(filt_items, 
                _now, 2)
            
            # Yesterday prices
            yday_df = Alarm.get_cassandra_prices(filt_items, 
                _now - datetime.timedelta(days=1), 2)
            logger.debug('Prices fetched from Cassandra...')
        except Exception as e:
            logger.error(e)
            raise errors.AppError(80005, "Could not retrieve data from DB.")
        if today_df.empty:
            logger.warning("No Products found today!")
            return {'today':[], 'prevday':[]}
        # Products DF
        info_df = pd.DataFrame(filt_items,
            columns=['item_uuid', 'product_uuid',
                'name', 'gtin', 'source'])
        # Add item_uuid to retrieve elements
        today_df = pd.merge(today_df, info_df,
            on='product_uuid', how='left')
        yday_df = pd.merge(yday_df, info_df,
            on='product_uuid', how='left')
        ### TODO
        # Add rows with unmatched products!
        non_matched = today_df[today_df['item_uuid'].isnull() | 
            (today_df['item_uuid'] == '')].copy()
        ### END TODO
        # Format only products with matched results
        today_df = today_df[~(today_df['item_uuid'].isnull()) & 
            (today_df['item_uuid'] != '')]
        yday_df = yday_df[~(yday_df['item_uuid'].isnull()) & 
            (yday_df['item_uuid'] != '')]
        # Filter elements with not valid retailers
        today_df = today_df[today_df['source'].isin(rets)]
        if not yday_df.empty:
            yday_df = yday_df[yday_df['source'].isin(rets)]
        # Convert datetime to date
        today_df['date'] = today_df['time'].apply(lambda x : x.date().__str__())
        if not yday_df.empty:
            yday_df['date'] = yday_df['time'].apply(lambda x : x.date().__str__())
        # Order  and Drop duplicates
        today_df.sort_values(by=['item_uuid','source', 'price'], inplace=True)
        if not yday_df.empty:
            yday_df.sort_values(by=['item_uuid','source', 'price'], inplace=True)
        today_df.drop_duplicates(subset=['item_uuid', 'source'], keep='first', inplace=True)
        if not yday_df.empty:
            yday_df.drop_duplicates(subset=['item_uuid', 'source'], keep='first', inplace=True)
        # Drop Store UUID and time column
        today_df.drop(['store_uuid', 'product_uuid',
            'name', 'time', 'gtin'], inplace=True, axis=1)
        if not yday_df.empty:
            yday_df.drop(['store_uuid', 'product_uuid',
                'name', 'time', 'gtin'], inplace=True, axis=1)
        logger.debug('Dataframes filtered!')
        resp = {
            'today': today_df\
                        .rename(columns={'source':'retailer'})\
                        .to_dict(orient='records'),
            'prevday': yday_df\
                        .rename(columns={'source':'retailer'})\
                        .to_dict(orient='records')
        }
        logger.info("Serving Alarm response..")
        # Convert in dict 
        return resp


    def divide_chunks(l, n): 
      
        # looping till length l 
        for i in range(0, len(l), n):  
            yield l[i:i + n] 

