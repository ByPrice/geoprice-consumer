import datetime
from uuid import UUID
import itertools
from io import StringIO
import math
import json
from collections import OrderedDict
from flask import g
import pandas as pd
import numpy as np
import requests
from app import errors, logger
from config import *
from app.models.item import Item
from app.models.task import Task
from app.utils.helpers import *


class Product(object):
    """ Class perform Query methods on Cassandra items

        TODO: Modify correctly methods to query existing Databases for 
        the enpoints and tasks, any useful methods can be here:
        https://github.com/ByPrice/byprice-price-history/blob/dev/app/product/models.py
    """

    @staticmethod
    def get_one():
        """
            Static Method to verify correct connection with Prices Cassandra Keyspace
        """
        try:
            q = g._db.query("SELECT product_uuid FROM price LIMIT 1")
        except:
            logger.error("Cassandra Connection error")
            return False
        for price in q:
            logger.info('Product UUID: ' + str(price.product_uuid))
        return {'msg':'Cassandra One Working!'}

    @staticmethod
    def get_by_store(i_uuid, p_uuid, lat, lng, radius=10.0, period=2):
        """ If `item_uuid` is passed, fetches assigned `product_uuid`s
            then queries over `price_by_product_date` with `product_uuid`
            and date. Then filters not valid stores after the location 
            sent and the minimum valid distance to take prices from. 
            Finally appends stores info from Geolocation Service and
            sorts response by price and distance.

            Params:
            -----
            i_uuid :  str
                Item UUID
            p_uuid :  str
                Product UUID
            lat : float
                Latitude
            lng : float
                Longitude
            radius  : float, default=10.0 
                Radius in kilometers
            period : int
                Amount of days to query backwards

            Returns:
            -----
            prods : list
                List of product prices 
        """
        logger.info("Executing by store query...")
        # If item_uuid is passed, call to retrieve
        # product_uuid's from Catalogue Service
        if i_uuid:
            prod_info = Item.get_by_item(i_uuid)
            prod_uuids = [str(p['product_uuid']) for p in prod_info]
        else: 
            prod_uuids = [str(p_uuid)]
        logger.debug("Found {} products in catalogue".format(len(prod_uuids)))
        # Generate days
        _days = tupleize_date(datetime.date.today(), period)
        # Perform query for designated item uuid and more recent than yesterday
        cass_query = """
            SELECT product_uuid, store_uuid, price, price_original,
            promo, url, time
            FROM price_by_product_date WHERE product_uuid = %s
            AND date = %s
            """
        qs = []
        # Iterate for each product-date combination
        for _p, _d in itertools.product(prod_uuids, _days):
            try:
                q = g._db.query(cass_query, 
                    (UUID(_p), _d),
                    timeout=10)
                if not q:
                    continue
                qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                continue
        logger.info("Fetched {} prices".format(len(qs)))
        logger.debug(qs[:1] if len(qs) > 1 else [])
        # Empty validation
        if len(qs) == 0:
            return []
        # Load Response into a DF
        filt_df = pd.DataFrame(qs)
        filt_df['item_uuid'] = str(i_uuid) if i_uuid else ''
        filt_df['store_uuid'] = filt_df.store_uuid.astype(str)
        filt_df['product_uuid'] = filt_df.product_uuid.astype(str)
        logger.info('Got prices, now fetching stores...')
        # Get all stores from geolocation and save into DF
        stores_df = get_all_stores()
        # Merge stores with prices to get (lat and lng)
        filt_df = pd.merge(filt_df,
            stores_df[['store_uuid', 'lat', 'lng', 'source']],
            on='store_uuid', how='left')
        try:
            # Filter by coordinates and radius size
            filt_df['distance'] = np\
                .arccos(np.sin(np.deg2rad(filt_df.lat))
                        * np.sin(np.deg2rad(lat))
                        + np.cos(np.deg2rad(filt_df.lat))
                        * np.cos(np.deg2rad(lat))
                        * np.cos(np.deg2rad(lng)
                                 - (np.deg2rad(filt_df.lng))
                                 )
                        ) * 6371
            filt_df = filt_df[filt_df['distance'] <= radius]
            # Drop store duplicates
            filt_df.sort_values(['time'],
                ascending=[0], inplace=True)
            filt_df.drop_duplicates(subset='store_uuid',
                keep='first', inplace=True)
            logger.info('Filtered response, got {} products'.format(len(filt_df)))            
        except Exception as e:
            logger.error(e)
            logger.warning('Could not Generate filters from from geolocation')
            return []
        # Compute discount
        filt_df['price_original'] = filt_df['price_original']\
            .apply(lambda x : x if (x and float(x) != 0.0) else np.nan)
        filt_df['price_original'] = filt_df.price_original\
            .combine_first(filt_df.price)
        filt_df['discount'] = 100.0 * \
            (filt_df.price_original - filt_df.price) / filt_df.price_original
        # Format date
        filt_df['date'] = filt_df['time'].astype(str)
        # Format response
        df_cols = ['item_uuid', 'product_uuid', 'price',
            'store_uuid', 'url', 'price_original',
            'discount', 'time', 'source', 'date', 'distance',
            'promo']
        prods = filt_df[df_cols]\
            .sort_values(by=['price', 'distance'])\
            .to_dict(orient='records')
        for i, irow in enumerate(prods):
            try:
                tmp_store = stores_df\
                            .loc[stores_df['store_uuid'] == irow['store_uuid']]\
                            .to_dict(orient="records")[0]
                logger.debug(tmp_store)
                d_time, d_name, d_address = Product.contact_store_info(tmp_store)
                # If centralized, generate record for each store
                prods[i]['store'] = {
                    'store_uuid' : tmp_store['store_uuid'],
                    'name': d_name,
                    'delivery_time': d_time,
                    'delivery_cost': float(tmp_store["delivery_cost"]) \
                        if tmp_store['delivery_cost'] is not None else None,
                    'delivery_radius': float(tmp_store["delivery_radius"]),
                    'address': d_address,
                    'latitude' : tmp_store['lat'],
                    'longitude' : tmp_store['lng'],
                    'postal_code' : str(tmp_store['zip']).zfill(5)
                }
            except Exception as e:
                logger.error(e)
                logger.warning('Issues formatting!')
        return prods

    @staticmethod
    def contact_store_info(tmp_store):
        """ Method to parse store contact info
        """
        d_time, d_name, d_address = '', '', ''
        try:
            d_time = tmp_store['delivery_time']
        except:
            pass
        try:
            d_name = tmp_store['name']
        except:
            pass
        try:
            d_address = tmp_store['full_address']
        except:
            pass
        return d_time, d_name, d_address
        
    @staticmethod
    def stores_by_ret(ret, stores):
        """ Return list of store dicts where are the same requested retailers"""
        rs, fs = [], stores.loc[stores.retailer == ret]
        for s in range(len(fs)):
            rs.append({'store_uuid':fs.iloc[s].uuid,
                        'lat': fs.iloc[s].lat,
                        'lng': fs.iloc[s].lng,
                        'postal_code': fs.iloc[s].zip})
        return rs

    @staticmethod
    def get_history_by_store(i_uuid, p_uuid, period=1):
        """ If `item_uuid` is passed, fetches assigned `product_uuid`s
            then queries over `price_by_product_date` with `product_uuid`
            and date. Then filters not valid stores after the location
            sent and the minimum valid distance to take prices from.

            Params:
            -----
            i_uuid :  str
                Item UUID
            p_uuid :  str
                Product UUID
            period : int
                Amount of days to query backwards

            Returns:
            -----
            metrics : dict
                JSON with Max, Min and Avg metrics
        """
        logger.debug("Executing by store history query...")
        # If item_uuid is passed, call to retrieve
        # product_uuid's from Catalogue Service
        if i_uuid:
            prod_info = Item.get_by_item(i_uuid)
            prod_uuids = [str(p['product_uuid']) for p in prod_info]
        else: 
            prod_uuids = [str(p_uuid)]
        # Generate days
        _days = tupleize_date(datetime.date.today(), period)
        # Perform query for designated item uuid and more recent than yesterday
        cass_query = """
            SELECT price, time
            FROM price_by_product_date
            WHERE product_uuid = %s
            AND date = %s
            """
        qs = []
        # Iterate for each product-date combination
        for _p, _d in itertools.product(prod_uuids, _days):
            try:
                q = g._db.query(cass_query, 
                    (UUID(_p), _d),
                    timeout=10)
                if not q:
                    continue
                qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                continue
        # Empty verification
        if len(qs) == 0:
            return {'history': {}, 'history_byretailer': {}}
        # Load Response into a DF
        bystore_df = pd.DataFrame(qs)
        bystore_df['date'] = bystore_df['time']\
            .apply(lambda x: x.date().isoformat())
        # Perform aggs
        _aggs = bystore_df.groupby('date').price\
            .agg(['min', 'max', 'mean'])
        # Format response
        stats_hist =  {
            'Máximo': _aggs['max'].reset_index()\
                .rename(columns={'max': 'price'})\
                .to_dict(orient='records'),
            'Mínimo': _aggs['min'].reset_index()\
                .rename(columns={'min': 'price'})\
                .to_dict(orient='records'),
            'Promedio': _aggs['mean'].reset_index()\
                .rename(columns={'max': 'price'})\
                .to_dict(orient='records')
        }
        return {'history': stats_hist, 'history_byretailer': {}}

    @staticmethod
    def generate_ticket(i_uuids, p_uuids, lat, lng, radius):
        """ Calls over `get_by_store` function to 
            retrieve valid data of several products
            in a loop.

            Params:
            -----
            i_uuids : list
                Item UUIDs
            p_uuids :  list
                Product UUIDs
            lat : float
                Latitude
            lng : float
                Longitude
            radius  : float, default=10.0 
                Radius in kilometers

            Returns:
            -----
            items : list
                List of  lists of product prices 
        """
        items = []
        # If Item UUIDs where sent
        if i_uuids:
            for i_uuid in i_uuids:
                items.append(
                    Product.get_by_store(i_uuid, None,
                        lat, lng, radius)
                )
        elif p_uuids:
            # If Product UUIDs where sent
            for p_uuid in p_uuids:
                items.append(
                    Product.get_by_store(None, p_uuid,
                        lat, lng, radius)
                )
        logger.info("Fetched {} product prices groups"\
            .format(len(items)))
        return items

    @staticmethod
    def get_store_catalogue(source, store_id, export=False):
        """ Query all items from certain store
            on the past 2 days leaving most recent.
            
            Params:
            -----
            source : str
                Source key
            store_id : str
                Store UUID
            export : bool, optional, default=False
                Object returning Flag

            Returns:
            -----
            prods : list -> export=False, pd.DataFrame -> export=True
                Product prices object
        """
        # Generate days
        _days = tupleize_date(datetime.date.today(), 2)
        
        cass_query = """
            SELECT product_uuid, price, promo,
            price_original, time,
            store_uuid
            FROM price_by_store 
            WHERE store_uuid = %s
            AND date = %s
            """
        qs = []
        # Iterate for each store-date combination
        for _s, _d in itertools.product([store_id], _days):
            try:
                q = g._db.query(cass_query,
                    (UUID(_s), _d),
                    timeout=120)
                if not q:
                    continue
                qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: "+str(e))
        if len(qs) == 0:
            if export:
                return pd.DataFrame()
            return []
        # Create DF and format datatypes
        df = pd.DataFrame(qs)
        df['product_uuid'] = df['product_uuid'].astype(str)
        df['store_uuid'] = df['store_uuid'].astype(str)
        df['source'] = source
        _ius = pd.DataFrame(
            Item.get_by_product(
                df['product_uuid'].tolist(),
                ['name', 'item_uuid', 'gtin']
            )
        )
        _ius['product_uuid'] = _ius['product_uuid'].astype(str)
        # Add Item UUIDs
        df = pd.merge(df,
            _ius[['item_uuid', 'product_uuid','name', 'gtin']],
            on='product_uuid', how='left')
        df.fillna('', inplace=True)
        # Fetch discount
        df['price_original'] = df['price_original']\
            .apply(lambda x : x if (x and float(x) != 0.0) else np.nan)
        df['price_original'] = df.price_original\
            .combine_first(df.price)
        df['discount'] = 100.0 * \
            (df.price_original - df.price) / df.price_original
        # Format date
        df['date'] = df['time'].astype(str)
        # Format response
        _fields = ['item_uuid', 'price', 'promo',
            'price_original', 'discount',
            'date', 'source', 'store_uuid'
        ]
        if export:
            # If Needs to retrieve table file
            return df
        return df[_fields].to_dict(orient='records')

    @staticmethod
    def get_count_by_store_24hours(retailer, store_id, last_hours=24):
        """ Query to retrieve quantity of items
            from certain store of the last hours
            defined.

            Params:
            -----
            retailer : str
                Source key
            store_id :  str
                Store UUID
            last_hours : int
                Last hours to look for
            
            Returns:
            -----
            res : dict
                Results dict
        """
        # Generate days
        _days = tupleize_date(datetime.date.today(), 2)
        _delta = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
        cass_query = """
            SELECT COUNT(1)
            FROM price_by_store
            WHERE store_uuid = %s
            AND date = %s
            AND time > %s
            """
        _count = 0
        # Iterate for each store-date combination
        for _s, _d in itertools.product([store_id], _days):
            try:
                q = g._db.query(cass_query,
                    (UUID(_s), _d, _delta),
                    timeout=120)
                if not q:
                    continue                
                _count += list(q)[0].count
            except Exception as e:
                logger.error("Cassandra Connection error: "+str(e))
        # Format response
        res = {
            'source': retailer,
            'store_uuid': store_id,
            'count': _count,
            'date_start': _delta.date().isoformat(),
            'date_end': datetime.date.today().isoformat()
        }
        logger.debug(res)
        return res

    
    @staticmethod
    def validate_count_engine(params):
        # Verify Request Params
        if 'retailer' not in params :
            raise errors.AppError("invalid_request", "retailer key missing")
        if 'store_uuid' not in params:
            raise errors.AppError("invalid_request", "store_uuid key missing")
        if 'date' not in params:
            raise errors.AppError("invalid_request", "date key missing")
        else:
            try:
                _date = datetime.datetime(*[int(d) for d in params['date'].split('-')])
            except Exception as e:
                logger.error(e)
                raise errors.AppError(80010, "Wrong Format: Date")
        return params


    @staticmethod
    def count_by_store_engine_task(task_id, params):
        """
        Get Count from store

        @Params:
         - "retailer" : retailer_key
         - "store_uuid" : store_uuid
         - "date" : date
         - "env" : env

        @Returns:
         - (flask.Response)  # if export: Mimetype else: JSON

        """
        # Param validation
        Product.validate_count_engine(params)

        result = Product.get_count_by_store_engine(
            task_id,
            params['retailer'],
            params['store_uuid'],
            params['date']
        )

        resp = {
            'data' : result,
            'msg' : 'Task completed'
        }
        logger.info('Finished computing {}!'.format(task_id))
        return resp


    @staticmethod
    def get_count_by_store_engine(task_id, retailer, store_uuid, date):
        """
            Method to query to retrieve quantity of items from certain store of the last hours defined
        """
        # Task initialization
        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        date_int = int(date.replace('-',''))
        cass_query = """
            SELECT count(product_uuid) AS rows 
            FROM price_by_store
            WHERE store_uuid = {store_uuid} AND 
            date = {date_int}
            """. format(store_uuid=UUID(store_uuid), date_int=date_int)
        logger.debug(cass_query)
        try:
            q = g._db.execute(cass_query, timeout=120)
        except Exception as e:
            logger.error("Cassandra Connection error: {error}".format(error=str(e)))
            return {'count': 0}
        task.progress = 100
        # Format response
        for row in q:
            prods = {
                        'count': row[0]
                    }
            logger.debug(prods)
            return prods
        return {'count': 0}


    @staticmethod
    def validate_count(params):
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
        if 'retailer' not in params:
            raise errors.AppError(40003, "Retailer param Missing!", 400)
        if 'store_id' not in params:
            raise errors.AppError(40003, "Store UUID param Missing!", 400)
        if 'date_start' not in params:
            raise errors.AppError(40003, "Start Date param Missing!", 400)
        if 'date_end' not in params:
            raise errors.AppError(40003, "End Date param Missing!", 400)

        return params
    

    @staticmethod
    def count_by_store_task(task_id, params):
        """ Start count by store task, first it validates parameters
            and then it builds a response

            Params:
            -----
            task_id:  str
                Task ID 
            params: dict
                Request Params
            
            Returns:
            -----
            flask.Response
                Prod Response
        """
        # Param validation
        Product.validate_count(params)

        result = Product.get_count_by_store(
            task_id,
            params['retailer'],
            params['store_id'],
            params['date_start'],
            params['date_end']
        )

        resp = {
            'data' : result,
            'msg' : 'Task completed'
        }
        logger.info('Finished computing {}!'.format(task_id))
        return resp



    @staticmethod
    def get_count_by_store(task_id, retailer, store_id, date_start, date_end):
        """ Query to retrieve quantity of items
            from certain store of time selected period

            Params:
            -----
            task_id :  str
                Task UUID
            retailer : str
                Source key
            store_id :  str
                Store UUID
            date_start : str
                Starting date (YYYY-MM-DD)
            date_end : str
                Ending date (YYYY-MM-DD)
            
            Returns:
            -----
            res : dict
                Results dict
        """
        # Task initialization
        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        # Generate days
        _d1 = datetime.datetime.strptime(date_start, '%Y-%m-%d')
        _d2 = datetime.datetime.strptime(date_end, '%Y-%m-%d')
        _days = tupleize_date(_d1.date(), (_d2-_d1).days)
        
        cass_query = """
            SELECT COUNT(1)
            FROM price_by_store
            WHERE store_uuid = %s
            AND date = %s
            """
        _count = 0
        # Iterate for each store-date combination
        for _s, _d in itertools.product([store_id], _days):
            try: 
                q = g._db.query(cass_query,
                    (UUID(_s), _d),
                    timeout=120)
                if not q:
                    continue                
                _count += list(q)[0].count
            except Exception as e:
                logger.error("Cassandra Connection error: "+str(e))
        task.progress = 50
        # Format response
        res = {
            'source': retailer,
            'store_uuid': store_id,
            'count': _count,
            'date_start': date_start,
            'date_end': date_end
        }
        logger.debug(res)
        task.progress = 100
        return res
    
    @staticmethod
    def get_st_catalog_file(store_uuid, store_name, retailer):
        """ Obtain all prices from certain store
            from the past 48hrs (2 days)

            Params:
            -----
            store_uuid : str
                Store UUID
            store_name : str
                Store Name
            retailer :  str
                Source Key
            
            Returns:
            _buffer : io.StringIO
                CSV file buffer
        """
        logger.debug('Fetching: {} {} products...'\
            .format(retailer, store_name))
        # Fetch catalogue info
        df = Product.get_store_catalogue(retailer,
            store_uuid, export=True)
        # DataFrame evaluation
        if df.empty:
            return None
        # Set Store name
        df['store'] = store_name
        # Select columns and Set indexes
        _fields = ['source', 'gtin', 'name',
            'price', 'promo', 'store']
        fdf = df[_fields]\
            .copy()\
            .set_index(['source','gtin','name'])
        logger.info('Finished formatting DF')
        # Generate Bytes Buffer
        _buffer = StringIO()
        iocsv = fdf.to_csv(_buffer)
        _buffer.seek(0)
        return _buffer

    @staticmethod
    def start_retailer_task(task_id, params):
        """ Start history product retailer task, first it validates parameters
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
                Product Response
        """
        logger.info('inside retailer task')
        # Verify Request Params
        item_uuid, prod_uuid = None, None
        if 'retailer' not in params:
            raise errors.AppError(80002, "Retailer parameter missing")
        if 'item_uuid' not in params:
            if 'prod_uuid' not in params:
                raise errors.AppError(80002, "Item/Product UUID parameter missing")
            prod_uuid = params['prod_uuid']
        else:
            item_uuid = params['item_uuid']    
        export = params['export'] if 'export' in params else False
        logger.debug(params)
        # Call function to fetch prices
        prod = Product\
            .get_prices_by_retailer(task_id, params['retailer'],
                item_uuid, prod_uuid, export)
        if not prod:
            raise errors.AppError(80009,
                "No prices in selected Retailer-Product pair")
        return {
            'data' : prod,
            'msg' : 'Retailer Task completed'
        }

    
    @staticmethod
    def get_prices_by_retailer(task_id, retailer, item_uuid, prod_uuid, export=False):
        """ Queries a product and returns its prices
            from each store of the requested retailer
            and general stats

            Params:
            -----
            retailer : str
                Source Key
            item_uuid : str
                Item UUID
            prod_uuid : str
                Item UUID
            export : bool, optional, default=False
                Exporting flag

            Returns:
            -----
            _buffer : io.StringIO
                File buffer 
            `or`

            stores : dict
                Store prices info
        """

        # Task initialization
        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        logger.debug('Fetching from {} ...'\
            .format(retailer))
        # If item_uuid is passed, call to retrieve
        # product_uuid's from Catalogue Service
        if item_uuid:
            prod_info = Item.get_by_item(item_uuid)
            prod_uuids = [str(p['product_uuid']) for p in prod_info]
        else: 
            prod_uuids = [str(prod_uuid)]
        logger.debug("Found {} products in catalogue".format(len(prod_uuids)))
        task.progress = 20
        # Generate days
        _days = tupleize_date(datetime.date.today(), 2)
        # Fetch Stores by retailer
        stores_j = fetch_store(retailer)
        store_ids = [s['uuid'] for s in stores_j]
        if not stores_j:
            return None
        logger.debug("Found {} stores".format(len(stores_j)))
        task.progress = 40
        # Execute Cassandra Query
        cass_query = """
            SELECT product_uuid, store_uuid,
            price, time
            FROM price_by_product_date 
            WHERE product_uuid = %s
            AND date = %s
            """
        qs = []
        # Iterate for each product-date combination
        for _p, _d in itertools.product(prod_uuids, _days):
            try:
                q = g._db.query(cass_query, 
                    (UUID(_p), _d),
                    timeout=10)
                if not q:
                    continue
                qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                continue
        logger.info("Fetched {} prices".format(len(qs)))
        task.progress = 80
        logger.debug(qs[:1] if len(qs) > 1 else [])
        # Empty validation
        if len(qs) == 0:
            return None
        # Generate DFs, add columns and filter
        cass_df = pd.DataFrame(qs)
        cass_df['store_uuid'] = cass_df['store_uuid'].astype(str)
        cass_df = cass_df[cass_df['store_uuid'].isin(store_ids)]
        cass_df['item_uuid'] = item_uuid
        # Compute previous Stats
        prev_df = cass_df\
            .sort_values(by=['time'], ascending=True)\
            .drop_duplicates(subset=['product_uuid','store_uuid'],
                            keep='first')
        # Filter prices by retailer and drop duplicates
        curr_df = cass_df\
            .sort_values(by=['time'], ascending=False)\
            .drop_duplicates(subset=['product_uuid','store_uuid'],
                            keep='first')
        if curr_df.empty:
            logger.error('No Recent prices')
            return None
        # Geolocation DF
        geo_df = pd.DataFrame(stores_j)
        geo_df.rename(columns={'uuid': 'store_uuid'}, inplace=True)
        geo_df['store_uuid'] = geo_df['store_uuid'].astype(str)
        # Conversion and merging        
        resp_df = pd.merge(curr_df[['store_uuid','price']],
                    geo_df[['name','store_uuid','lat','lng']],
                    how='left', on='store_uuid')
        resp_df['price'] = resp_df['price']\
            .apply(lambda x: round(x, 2))
        _stores = resp_df[['name','price', 'lat', 'lng']]\
                    .to_dict(orient="records")
        logger.info('Computed results!')
        # Generate Response depending Export
        if not export:
            return {
                'stores': _stores,
                'max': resp_df['price'].max(),
                'min': resp_df['price'].min(),
                'avg': resp_df['price'].mean(),
                'prev_max': round(prev_df['price'].max(), 2),
                'prev_min': round(prev_df['price'].min(), 2),
                'prev_avg': round(prev_df['price'].mean(), 2)
                }
        # Generate Bytes Buffer
        _buffer = StringIO()
        iocsv = resp_df[['name','price', 'lat', 'lng']]\
            .to_csv(_buffer)
        _buffer.seek(0)
        task.progress = 100
        logger.info('Finished computing {}!'.format(task_id))
        return _buffer

    @staticmethod
    def fetch_detail_price(stores, item, _t0, _t1=None):
        """ Method to query detail prices from C*

            Params:
            -----            
            stores : list
                Stores UUIDs
            item : str
                Item UUID
            _t0 : datetime.datetime
                Start Time formatted string
            _t1: datetime.datetime
                End Time formatted string

            Returns:
            -----
            c_resp :  list
                Response with store_uuid and price
        """
        # If item_uuid is passed, call to retrieve
        # product_uuid's from Catalogue Service
        prod_info = Item.get_by_item(item)
        prod_uuids = [str(p['product_uuid']) for p in prod_info]
        logger.info("Found {} products in catalogue".format(len(prod_uuids)))
        logger.debug(prod_uuids)
        # Generate days
        _period = (_t1-_t0).days if _t1 else 2
        _days = tupleize_date(_t0.date(), _period)
        logger.debug(_days)
        cass_query = """
            SELECT product_uuid,
            store_uuid, price, time
            FROM price_by_product_store 
            WHERE product_uuid = %s
            AND store_uuid = %s
            AND date = %s
            """
        qs = []
        # Iterate for each product-date combination
        for _p, _s, _d in itertools.product(prod_uuids, stores, _days):
            try:
                q = g._db.query(cass_query, 
                    (UUID(_p), UUID(_s), _d),
                    timeout=10)
                if not q:
                    continue
                qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                continue
        return qs

    @staticmethod
    def start_compare_details_task(task_id, params):
        """ Start history product retailer task, first it validates parameters
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
                Product Response
        """
        logger.info("Comparing pairs Retailer-Item")
        # Verify Params
        if 'fixed_segment' not in params:
            raise errors.AppError(80002, "Fixed Segment missing")
        if 'added_segments' not in params:
            raise errors.AppError(80002, "Added Segments missing")
        if not isinstance(params['fixed_segment'], dict):
            raise errors.AppError(80010, "Wrong Format: Fixed Segment")
        if not isinstance(params['added_segments'], list):
            raise errors.AppError(80010, "Wrong Format: Added Segments")
        if 'date' in params:
            try:
                _date = datetime.datetime(*[int(d) for d in params['date'].split('-')])
                if _date > datetime.datetime.utcnow().date():
                    _date = datetime.datetime.utcnow()
            except Exception as e:
                logger.error(e)
                raise errors.AppError(80010, "Wrong Format: Date")
        else:
            _date = datetime.datetime.utcnow()
        if 'format' in params:
            if not isinstance(params['format'], str):
                raise errors.AppError(80010, "Wrong Format: Format")
        else:
            params['format'] = 'json'
        # Call function to fetch prices
        prod = Product\
            .get_pairs_ret_item(task_id, 
                params['fixed_segment'],
                params['added_segments'],
                _date,
                params['format'])
        if not prod:
            logger.warning("Not able to fetch prices.")
            raise errors.AppError(80009,
                "No prices with that Retailer and item combination.")
        return {
            'data' : prod,
            'msg' : 'Retailer Task completed'
        }
            
    @staticmethod
    def get_pairs_ret_item(task_id, fixed, added, date, format):
        """ Compare segments of pairs (retailer-item)
            between the fixed against all the added

            Params:
            -----
            fixed : dict
                First Pair of Retailer-Item
            added : list
                Pairs of Retailer-Item's
            date : datetime.datetime
                Date of comparison
            format : str
                Response Format

            Input Example:
            -----
            params {
            "fixed_segment" : {
                "store_uuid": UUID,
                "item_uuid" : UUID,
                "retailer": str,
                "name": str
            },
            "added_segments" : [
                {
                "store_uuid": UUID,
                    "item_uuid" : UUID,
                    "retailer": str,
                    "name": str
                }
            ],
            "date": str(YYYY-MM-DD)
        }

            Returns:
            -----
            compares : dict
                JSON response of comparison
        """
        # Task initialization
        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        # Fetch Retailers and Time
        logger.debug("Setting Comparison...")
        _rets = [fixed['retailer']] + [x['retailer'] for x in added]
        _ret_keys = [{'key': r} for r in _rets]
        _time = date - datetime.timedelta(days=1)
        # Create Geo DF
        geo_df = get_all_stores(_ret_keys)
        logger.info("Fetched {} stores".format(len(geo_df)))
        task.progress = 30
        # Fetch Fixed Prices DF
        fix_price = Product.fetch_detail_price(
                geo_df[geo_df['source'] == fixed['retailer']]\
                    ['store_uuid'].tolist(),
                fixed['item_uuid'], _time)
        if not fix_price:
            raise errors.AppError(80009, "No available prices for that combination.")
        # Fetch Added Prices DF
        added_prices = []
        for _a in added:
            added_prices\
                .append(
                    Product.fetch_detail_price(
                        geo_df[geo_df['source'] == _a['retailer']]\
                            ['store_uuid'].tolist(),
                        _a['item_uuid'], _time)
                )

        task.progress = 60
        # Build Fix DF, cast and drop dupls
        fix_df = pd.DataFrame(fix_price)
        fix_df['store_uuid'] = fix_df['store_uuid'].astype(str)
        fix_df['product_uuid'] = fix_df['product_uuid'].astype(str)
        fix_df['item_uuid'] = fixed['item_uuid']
        fix_df.sort_values(by=["time"], ascending=False, inplace=True)
        fix_df.drop_duplicates(subset=['store_uuid'], inplace=True)
        # Add Geolocated info and rename columns
        fix_df = pd.merge(
            fix_df[['item_uuid', 'product_uuid',
                    'store_uuid','price','time']],
            geo_df[['store_uuid','source',
                    'lat','lng', 'name']],
            how='left', on="store_uuid")
        fix_df.rename(columns={'name':'store'},
                    inplace=True)
        logger.info('Built Fixed DF')
        added_dfs = []
        # Loop over all the added elements
        for j, _a in enumerate(added_prices):
            if len(_a) == 0:
                added_dfs.append(pd.DataFrame())
                continue
            # Build Added DF, cast and drop dupls
            _tmp = pd.DataFrame(_a)
            _tmp['store_uuid'] = _tmp['store_uuid'].apply(lambda x: str(x))
            _tmp['item_uuid'] = added[j]['item_uuid']
            _tmp.sort_values(by=["time"], ascending=False, inplace=True)
            _tmp.drop_duplicates(subset=['store_uuid'], inplace=True)
            # Add Geolocated info and rename columns
            _tmp = pd.merge(
                _tmp[['item_uuid', 'product_uuid',
                    'store_uuid','price','time']],
                geo_df[['store_uuid','source',
                        'lat','lng', 'name']],
                how='left', on="store_uuid")
            _tmp.rename(columns={'name':'store'},
                        inplace=True)
            _tmp['store_uuid'] = _tmp.store_uuid.astype(str)
            _tmp['product_uuid'] = _tmp.product_uuid.astype(str)
            # Added to the list
            added_dfs.append(_tmp)
        logger.debug('Built Added DFs')
        if format == 'csv':
            # CSV like formatter
            csv_df = []
            # Fetch Fix Name
            _dfs = [fix_df] + added_dfs
            for ad_f in _dfs:
                # Fetch product name
                if ad_f.empty:
                    continue
                ad_name = ad_f['item_uuid'].tolist()[0]
                try:
                    #ad_name = requests.get("http://"+SRV_ITEM+"/item/info/"+ad_name).json()['names'][0]
                    ad_name = requests.get("http://"+SRV_CATALOGUE+"/item/details?uuid="+ad_name).json()['name'].upper()
                except:
                    pass
                # Fetch store name
                _df = ad_f.copy()
                _df['store_name'] = _df\
                    .apply(lambda z: z['store'].upper() \
                            if z['source'].upper() in z['store'].upper() \
                            else ' '.join([z['source'].upper(), z['store'].upper()]), 1)
                tmp_csv = _df[['store_name', 'price']]\
                            .rename(columns={'price': ad_name})\
                            .drop_duplicates('store_name')\
                            .set_index('store_name').T\
                            .reset_index()\
                            .rename(columns={'index':'producto'})\
                            .set_index('producto')
                csv_df.append(tmp_csv)
                #csv_cols.union(set(list(tmp_csv.columns)))
            df_to_exp = pd.concat(csv_df)
            task.progress = 100
            return df_to_exp.reset_index().to_dict(orient='records')
        # Construct Response
        _rows = []
        for _j, _jrow in fix_df.iterrows():
            # Iterate over all fixed and retrieve data
            _jth = {
                'fixed': _jrow[['store',
                            'source',
                            'item_uuid',
                            'product_uuid',
                            'price']].to_dict()
            }
            _segs = []
            for _ith, _deepai in enumerate(added_dfs):
                # Use deep copy for calculations
                _ai = _deepai.copy()
                # Set unfound price like dict
                _jkth = {
                    "store": None,
                    "source": added[_ith]['retailer'],
                    "item_uuid": added[_ith]['item_uuid'],
                    "product_uuid": _ai.product_uuid.tolist()[0],
                    "price": None,
                    "diff": None,
                    "dist": None
                }
                if len(_ai) == 0:
                    _segs.append(_jkth)
                    continue
                # Compute Distance
                _ai['dist'] = np\
                    .arccos(np.sin(np.deg2rad(_ai.lat))
                        * np.sin(np.deg2rad(_jrow['lat']))
                        + np.cos(np.deg2rad(_ai.lat))
                        * np.cos(np.deg2rad(_jrow['lat']))
                        * np.cos(np.deg2rad(_jrow['lng'])
                                 - (np.deg2rad(_ai.lng))
                                 )
                        ) * 6371
                # Sort by distance
                _ai.sort_values(by=['dist'], inplace=True)
                _ai.reset_index(inplace=True)
                # Update jkth element
                _jkth.update(_ai.loc[0][['store', 'price', 'dist']]\
                                .to_dict())
                # Add difference (Fixed - Added)
                _jkth['dist'] = round(_jkth['dist'], 2)
                _jkth['diff'] = _jrow['price'] - _ai.loc[0]['price']
                # Add to segments
                _segs.append(_jkth)
            # Add computed Segments to Row
            _jth['segments'] = _segs
            # Add row to Rows
            _rows.append(_jth)            
            logger.debug(_j)
        logger.info('Created Rows!')
        task.progress = 80
        # Generate Fixed Stores
        _valid_sts = []
        fix_df['name'] = fix_df['store'].astype(str)
        _valid_sts.append({
            'source': fix_df.loc[0]['source'],
            'item_uuid': fix_df.loc[0]['item_uuid'],
            'product_uuid': fix_df.loc[0]['product_uuid'],
            'stores' : [_r.to_dict() for _l, _r in 
                        fix_df[['store_uuid','name']]\
                        .drop_duplicates(subset=['store_uuid'])\
                        .iterrows()]})
        # Generate Added Stores
        for _adf in added_dfs:
            if len(_adf) == 0:
                continue
            _adf['name'] = _adf['store'].apply(lambda x: str(x))
            _valid_sts.append({
                'source': _adf.loc[0]['source'],
                'item_uuid': _adf.loc[0]['item_uuid'],
                'product_uuid': _adf.loc[0]['product_uuid'],
                'stores' : [_r.to_dict() for _l, _r in 
                            _adf[['store_uuid','name']]\
                            .drop_duplicates(subset=['store_uuid'])\
                            .iterrows()]})
        logger.info('Generated Stores!')
        task.progress = 100
        return {
            'date': date.isoformat(),
            'segments': _valid_sts,
            'rows': _rows
        }

    @staticmethod
    def validate_store_item(params):
        "Validates params for store item"
        # Existance verif
        if 'fixed_segment' not in params:
            raise errors.AppError(80002, "Fixed Segment missing")
        if 'added_segments' not in params:
            raise errors.AppError(80002, "Added Segments missing")
        # Datatype verif
        if not isinstance(params['fixed_segment'], dict):
            raise errors.AppError(80010, "Wrong Format: Fixed Segment")
        if not isinstance(params['added_segments'], list):
            raise errors.AppError(80010, "Wrong Format: Added Segments")
        # Dates verif
        if ('date_ini' not in params) or ('date_fin' not in params):
            raise errors.AppError(80002, "Missing Dates params")
        if 'interval' in params:
            if params['interval'] not in ['day','week','month']:
                raise errors.AppError(80010, "Wrong Format: interval type")
        else:
            params['interval'] = 'day'
        return params


    @staticmethod
    def compare_store_item_task(task_id, params):
        """ Start Compare store item task, first it validates parameters
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
        # Validate params
        Product.validate_store_item(params)
        # Parse and start task
        prod = Product.get_pairs_store_item(
            task_id,
            params['fixed_segment'],
            params['added_segments'],
            params
        )
        if not prod:
            raise errors.AppError(80009,
                "No products with that Store and item combination.") 
        resp = {
            'data' : prod,
            'msg' : 'Task completed'
        }
        logger.info('Finished computing {}!'.format(task_id))
        return resp


    @staticmethod
    def get_pairs_store_item(task_id, fixed, added, params):
        """ Compare segments of pairs (store-item)

            Params:
            -----
            fixed : dict
                First Pair of Store-Item
            added : list
                Pairs of Store-Item's
            params : dict
                Dates and Interval type

            Returns:
            -----
            _resp : dict
                JSON response of comparison
        """
        # Task initialization
        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        # Vars
        _resp = {}
        # Obtain distances
        _rets = [fixed['retailer']] + [x['retailer'] for x in added]
        dist_dict = obtain_distances(fixed['store_uuid'],
                    [x['store_uuid'] for x in added],
                    _rets)
        task.progress = 10
        # Obtain date groups
        date_groups = grouping_periods(params)
        task.progress = 20
        # Fetch fixed prices
        fix_store = Product\
            .fetch_detail_price(
                    [fixed['store_uuid']],
                    fixed['item_uuid'],
                    date_groups[0][0],
                    date_groups[-1][-1])
        task.progress = 60
        if not fix_store:
            raise errors.AppError(80009, "No available prices for that combination.")
        fix_st_df = pd.DataFrame(fix_store)
        fix_st_df['name'] = fixed['name']
        fix_st_df['time_js'] = fix_st_df['time'].apply(date_js())
        ##### 
        # TODO:
        # Add agroupation by Interval for graph
        #####
        # Added Fixed Response
        _resp['fixed'] = {
            'name': fixed['name'],
            "data": [[_t['time_js'], round(_t['price'], 2)]
                        for _i, _t in fix_st_df.iterrows()],
            "max": fix_st_df['price'].max(),
            "min": fix_st_df['price'].min(),
            "avg": fix_st_df['price'].mean(),
            "std": fix_st_df['price'].std() if fix_st_df['price'].count() > 1 else 0
        }
        logger.info('Fetched fixed values')
        # Fetch added prices
        _resp['segments'] =[]
        task.progress = 70
        for _a in added:
            _tmp_st =  Product.fetch_detail_price(
                    [_a['store_uuid']],
                    _a['item_uuid'],
                    date_groups[0][0],
                    date_groups[-1][-1])
            if not _tmp_st:
                logger.warning("{} store with no Prices".format(_a['store_uuid']))
                continue
            # Construct DF
            _tmp_df = pd.DataFrame(_tmp_st)
            _tmp_df['name'] = _a['name']
            _tmp_df['time_js'] = _tmp_df['time'].apply(date_js())
            # Format dict
            _tmp_rsp = {
                'name': _a['name'],
                "data": [[_t['time_js'], round(_t['price'], 2)]
                            for _i, _t in _tmp_df.iterrows()],
                "max": _tmp_df['price'].max(),
                "min": _tmp_df['price'].min(),
                "avg": _tmp_df['price'].mean(),
                "std": _tmp_df['price'].std() if _tmp_df['price'].count() > 1 else 0,
                "dist": dist_dict[_a['store_uuid']] 
            }
            # Add to segments
            _resp['segments'].append(_tmp_rsp)
        task.progress = 100
        logger.info('Fetched all segments')
        # Construct response
        return _resp

    @staticmethod
    def get_stats(item_uuid, prod_uuid):
        """ Get max, min, avg price from 
            item_uuid or product_uuid

            Params:
            -----
            item_uuid : str
                Item UUID
            prod_uuid : str
                Product UUID

            Returns:
            -----
            _stats : dict
                JSON response
        """
        # If item_uuid is passed, call to retrieve
        # product_uuid's from Catalogue Service
        if item_uuid:
            prod_info = Item.get_by_item(item_uuid)
            prod_uuids = [str(p['product_uuid']) for p in prod_info]
        else: 
            prod_uuids = [str(prod_uuid)]
        logger.debug("Found {} products in catalogue".format(len(prod_uuids)))
        # Generate days
        _days = tupleize_date(datetime.date.today(), 2)
        cass_query = """
            SELECT MAX(price) as max,
                MIN(price) as min,
                AVG(price) as avg
            FROM price_by_product_date
            WHERE product_uuid=%s
            AND date=%s
            """
        qs = []
        # Iterate for each product-date combination
        for _p, _d in itertools.product(prod_uuids, _days):
            try:
                q = g._db.query(cass_query, 
                    (UUID(_p), _d),
                    timeout=10)
                if not q:
                    continue
                qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                continue
        if len(qs) == 0:
            return {}
        # Fetch agg values:        
        df = pd.DataFrame(qs)
        df.fillna(0.0, inplace=True)
        _stats = {
            'avg_price' : round(df['avg'].mean(), 2),
            'max_price' : round(df['max'].max(), 2),
            'min_price' : round(df['min'].min(), 2)
        }
        if _stats['avg_price'] == 0:
            return {}
        return _stats


    @staticmethod
    def validate_count_ret_eng_params(params):
        if 'retailer' not in params :
            raise errors.AppError(80002,
                "Retailer param missing")
        if 'date' not in params:
            raise errors.AppError(80002,
                "Date param missing")
        logger.debug(params)
        return params


    @staticmethod
    def count_by_retailer_engine_task(task_id, params):
        # Validate params
        Product.validate_count_ret_eng_params(params)
        # Parse and start task
        prod = Product.count_by_retailer_engine(
            task_id,
            params['retailer'],
            params['date']
        )

        resp = {
            'data' : prod,
            'msg' : 'Task completed'
        }
        logger.info('Finished computing {}!'.format(task_id))
        return resp


    @staticmethod
    def count_by_retailer_engine(task_id, retailer, _date):
        """ Get max, min, avg price from 
            item_uuid or product_uuid

            Params:
            -----
            retailer : str
                Source / Retailer key
            _date : str
                Date (YYYY-MM-DD HH:mm:SS)

            Returns:
            -----
            _count : dict
                JSON response
        """
        # Task initialization
        task = Task(task_id)
        task.task_id = task_id

        # Format time
        _time = datetime.datetime\
            .strptime(_date, '%Y-%m-%d %H:%M:%S')
        _time_plus = _time + datetime.timedelta(hours=1)
        # Generate days
        _days = tupleize_date(_time.date(), 2)
        stores = g._geolocation.get_stores(rets=[retailer])
        uuids = [ st['uuid'] for st in stores if 'uuid' in st ]
        tot = len(uuids) * len(_days)
        unit = 100/tot
        cass_query = """
            SELECT COUNT(1) AS count 
            FROM price_by_store
            WHERE store_uuid = %s 
            AND date = %s
            AND time > %s
            AND time < %s
            """
        qs = []
        count = 0
        for _d in _days:
            for store_uuid in uuids:
                count += 1
                task.progress = int(count*unit)
                try:
                    q = g._db.query(cass_query, 
                            (UUID(store_uuid), _d, _time, _time_plus),
                            timeout=30)
                    if not q:
                        continue
                    qs += list(q)
                except Exception as e:
                    logger.error("Cassandra Connection error: " + str(e))
                    continue
        if len(qs) == 0:
            return {'count' : 0}
        # Fetch agg values:        
        df = pd.DataFrame(qs)
        df.fillna(0.0, inplace=True)
        _count = {
            'count' : int(df['count'].sum()),
        }
        logger.info('Found {} points for {} ({} - {})'\
            .format(_count['count'], retailer,
                    _time, _time_plus))
        task.progress = 100
        return _count


    @staticmethod
    def start_compare_stores_task(task_id, params):
        """ Start history product retailer task, first it validates parameters
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
                Product Response
        """
        logger.info("Comparing pairs Ret-Item Stores")
        # Verify Params
        if 'fixed_segment' not in params:
            raise errors.AppError("invalid_request", "Fixed Segment missing")
        if 'added_segments' not in params:
            raise errors.AppError("invalid_request", "Added Segments missing")
        if not isinstance(params['fixed_segment'], dict):
            raise errors.AppError("invalid_request", "Wrong Format: Fixed Segment")
        if not isinstance(params['added_segments'], list):
            raise errors.AppError("invalid_request", "Wrong Format: Added Segments")
        if 'territory' not in params:
            params['territory'] = []
        else:
            if not isinstance(params['territory'], list):
                raise errors.AppError("invalid_request", "Wrong Format: Territory")
        if 'date' in params:
            try:
                _date = datetime.datetime(*[int(d) for d in params['date'].split('-')])
                if _date > datetime.datetime.utcnow().date():
                    _date = datetime.datetime.utcnow()
            except Exception as e:
                logger.error(e)
                raise errors.AppError("invalid_request", "Wrong Format: Date")
        else:
            _date = datetime.datetime.utcnow()
        # Call function to fetch prices
        prod = Product.get_pairs_ret_item_stores(task_id, 
                                params['fixed_segment'],
                                params['added_segments'],
                                _date,
                                params['territory'])
        if not prod:
            logger.error("Not able to fetch prices.")
            return jsonify([])
        
        return {
            'data' : prod,
            'msg' : 'Compare Stores Task completed'
        }


    @staticmethod
    def get_pairs_ret_item_stores(task_id, fixed, added, date, territory):
        """ Method to compare segments of pairs (retailer-item) and stores

            @Params:
            - fixed : (dict) First Pair of Retailer-Item
            - added : (list) Pairs of Retailer-Item's
            - date : (datetime.datetime) Date of comparison
            - territory : (list) List of Stores that apply

            @Returns:
            dict
                JSON buffer response of comparison
        """
        # Task initialization
        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        logger.debug("Setting Comparison...")
        # Fetch Retailers and Time
        _rets = [fixed['retailer']] + [x['retailer'] for x in added]
        _time = (date - datetime.timedelta(days=1)).date().__str__()
        _time_f = (date + datetime.timedelta(days=1)).date().__str__()
        # Create Geo DF
        _st_list = []
        for _r in _rets:
            try:
                _stj = requests\
                        .get(SRV_PROTOCOL+"://"+SRV_GEOLOCATION+"/store/retailer?key="+_r)\
                        .json()
                for _i, _s in enumerate(_stj):
                    _stj[_i].update({'retailer': _r})
            except Exception as e:
                logger.error(e)
                raise errors.AppError("stores_issue",
                            "Could not fetch Stores!")
            _st_list += _stj
        geo_df = pd.DataFrame(_st_list)
        geo_df['store_uuid'] = geo_df['uuid'].apply(lambda x: str(x))
        logger.debug('Got Stores!')
        task.progress = 10
        # Filter Territories
        if territory:
            geo_df = geo_df[geo_df.store_uuid.isin(territory)]
            if geo_df.empty:
                raise errors.AppError('no_stores', 'No stores in selected territory and retailers.')
        # Fetch Fixed Prices DF
        fix_price = Product.fetch_detail_price(
                                geo_df[geo_df['retailer'] == fixed['retailer']]['store_uuid'].tolist(),
                                fixed['item_uuid'],
                                datetime.datetime.strptime(_time, '%Y-%m-%d'),
                                datetime.datetime.strptime(_time_f, '%Y-%m-%d'))
        task.progress = 40
        # Fetch Added Prices DF
        added_prices = []
        for _a in added:
            added_prices.append(
                Product.fetch_detail_price(
                        geo_df[geo_df['retailer'] == _a['retailer']]['store_uuid'].tolist(),
                        _a['item_uuid'],
                        datetime.datetime.strptime(_time, '%Y-%m-%d'),
                        datetime.datetime.strptime(_time_f, '%Y-%m-%d')
                    )
                )
        task.progress = 50
        # Build Fix DF, cast and drop dupls
        fix_df = pd.DataFrame(fix_price)
        if not fix_df.empty:
            fix_df['store_uuid'] = fix_df['store_uuid'].apply(lambda x: str(x))
            fix_df['item_uuid'] = fixed['item_uuid']
            fix_df.sort_values(by=["time"], ascending=False, inplace=True)
            fix_df.drop_duplicates(subset=['store_uuid'], inplace=True)
            # Add Geolocated info and rename columns
            fix_df = pd.merge(fix_df[['item_uuid','store_uuid','price','time']],
                geo_df[['store_uuid','retailer','lat','lng', 'name']],
                how='left', on="store_uuid")
            fix_df.rename(columns={'name':'store'},
                            inplace=True)
        logger.debug('Built Fixed DF')
        added_dfs = []
        # Loop over all the added elements
        for _a in added_prices:
            if len(_a) == 0:
                added_dfs.append(pd.DataFrame())
                continue
            # Build Added DF, cast and drop dupls
            _tmp = pd.DataFrame(_a)
            _tmp['store_uuid'] = _tmp['store_uuid'].apply(lambda x: str(x))
            _tmp['item_uuid'] = added[0]['item_uuid']
            _tmp.sort_values(by=["time"], ascending=False, inplace=True)
            # Add Geolocated info and rename columns
            _tmp = pd.merge(_tmp[['item_uuid','store_uuid','price','time']],
                geo_df[['store_uuid','retailer','lat','lng', 'name']],
                how='left',
                on="store_uuid")
            _tmp.rename(columns={'name':'store'},
                        inplace=True)
            # Added to the list
            added_dfs.append(_tmp)
        logger.debug('Built Added DFs')
        task.progress = 70
        # Construct Response 
        resp_df = []
        # Fetch Fix Name
        _dfs = [fix_df] + added_dfs
        for ad_f in _dfs:
            # Fetch product name
            if ad_f.empty:
                continue
            ad_name = ad_f['item_uuid'].tolist()[0]
            try:
                ad_name = requests.get(SRV_PROTOCOL+"://"+SRV_CATALOGUE+"/item/details?uuid="+ad_name).json()['name'].upper()
            except:
                pass
            _df = ad_f.copy()
            tmp_csv = _df[['store_uuid', 'price']]\
                        .rename(columns={'price': ad_name})\
                        .drop_duplicates('store_uuid')\
                        .set_index('store_uuid').T\
                        .reset_index()\
                        .rename(columns={'index': 'item'})
            tmp_csv['item_uuid'] = ad_f['item_uuid'].tolist()[0]
            resp_df += tmp_csv.to_dict(orient='records')
        task.progress = 100
        return resp_df