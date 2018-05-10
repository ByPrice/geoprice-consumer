import datetime
import uuid
import operator
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
from app.utils.helpers import tupleize_date

class Product(object):
    """ Class perform Query methods on Cassandra items
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
            'url', 'price_original', 'discount', 'time',
            'source', 'date', 'distance', 'promo']
        prods = filt_df[df_cols]\
            .rename(columns={'price_original': 'previous_price'})\
            .sort_values(by=['price', 'distance'])\
            .to_dict(orient='records')
        for i, irow in enumerate(prods):
            try:
                tmp_store = stores_df\
                            .loc[stores_df['store_uuid'] == irow['store_uuid']]\
                            .to_dict(orient="records")[0]
                d_time, d_name, d_address = Product.contact_store_info(tmp_store)
                # If centralized, generate record for each store
                prods[i]['store'] = {
                    'store_uuid' : irow['store_uuid'],
                    'name': d_name,
                    'delivery_time': d_time,
                    'delivery_cost': float(tmp_store["delivery_cost"]) \
                        if tmp_store['delivery_cost'] is not None else None,
                    'delivery_radius': float(tmp_store["delivery_radius"]),
                    'address': d_address,
                    'latitude' : irow['lat'],
                    'longitude' : irow['lng'],
                    'postal_code' : str(irow['zip']).zfill(5)
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
            SELECT price 
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
            'Máximo': aggs['max'].reset_index()\
                .rename(columns={'max': 'price'})\
                .to_dict(orient='records'),
            'Mínimo': aggs['min'].reset_index()\
                .rename(columns={'min': 'price'})\
                .to_dict(orient='records'),
            'Promedio': aggs['mean'].reset_index()\
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
    def get_store_catalogue(source, store_id):
        """ Query all items from certain store
            on the past 2 days leaving most recent.
            
            Params:
            -----
            source : str
                Source key
            store_id : str
                Store UUID

            Returns:
            -----
            prods : list
                List of product prices
        """
        # Generate days
        _days = tupleize_date(datetime.date.today(), 2)
        cass_query = """
            SELECT product_uuid, price, 
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
            return []
        # Create DF and format datatypes
        df = pd.DataFrame(qs)
        df['product_uuid'] = df['product_uuid'].astype(str)
        df['store_uuid'] = df['store_uuid'].astype(str)
        df['source'] = source
        _ius = pd.DataFrame(
            Item.get_by_products(df['product_uuid'].tolist())
        )
        _ius['product_uuid'] = _ius['product_uuid'].astype(str)
        # Add Item UUIDs
        df = pd.merge(df, _ius[['item_uuid', 'product_uuid']],
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
        _fields = ['item_uuid', 'price',
            'price_original', 'discount',
            'date', 'source', 'store_uuid'
        ]
        return df[_fields].to_dict(orient='records')

    @staticmethod
    def get_count_by_store_24hours(retailer, store_id, last_hours=24):
        """
            Method to query to retrieve quantity of items from certain store of the last hours defined
        """
        cass_query = """
                    SELECT COUNT(*) FROM price_store 
                    WHERE retailer = '%s' 
                    AND store_uuid=%s
                    AND time > '%s'
                    """%(retailer,
                        store_id,
                        str(datetime.datetime.today() + datetime.timedelta(hours=-24))[:-7])
        logger.debug(cass_query)

        try: 
            q = g._db.execute(cass_query, timeout=120)
            # Format response
            for it in q:
                prods = {
                            'retailer' 		: retailer,
                            'store'			: store_id,
                            'count'			: str(it.count),
                            'date_end'		: str(datetime.date.today()),
                            'date_start'	: str(datetime.date.today() + datetime.timedelta(hours=-24))
                        }
                logger.debug(prods)
            return prods
            
        except Exception as e:
            logger.error("Cassandra Connection error: "+str(e))

    @staticmethod
    def get_count_by_store(retailer, store_id, date_start, date_end):
        """
            Method to query to retrieve quantity of items from certain store of the last hours defined
        """
        cass_query = """
                    SELECT item_uuid FROM price_store 
                    WHERE retailer = '%s' 
                    AND store_uuid=%s
                    AND time >= '%s'
                    AND time <  '%s'
                    """%(retailer,
                        store_id,
                        date_start,
                        str(datetime.datetime.strptime(date_end[2:], '%y-%m-%d').date()+ datetime.timedelta(days=+1)))
        logger.debug(cass_query)
        try: 
            q = g._db.execute(cass_query, timeout=120)
        except Exception as e:
            logger.error("Cassandra Connection error: "+str(e))
            return False
        # Format response
        prods = {
                    'retailer' 		: retailer,
                    'store_uuid'	: store_id,
                    'count'			: len(list(q)),
                    'date_start'	: date_start,
                    'date_end'		: date_end
                }
        logger.debug(prods)
        return prods
    
    @staticmethod
    def get_st_catag_file(store_uuid, store_name,retailer):
        """
            Method to obtain all Prices from certain store from the past 48hrs
        """
        logger.debug('Fetching: {}'.format(store_name))
        cass_query = """
                    SELECT item_uuid, retailer, price, promo FROM price_store 
                    WHERE retailer = '{}' 
                    AND store_uuid={}
                    AND time > '{}'
                    """.format(retailer,
                        store_uuid,
                        (datetime.datetime.utcnow()-datetime.timedelta(days=2)).isoformat()[:-4])
        logger.debug(cass_query)
        # Query all price catalog by store
        try:
            c_resp = list(g._db.execute(cass_query, timeout=120))
            logger.info('C* Prices fetched!')
            if not c_resp:
                raise Exception('Empty store')
        except Exception as e:
            logger.error(e)
            return False
        # Generate DF from prices
        df = pd.DataFrame(c_resp)
        df['item_uuid'] = df['item_uuid'].apply(lambda x: str(x))
        logger.info('DF created')
        # Query items names from endpoint
        try:
            names = requests.get('http://gate.byprice.com/item/item/retailer?retailer={}'.format(retailer)).json()
            if not names:
                raise Exception('Empty Retailer')
            nm_df = pd.DataFrame(names)
            nm_df['item_uuid'] = nm_df['item_uuid'].apply(lambda x: str(x))
            logger.info('Names DF done! , Items:{}'.format(len(nm_df)))
        except Exception as e:
            logger.error(e)
            return False
        # Merge names with prices
        fdf = pd.merge(df,nm_df,on='item_uuid',how='left')
        # Add and drop necessary columns
        fdf['store'] = store_name
        fdf['name'] = fdf['name'].apply(lambda x: str(x).upper())
        fdf['retailer'] = fdf['retailer'].apply(lambda x: str(x).upper())
        fdf.drop('item_uuid', axis=1, inplace=True)
        # Set indexes
        fdf.set_index(['retailer','gtin','name'],inplace=True)
        logger.info('Finished formatting DF')
        # Generate Bytes Buffer
        _buffer = StringIO()
        iocsv = fdf.to_csv(_buffer)
        _buffer.seek(0)
        return _buffer
    
    @staticmethod
    def get_prices_by_retailer(retailer, item_uuid, export=False):
        """ Method that queries a product and returns its prices
            from each store of the requested retailer

            @Params:
            - retailer : (str) Retailer Key
            - item_uuid : (str) Item UUID
            - export : (bool, optional) Exporting flag

            @Returns:
            - (StringIO) File buffer 
            or
            - (dict) Structured response
        """
        logger.debug('Fetching: {} from {}'.format(item_uuid, retailer))
        now = datetime.date.today()
        # Fetch Stores by retailer
        try:
            stores_j = requests\
                .get("http://"+SRV_GEOLOCATION+"/store/retailer?key="+retailer)\
                .json()
            logger.info("Fetched stores!")
        except Exception as e:
            logger.error(e)
            return None
        logger.debug("Found {} stores".format(len(stores_j)))
        # Execute Cassandra Query
        cass_query = """
                    SELECT item_uuid, store_uuid,
                    price, retailer, time
                    FROM price_item 
                    WHERE item_uuid = {}
                    AND time > '{}'
                    """.format(item_uuid,
                        (now-datetime.timedelta(days=1)).__str__())
        logger.debug(cass_query)
        # Query item prices by retailer
        try:
            c_resp = list(g._db.execute(cass_query, timeout=120))
            logger.info('C* Prices fetched!')
            if not c_resp:
                raise Exception('No prices!!')
        except Exception as e:
            logger.error(e)
            return None
        logger.debug("Found {} prices".format(len(c_resp)))
        # Generate DFs and filter
        cass_df = pd.DataFrame(c_resp)
        # Compute previous Stats
        prev_df = cass_df[cass_df['retailer'] == retailer]\
                    .sort_values(by=['time'], ascending=True)\
                    .drop_duplicates(subset=['item_uuid','store_uuid'],
                                    keep='first')
        # Filter prices by retailer and drop duplicates
        filt_df = cass_df[cass_df['retailer'] == retailer]\
                    .sort_values(by=['time'], ascending=False)\
                    .drop_duplicates(subset=['item_uuid','store_uuid'],
                                    keep='first')
        if len(filt_df) == 0:
            logger.error('Empty Retailer')
            return None
        # Geolocation DF
        geo_df = pd.DataFrame(stores_j)
        geo_df.rename(columns={'uuid': 'store_uuid'}, inplace=True)
        # Conversion and merging
        filt_df['store_uuid'] = filt_df['store_uuid'].apply(lambda x : str(x))
        geo_df['store_uuid'] = geo_df['store_uuid'].apply(lambda x : str(x))
        resp_df = pd.merge(filt_df[['store_uuid','price']],
                    geo_df[['name','store_uuid','lat','lng']],
                    how='left', on='store_uuid')
        resp_df['price'] = resp_df['price'].apply(lambda x: round(x, 2))
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
        iocsv = resp_df[['name','price', 'lat', 'lng']].to_csv(_buffer)
        _buffer.seek(0)
        return _buffer

    
    @staticmethod
    def fetch_detail_price(retailer, stores, item, _t, _t1=None):
        """ Method to query detail prices from C*

            @Params:
            - retailer : (str) Retailer key
            - stores : (list) Stores UUIDs
            - item : (str) Item UUID
            - _t : (str) Time formatted string

            @Returns:
            - (list) : Query response with Store_uuid, retailer and price
        """
        stores_str = str(tuple(stores)).replace("'","")
        if len(stores) == 1:
            stores_str = stores_str.replace(',','')        
        cass_query = """
                    SELECT item_uuid, retailer,
                    store_uuid, price, time
                    FROM price_details 
                    WHERE item_uuid = {}
                    AND retailer = '{}'
                    AND store_uuid IN {}
                    AND time > '{}'
                    """.format(item,
                        retailer,
                        stores_str,
                        _t)
        if _t1:
            cass_query += " AND time < '{}'".format(_t1)
        logger.debug(cass_query)
        # Query item price details
        try:
            c_resp = list(g._db.execute(cass_query, timeout=120))
            logger.info('C* Prices fetched!')
            logger.debug(len(c_resp))
            if not c_resp:
                raise Exception('No prices!!')
            return c_resp
        except Exception as e:
            logger.error(e)
            return []
    
    @staticmethod
    def get_pairs_ret_item(fixed, added, date):
        """ Method to compare segments of pairs (retailer-item)

            @Params:
            - fixed : (dict) First Pair of Retailer-Item
            - added : (list) Pairs of Retailer-Item's
            - date : (datetime.datetime) Date of comparison

            @Returns:
            - (dict) JSON response of comparison
        """
        logger.debug("Setting Comparison...")
        # Fetch Retailers and Time
        _rets = [fixed['retailer']] + [x['retailer'] for x in added]
        _time = (date - datetime.timedelta(days=1)).date().__str__()
        # Create Geo DF
        _st_list = []
        for _r in _rets:
            try:
                _stj = requests\
                        .get("http://"+SRV_GEOLOCATION+"/store/retailer?key="+_r)\
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
        # Fetch Fixed Prices DF
        fix_price = Product.fetch_detail_price(fixed['retailer'],
                geo_df[geo_df['retailer'] == 
                        fixed['retailer']]['store_uuid'].tolist(),
                fixed['item_uuid'],
                _time)
        if not fix_price:
            raise errors.AppError("no_price", "No available prices for that combination.")
        # Fetch Added Prices DF
        added_prices = []
        for _a in added:
            added_prices.append(Product\
                .fetch_detail_price(_a['retailer'],
                    geo_df[geo_df['retailer'] == 
                            _a['retailer']]['store_uuid'].tolist(),
                    _a['item_uuid'],
                    _time)
                )
        # Build Fix DF, cast and drop dupls
        fix_df = pd.DataFrame(fix_price)
        fix_df['store_uuid'] = fix_df['store_uuid'].apply(lambda x: str(x))
        fix_df['item_uuid'] = fix_df['item_uuid'].apply(lambda x: str(x))
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
            _tmp['item_uuid'] = _tmp['item_uuid'].apply(lambda x: str(x))
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
        # Construct Response
        _rows = []
        for _j, _jrow in fix_df.iterrows():
            # Iterate over all fixed and retrieve data
            _jth = {
                'fixed': _jrow[['store',
                            'retailer',
                            'item_uuid',
                            'price']].to_dict()
            }
            _segs = []
            for _ith, _deepai in enumerate(added_dfs):
                # Use deep copy for calculations
                _ai = _deepai.copy()
                # Set unfound price like dict
                _jkth = {
                    "store": None,
                    "retailer": added[_ith]['retailer'],
                    "item_uuid": added[_ith]['item_uuid'],
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
        logger.info('Created Rows')
        # Generate Fixed Stores
        _valid_sts = []
        fix_df['name'] = fix_df['store'].apply(lambda x: str(x))
        _valid_sts.append({
            'retailer': fix_df.loc[0]['retailer'],
            'item_uuid': fix_df.loc[0]['item_uuid'],
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
                'retailer': _adf.loc[0]['retailer'],
                'item_uuid': _adf.loc[0]['item_uuid'],
                'stores' : [_r.to_dict() for _l, _r in 
                            _adf[['store_uuid','name']]\
                            .drop_duplicates(subset=['store_uuid'])\
                            .iterrows()]})
        logger.info('Generated Stores!')
        return {
            'date': date,
            'segments': _valid_sts,
            'rows': _rows
        }

    @staticmethod
    def get_pairs_store_item(fixed, added, params):
        """ Method to compare segments of pairs (retailer-item)

            @Params:
            - fixed : (dict) First Pair of Store-Item
            - added : (list) Pairs of Store-Item's
            - params : (dict) Dates and Interval type

            @Returns:
            - (dict) JSON response of comparison
        """
        # Vars
        _resp = {}
        # Obtain distances
        _rets = [fixed['retailer']] + [x['retailer'] for x in added]
        dist_dict = obtain_distances(fixed['store_uuid'],
                                    [x['store_uuid'] for x in added],
                                    _rets)
        # Obtain date groups
        date_groups = grouping_periods(params)
        # Fetch fixed prices
        fix_store = Product.fetch_detail_price(fixed['retailer'],
                    [fixed['store_uuid']],
                    fixed['item_uuid'],
                    date_groups[0][0].date().__str__(),
                    date_groups[-1][-1].date().__str__())
        if not fix_store:
            raise errors.AppError("no_price", "No available prices for that combination.")
        fix_st_df = pd.DataFrame(fix_store)
        fix_st_df['name'] = fixed['name']
        fix_st_df['time_js'] = fix_st_df['time'].apply(date_js())
        ##### 
        # TODO:
        # Add agroupation by Interval
        #####
        # Added Fixed Response
        _resp['fixed'] = {
            'name': fixed['name'],
            "data": [[_t['time_js'], round(_t['price'], 2)]
                        for _i, _t in fix_st_df.iterrows()],
            "max": fix_st_df['price'].max(),
            "min": fix_st_df['price'].min(),
            "avg": fix_st_df['price'].mean(),
            "std": fix_st_df['price'].std()
        }
        logger.info('Fetched fixed values')
        # Fetch added prices
        _resp['segments'] =[]
        for _a in added:
            _tmp_st =  Product.fetch_detail_price(_a['retailer'],
                    [_a['store_uuid']],
                    _a['item_uuid'],
                    date_groups[0][0].date().__str__(),
                    date_groups[-1][-1].date().__str__())
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
                "std": _tmp_df['price'].std(),
                "dist": dist_dict[_a['store_uuid']] 
            }
            # Add to segments
            _resp['segments'].append(_tmp_rsp)
        logger.info('Fetched all segments')
        # Construct response
        return _resp


def obtain_distances(fixed, added, rets):
    """ Method to obtain distances from all stores

        @Params:
        - fixed : (str) Store UUID from Fixed 
        - added : (list) Store UUIDs from Added
        - rets : (str) Retailers 
    """
    _st_list = []
    for _r in rets:
            try:
                _stj = requests\
                        .get("http://"+SRV_GEOLOCATION+"/store/retailer?key="+_r)\
                        .json()
                for _i, _s in enumerate(_stj):
                    _stj[_i].update({'retailer': _r})
            except Exception as e:
                logger.error(e)
                raise errors.AppError("stores_issue",
                            "Could not fetch Stores!")
            _st_list += _stj
    _geo_df = pd.DataFrame(_st_list)
    _geo_df['store_uuid'] = _geo_df['uuid'].apply(lambda x: str(x))
    _f =  _geo_df[_geo_df['store_uuid'] == fixed]
    f_lat, f_lng = _f['lat'].values[0], _f['lng'].values[0]
    _geo_df['fdist'] =  np\
                .arccos(np.sin(np.deg2rad(_geo_df.lat))
                    * np.sin(np.deg2rad(f_lat))
                    + np.cos(np.deg2rad(_geo_df.lat))
                    * np.cos(np.deg2rad(f_lat))
                    * np.cos(np.deg2rad(f_lng)
                                - (np.deg2rad(_geo_df.lng))
                                )
                    ) * 6371
    _added_d = {}
    for _a in added:
        _found = _geo_df[_geo_df['store_uuid'] == _a]
        if _found.empty:
            logger.warning('Store not found: ' + _a)
            continue
        _added_d.update({
            _a: round(_found['fdist'].tolist()[0], 2)
        })
    #logger.debug(_added_d)
    logger.info('Got distances')
    return _added_d


def date_js():
    """ Lambda method to convert datetime object into 
        JS timestamp.

        @Returns
         (lambda) Converter function to JS timestamp
    """
    return lambda djs: int((djs - 
                        datetime.datetime(1970, 1, 1,0,0))\
                        /datetime.timedelta(seconds=1)*1000)


def grouping_periods(params):
    """ Method the receives a dict params with the following keys:
         
        @Params:
        { 
            date_ini: (str) ISO format Date,
            date_fin: (str) ISO format Date,
            interval: (str) day | week | month
        }

        @Returns:
        (list) -  a group of valid date ranges upon this values.
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