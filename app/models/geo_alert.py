import datetime
from flask import g
from app import errors
from ByHelpers import applogger
import pandas as pd
from pandas import DataFrame
import numpy as np
import uuid
from config import *
import requests

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'
logger = applogger.get_logger()

def get_items_str(items_list):
    """Method to convert List of dicts into tuple-like string"""
    item_ids = tuple(items_list)
    if len(item_ids) == 1:
        return str(item_ids).replace("'",'').replace(',','')
    return str(item_ids).replace("'",'')

def get_yday(today):
    """ Method that gets fay prior date string from a day in format YYYY-MM-DD """
    return str(datetime.date(*[int(x) for x in today.split('-')]) +
            datetime.timedelta(days=-1))

class Alert(object):
    """
        Class perform Query methods on Cassandra items to verify alert

        TODO: Verify all methods are working correctly with Cassandra v3
    """
    def __init__(self):
        pass

    @staticmethod
    def prices_vs_prior(params):
        """ Static method to compute prices after queries params.
            Args:
                + params: (dict) - {
                                        'uuids' : (list),
                                        'retailers': (list),
                                        'today': (str) "YYYY-MM-DD"
                                    }
        """
        logger.debug('Fetching alert prices...')
        try:
            # Today prices
            prx_today = g._db.execute("""
                SELECT item_uuid, retailer, store_uuid, price, time
                FROM price_item
                WHERE item_uuid IN {}
                AND time > '{}'
                """.format(get_items_str(params['uuids']),
                            params['today']))
            # Yesterday prices
            prx_yday = g._db.execute("""
                select item_uuid, retailer, store_uuid,
                price, time from price_item
                where item_uuid IN {}
                and time > '{}' AND time < '{}'
                """.format(get_items_str(params['uuids']),
                            get_yday(params['today']),
                            params['today']))
            logger.debug('Prices fetched from Cassandra...')
        except Exception as e:
            logger.error(e)
            raise errors.ApiError('db_issues', "Could not retrieve data from DB.")

        # Generate DataFrames to proceed filtering
        today_df = pd.DataFrame(list(prx_today))
        yday_df = pd.DataFrame(list(prx_yday))
        try:
            # Filter elements with not valid retailers
            today_df['valid'] = today_df['retailer'].apply(lambda x : True if x in params['retailers'] else False)
            yday_df['valid'] = yday_df['retailer'].apply(lambda x : True if x in params['retailers'] else False)
            today_df = today_df[today_df['valid']]
            yday_df = yday_df[yday_df['valid']]
        except:
            logger.warning('No retailers to filter')
            return {"today":[],"prevday":[]}
        # Convert datetime to date
        today_df['date'] = today_df['time'].apply(lambda x : x.date().__str__())
        yday_df['date'] = yday_df['time'].apply(lambda x : x.date().__str__())
        # Drop Store UUID column
        today_df.drop(['store_uuid','valid', 'time'], inplace=True, axis=1)
        yday_df.drop(['store_uuid','valid','time'], inplace=True, axis=1)
        # Order  and Drop duplicates
        today_df.sort_values(by=['item_uuid','retailer', 'price'], inplace=True)
        yday_df.sort_values(by=['item_uuid','retailer', 'price'], inplace=True)
        today_df.drop_duplicates(subset=['item_uuid', 'retailer'], keep='first', inplace=True)
        yday_df.drop_duplicates(subset=['item_uuid', 'retailer'], keep='first', inplace=True)
        logger.debug('Dataframes filtered!')
        # Convert in dict
        return {
                'today': today_df.to_dict(orient='records'),
                'prevday': yday_df.to_dict(orient='records')
            }


    @staticmethod
    def get_geolocated(params):
        """
            @Params:
                - stores
                - items
                - retailers
                - date
        """
        items = []
        stores = []
        size_items = 100
        size_stores = 100

        # Divide in chunks
        _stores = [ s[0] for s in params['stores'] ]
        _items = [ s[0] for s in params['items'] ]
        _retailers = params['retailers']

        chunks_items = [ _items[i:i+size_items] for i in range(0, len(_items), size_items)]
        chunks_stores = [ _stores[i:i+size_stores] for i in range(0, len(_stores), size_stores)]
        rows = []

        # Loop stores
        for ch_items in chunks_items:
            # Loop items
            for ch_stores in chunks_stores:
                # Get the
                rows += g._db.execute("""
                    select item_uuid, retailer, store_uuid, price, time, promo
                    from price_details
                    where item_uuid in ({})
                    and store_uuid in ({})
                    and retailer in  ({})
                    and time >= '{}'
                    and time < '{}'
                """.format(
                    """, """.join(ch_items),
                    """, """.join(ch_stores),
                    """, """.join( [""" '{}' """.format(r) for r in _retailers] ),
                    params['date'],
                    datetime.datetime.strptime(
                        params['date'],
                    '%Y-%m-%d') + datetime.timedelta(days=1)
                ))

        # reference price
        ref = { it[0] : it[1] for it in params['items'] }

        def get_ref(x):
            """ Get reference price with
                error handling in case
                of KeyError
            """
            try:
                return ref[str(x)]
            except:
                return None

        def breaks_rule(row,v,vt):
            """ Check if the row breaks the price rule
                if so returns true or false
            """
            price_ref = float(row['reference'])
            price = float(row['price'])

            if vt == 'percent':
                upper_boundry = price_ref+price_ref*(float(v)/100)
                lower_boundry = price_ref-price_ref*(float(v)/100)
            else:
                upper_boundry = price_ref+float(v)
                lower_boundry = price_ref-float(v)

            # If its beyond the thresholds
            if price > upper_boundry or price_ref < lower_boundry:
                return True
            else:
                return False

        if not rows:
            return []
        df = DataFrame(rows)
        df['reference'] = df['item_uuid'].apply(lambda x: get_ref(x))
        df.dropna(subset=['reference'],inplace=True)

        # Change types of item_uuid, store_uuid, date
        df['activate'] = df.apply(
            lambda x: breaks_rule(
                x,
                params['variation'],
                params['variation_type']
            ),
            axis=1
        )

        return df[df['activate'] == True].to_dict(orient='records')


    @staticmethod
    def get_price_compare(params):

        print('inside get_price_compare')

        def breaks_rules(row):
            """ Check if the row breaks the price rule
                if so returns true or false
            """
            price_ref = float(row['price'])
            price = float(row['price_compare'])

            if row['type'] == 'percent':
                upper_boundry = price_ref+price_ref*(float(row['variation'])/100)
                lower_boundry = price_ref-price_ref*(float(row['variation'])/100)
            else:
                upper_boundry = price_ref+float(row['variation'])
                lower_boundry = price_ref-float(row['variation'])

            # If its beyond the thresholds
            if price > upper_boundry or price < lower_boundry:
                return True
            else:
                return False

        items = []
        _stores = []
        _items = []
        _retailers = []
        size_items = 100
        size_stores = 100

        alerts_df = pd.DataFrame(params['alerts'])

        # Divide in chunks
        for alert in params['alerts']:
            items = items + [alert['item_uuid']] + [alert['item_uuid_compare']]

        _items = list(set(items))
        _stores = params['stores']
        _retailers = params['retailers']

        chunks_items = [ _items[i:i+size_items] for i in range(0, len(_items), size_items)]
        chunks_stores = [ _stores[i:i+size_stores] for i in range(0, len(_stores), size_stores)]
        rows = []

        # Loop stores
        for ch_items in chunks_items:
            # Loop items
            for ch_stores in chunks_stores:
                '''print("""
                    select item_uuid, retailer, store_uuid, price, promo
                    from price_details
                    where item_uuid in ({})
                    and store_uuid in ({})
                    and retailer in  ({})
                    and time > '{}'
                """.format(
                    """, """.join(ch_items),
                    """, """.join(ch_stores),
                    """, """.join([""" '{}' """.format(r) for r in _retailers]),
                    params['date']
                ))'''
                # Get the
                rows += g._db.execute("""
                    select item_uuid, retailer, store_uuid, price, promo
                    from price_details
                    where item_uuid in ({})
                    and store_uuid in ({})
                    and retailer in  ({})
                    and time > '{}'
                """.format(
                    """, """.join(ch_items),
                    """, """.join(ch_stores),
                    """, """.join( [""" '{}' """.format(r) for r in _retailers] ),
                    params['date']
                ))


        prices_df = pd.DataFrame(rows).drop_duplicates()

        if len(prices_df) == 0:
            return []

        prices_df['item_uuid'] = prices_df['item_uuid'].apply(lambda x: str(x))
        prices_df['store_uuid'] = prices_df['store_uuid'].apply(lambda x: str(x))

        results_df = alerts_df.merge(prices_df, on='item_uuid', how='left')
        prices_df = prices_df.rename(index=str, columns={"item_uuid": "item_uuid_compare", "retailer": "retailer_compare", \
                                            "store_uuid": "store_uuid_compare", "price": "price_compare", "promo": "promo_compare"})
        results_df = results_df.merge(prices_df, on='item_uuid_compare', how='left')

        results_df['alert'] = results_df.apply(
            lambda x: breaks_rules(x),
            axis=1
        )

        results_df = results_df.loc[results_df['alert'] == True]

        if len(results_df) == 0:
            return []

        results_df['diff'] = results_df['price_compare'] - results_df['price']

        retailers = results_df['retailer'].drop_duplicates().tolist() + results_df['retailer_compare'].drop_duplicates().tolist()

        # get stores for all the retailers in alerts
        stores = []
        for retailer in retailers:
            stores += requests.get(geo_stores_url%(retailer)).json()

        stores_df = pd.DataFrame(stores)
        # select only that columns
        stores_df = stores_df[['name','city', 'state', 'zip', 'uuid']].rename(index=str, columns={"uuid": "store_uuid", "name": "store_name"})
        stores_df['city'] = stores_df['city'].str.strip()
        # merge with store_uuid and store_uuid_compare
        results_df = results_df.merge(stores_df, on='store_uuid', how='left')
        stores_df = stores_df.rename(index=str, columns={"store_uuid": "store_uuid_compare", "store_name": "store_name_compare",\
                                                         "city": "city_compare", "state": "state_compare", "zip": "zip_compare"})
        results_df = results_df.merge(stores_df, on='store_uuid_compare', how='left')

        # get items info
        items = results_df['item_uuid'].drop_duplicates().tolist() + results_df['item_uuid_compare'].drop_duplicates().tolist()
        items_info = requests.get('http://gate.byprice.com/bpcatalogue/product/by/iuuid?cols=item_uuid,gtin&keys={}'.format(\
                            ','.join(items))).json()['products']

        items_df = pd.DataFrame(items_info)
        items_df.drop(['product_uuid','product_id'], inplace=True, axis=1)
        items_df = items_df.drop_duplicates(subset='source', keep="first")
        items_df = items_df.rename(index=str, columns={"name": "item_name", "source": "retailer"})
        # merge item info
        results_df = results_df.merge(items_df, on=['item_uuid', "retailer"], how='left')
        items_df = items_df.rename(index=str, columns={"item_name": "item_name_compare", "item_uuid": "item_uuid_compare", \
                                                        "retailer": "retailer_compare", "gtin": "gtin_compare"})
        results_df = results_df.merge(items_df, on=['item_uuid_compare', 'retailer_compare'], how='left')

        results_df = results_df.dropna()

        return results_df.to_dict(orient='records')
