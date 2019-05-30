import datetime
from flask import g
from app import errors
from ByHelpers import applogger
import pandas as pd
from pandas import DataFrame
from app.models.history_alarm import Alarm
from app.utils.helpers import tuplize
import numpy as np
import uuid
from config import *
import requests

geo_stores_url = SRV_PROTOCOL + "://" + SRV_GEOLOCATION + '/store/retailer?key=%s'
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

class Alert(Alarm):
    """
        Class perform Query methods on Cassandra items to verify aler,
        reuses various methods from the Alarm Class

        TODO: Verify all methods are working correctly with Cassandra v3
    """
    def __init__(self):
        pass

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

    @staticmethod
    def get_price_compare(params):
        """ Get Price compare given Alert rules, stores and retailers

            Params:
            -----
            dict
                Dict containing {`alerts`, `retailers`, `stores`} keys as params
        """
        # Aux assignation
        items, _stores, _items, _retailers = [], [], [], []
        size_items, size_stores = 100, 100

        # Alerts DF
        alerts_df = pd.DataFrame(params['alerts'])

        # Divide in chunks
        for alert in params['alerts']:
            items = items + [alert['item_uuid']] + [alert['item_uuid_compare']]

        _items = list(set(items))
        _stores = params['stores']
        _retailers = params['retailers']

        chunks_items = [ _items[i:i+size_items] for i in range(0, len(_items), size_items)]
        chunks_stores = [ _stores[i:i+size_stores] for i in range(0, len(_stores), size_stores)]        
        # Define Chunk dates
        chunks_dates = [int(params['date'].replace('-',''))]
        aux_date = datetime.datetime.strptime(params['date'],'%Y-%m-%d').date()
        for _d in range((datetime.date.today() - aux_date).days):
            chunks_dates.append(
                int((aux_date + datetime.timedelta(days=1)).strftime('%Y%m%d'))
            )
        # Fetch Products from Item UUIDs
        prods, rows = [], []
        # Loop stores
        for ch_items in chunks_items:
            # Loop items
            _temp_prods = []
            for _chi in ch_items:
                _temp_prods += g._catalogue.get_products_by_item(_chi, 
                    cols=['product_uuid', 'name', 'item_uuid', 'gtin']) 
                prods += _temp_prods
            ch_prods = [ _tp['product_uuid'] for _tp in _temp_prods]
            for ch_stores in chunks_stores:
                # Get prices 
                rows += g._db.query("""SELECT product_uuid, 
                                source as retailer, 
                                store_uuid, price, promo
                            FROM price_by_product_store
                            WHERE product_uuid IN {}
                            AND store_uuid IN {}
                            AND date IN {}
                            """.format(
                                tuplize(ch_prods, is_uuid=True),
                                tuplize(ch_stores, is_uuid=True),
                                tuplize(chunks_dates)
                            ))
        prices_df = pd.DataFrame(rows).drop_duplicates()
        if len(prices_df) == 0:
            return []
        # Add Item and Product UUIDs
        prods_df = pd.DataFrame(prods)
        prods_by_uuid = {p['product_uuid'] : p for p in prods}
        prices_df['product_uuid'] = prices_df['product_uuid'].apply(lambda x: str(x))
        prices_df['store_uuid'] = prices_df['store_uuid'].apply(lambda x: str(x))
        print('Prices Stats')
        print(prices_df.count())
        prices_df['item_uuid'] = prices_df['product_uuid'].apply(lambda x: )
        print('Prices Stats with UUID')
        print(prices_df.count())
        print(prods_df.head())
        input('BREAKPOINT')
        results_df = alerts_df.merge(prices_df, on='item_uuid', how='left')
        prices_df = prices_df.rename(index=str, 
                                    columns={
                                        "item_uuid": "item_uuid_compare", 
                                        "retailer": "retailer_compare",
                                        "store_uuid": "store_uuid_compare", 
                                        "price": "price_compare", 
                                        "promo": "promo_compare"
                                    })
        results_df = results_df.merge(prices_df, 
                                    on='item_uuid_compare', 
                                    how='left')

        results_df['alert'] = results_df.apply(
            lambda x: Alert.breaks_rules(x),
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
        items_info = requests.get(SRV_PROTOCOL + "://" + SRV_CATALOGUE + '/product/by/iuuid?cols=item_uuid,gtin&keys={}'.format(\
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
