import boto3
from flask import g
from config import *
from app import logger, errors
from io import StringIO
import pandas as pd
import datetime
from app.models.price import Price

# Aux Vars
BUCKET = "geoprice"

class Dump(object):
    """ Class to retrieve and put objects into S3
    """

    grouping_cols = {
        'day': ['year', 'month', 'day'],
        'month': ['year', 'month'],
        'week': ['year', 'week']
    }

    def get_recent_from_s3(fname):
        """ Get most recent existance of the file name
            given in S3 for hte given environment

            Params:
            -----
            fname: str
                Filename to check
        """
        s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        _key = ENV.lower() + "/" + fname
        # Fetch file
        try:
            remote_r = s3.get_object(
                Bucket=BUCKET,
                Key=_key
            )
            fbuff = remote_r['Body']
            df = pd.read_csv(fbuff).drop('Unnamed: 0', axis=1, errors='ignore')
        except Exception:
            logger.warning("Not found file!")
            raise errors.AppError("no_file_found", "Not found dump file in S3")
        return df

    @staticmethod
    def df_to_s3(df, source):
        """ Save dataframe directly to s3, for quick access
            to the most recent information
            File Path:   s3://<BUCKET>/<ENV>/<SOURCE>_stats_aggregate.csv

            Params:
            ----
            df: pd.DataFrame
                Data DataFrame
            source: str
                Source key
        """
        now = datetime.datetime.utcnow()
        # Key for bucket
        filename = ENV.lower() \
            + "/" + source + "_stats_aggregate.csv"
        bucket = BUCKET
        try:
            # Generate buffer
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            # Put object
            s3.put_object(
                Bucket=bucket,
                Key=filename,
                Body=csv_buffer.getvalue()
            )
            logger.info("Correctly stored file in S3!")
            return True
        except Exception as e:
            logger.warning("Could not save file to s3!")
            logger.error(e)
            return False
        

    def df_to_s3_historic(df, source):
        """ Save dataframe directly to s3 to save history
            File Path:   s3://<BUCKET>/<ENV>/<YEAR>/<MONTH>/<DAY>/<SOURCE>_stats_aggregate.csv

            Params:
            ----
            df: pd.DataFrame
                Data DataFrame
            source: str
                Source key
        """
        now = datetime.datetime.utcnow()
        # Key for bucket
        filename = ENV.lower() \
            + "/" + now.strftime("%Y")\
            + "/" + now.strftime("%m") \
            + "/" + now.strftime("%d") \
            + "/" + source + "_stats_aggregate.csv"
        bucket = BUCKET
        try:
            # Generate buffer
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            # Put object
            s3.put_object(
                Bucket=bucket,
                Key=filename,
                Body=csv_buffer.getvalue()
            )
            logger.info("Correctly stored historic file in S3!")
            return True
        except Exception as e:
            logger.warning("Could not save historic file to s3!")
            logger.error(e)
            return False


    @staticmethod
    def decompose_filters(filters, rets, date_start, date_end):
        """ Given filters with time intervals, stores, retailers and items,
            return decomposed elements for querying ing C*

            Params:
            -----
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
        # Return elements
        return dates, retailers, retailers_by_key, \
            stores, stores_by_uuid, items, \
            items_by_uuid, prods, prods_by_uuid
        
    @staticmethod
    def get_compare_by_store(filters, rets, _ini, _fin, _inter):
        """ Query cassandra and grop response by items vs stores

            Params:
            -----
            - filters: (list) 
                Item Filters [item_uuids, store_uuids, retailers]
            - rets: (list) List of posible retailers to query depending
                    on the client configurations
            - date_start: (str) 
                ISO format Start Date
            - date_end: (str) 
                ISO format End Date 
            - interval: (str) 
                Time interval  
        """
        # Validate Params and decompose filters
        dates, retailers, \
            retailers_by_key, \
            stores, stores_by_uuid, \
            items, items_by_uuid, \
            prods, prods_by_uuid = Dump.decompose_filters(
                                filters, 
                                rets, 
                                _ini,
                                _fin
                            )
        # Start fetching prices
        prices = Price.query_by_product_store(
            stores=list(stores_by_uuid.keys()),
            products=list(prods_by_uuid.keys()),
            dates=dates
        )
        if not prices:
            logger.warning("No prices found")
            # Put an empty result
            raise Exception('No se encontraron precios, intenta nuevamente')
        
        # Load DF and filter results
        prices_df = pd.DataFrame(prices)
        prices_df.product_uuid = prices_df.product_uuid.apply(lambda x: str(x))
        prices_df['item_uuid'] = prices_df.product_uuid.apply(lambda z: prods_by_uuid[z]['item_uuid'])
        prices_df.store_uuid = prices_df.store_uuid.apply(lambda x: str(x))
        
        # Remove Duplicates by day, take more recent
        prices_df.sort_values(['time'], ascending=False, inplace=True)
        prices_df\
            .drop_duplicates(
                    ['item_uuid', 'store_uuid', 'date'],
                    keep='first',
                    inplace=True
        )
        logger.info('Droping duplicates and starting to format..')
        # Merge with item and stores DF
        items_df = pd.DataFrame(items)
        items_df.item_uuid = items_df.item_uuid.apply(lambda x: str(x))
        stores_df = pd.DataFrame(stores).rename(columns={
            "uuid": "store_uuid",
            "name": "store"
        })
        prices_df = pd.merge(
            prices_df,
            items_df,
            on='item_uuid',
            how='left')
        prices_df = pd.merge(
            prices_df,
            stores_df[['store_uuid','store']],
            on='store_uuid',
            how='left')
        #prices_df.retailer_name = prices_df.retailer.apply(lambda x: rets[x])
        prices_df['retailer_name'] = prices_df['retailer'].apply(lambda x: rets[x])
        
        # Add Grouping dates
        prices_df['day'] = prices_df['time'].apply(lambda x: x.day)
        prices_df['month'] = prices_df['time'].apply(lambda x: x.month)
        prices_df['year'] = prices_df['time'].apply(lambda x: x.year)
        prices_df['week'] = prices_df['time'].apply(lambda x: x.isocalendar()[1])

        # Verify if **grouping_periods** is needed
        logger.info('Finished format, writing result..')
        table_result = {}
        # Group by retailer
        table_result = []
        # Group by intervals, item and store
        for temp_int, i_df in prices_df.groupby(Dump.grouping_cols[_inter] + ['item_uuid', 'store_uuid']):
            table_result.append({
                'gtin': i_df.gtin.tolist()[0],
                'item_uuid' : i_df.item_uuid.tolist()[0],
                'store_uuid' : i_df.store_uuid.tolist()[0],
                'retailer' : i_df.retailer.tolist()[0],
                'retailer_name' : i_df.retailer_name.tolist()[0],
                'name': i_df.name.tolist()[0],
                'promo': i_df.promo.tolist()[0],
                'store': "{} - {}".format(
                    i_df.retailer_name.tolist()[0],
                    i_df.store.tolist()[0]
                ),
                'price': round(i_df.price.mean(), 2),
                'date': i_df.time.tolist()[0].date().isoformat()
            })
        return table_result