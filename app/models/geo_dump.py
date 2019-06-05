import boto3
from config import *
from app import logger, errors
from io import StringIO
import pandas as pd


# Aux Vars
BUCKET = "geoprice"

class Dump(object):
    """ Class to retrieve and put objects into S3
    """

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
        # Validate Params
        filters += [{'retailer': _ret} for _ret in rets.keys()]
        items = fetch_items(filters)
        stores = fetch_stores([r for r in rets.keys()])
        
        # Create Stores DF 
        stores_df = pd.DataFrame(stores)
        stores_df.rename(columns={'name': 'store', 'uuid' : 'store_uuid'}, inplace=True)
        stores_df['store_uuid'] = stores_df.store_uuid.apply(lambda x: str(x))
        
        # Filter stores by filters <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        if ('store' in [list(_fil.keys())[0] for _fil in filters]):
            _sids = [ _su['store'] for _su in filters if 'store' in _su]
            stores_df = stores_df[stores_df['store_uuid'].isin(_sids)]

        logger.info('Filtering over {} stores'.format(len(stores_df)))
        # Start fetching prices
        prices = Price.fetch_prices(
            [r for r in rets.keys()],
            stores_df.store_uuid.tolist(),
            [i['item_uuid'] for i in items],
            _ini,
            _fin)

        if not prices:
            # Put an empty result
            raise Exception('Could not fetch Prices!')
        
        # Load DF and filter results
        prices_df = pd.DataFrame(prices)
        prices_df.item_uuid = prices_df.item_uuid.apply(lambda x: str(x))
        prices_df.store_uuid = prices_df.store_uuid.apply(lambda x: str(x))
        
        # Remove Duplicates by day, take more recent
        prices_df.sort_values(['time'], ascending=False, inplace=True)
        prices_df.drop_duplicates(
            ['item_uuid', 'store_uuid', 'date'],
            keep='first',
            inplace=True)

        logger.info('Droping duplicates and starting to format..')
        # Merge with item and stores DF
        items_df = pd.DataFrame(items)
        items_df.item_uuid = items_df.item_uuid.apply(lambda x: str(x))
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
        for temp_int, i_df in prices_df.groupby(
            Price.grouping_cols[_inter] + ['item_uuid', 'store_uuid']
        ):
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