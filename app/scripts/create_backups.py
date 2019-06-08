"""
    Script that fetches info from C*
    and save them to S3 Bucket in AWS for Athena Usage.

    ** The date to which this is run, is to retrieve data
    from the day before.
"""
import argparse
import datetime
import sys
from uuid import UUID
import pandas as pd
import numpy as np
from scipy import stats
from flask import g
from config import *
from ByHelpers import applogger
from app.consumer import with_context
from app.utils.helpers import get_all_stores
from uuid import UUID
from tqdm import tqdm
import boto3
import random

# Logger
logger = applogger.get_logger()
# Bucket 
BUCKET_DIR= '{}price_retailer'.format(
    '' if ENV == 'PROD' else 'dev/'
)

def backup_daily_data(_day):
    """ Query for a certain date data 
        from `price_by_store_date` (PUUID, price, date)

        Params:
        -----
        _day : datetime.date
            Querying date

        Returns:
        -----
        _tfiles : list
            List of TMP file paths to generate stats
    """
    logger.info('Successfully connected to C*')
    stores = get_all_stores()
    logger.info("Found {} stores to back".format(len(stores)))
    cass_qry = """SELECT *
        FROM price_by_store 
        WHERE date = %s 
        AND store_uuid = %s
    """
    # Fetch Data from a Day before for todays Aggregates
    _day = int((_day - datetime.timedelta(days=1)).isoformat().replace('-', ''))
    _daily_count, st_list = 0, stores.store_uuid.tolist()
    # Fetch prices by store
    for _part, _st in enumerate(st_list):
        try:
            q = g._db.query(cass_qry, (_day, UUID(_st)), timeout=200)
            if not q:
                continue
        except Exception as e:
            logger.error("Cassandra Connection error: " + str(e))
            continue
        # Format response
        dtr = pd.DataFrame(q).drop(['lat', 'lng'], axis=1, errors='ignore')
        dtr['product_uuid'] = dtr.product_uuid.astype(str)
        dtr['store_uuid'] = dtr.store_uuid.astype(str)
        dtr = pd.merge(dtr, 
            stores[['store_uuid',
                    'zip', 'city','state', 'lat','lng']], 
            on='store_uuid', how='left')
        dtr['source'] = dtr['source'].fillna('')
        # Count amount of prices
        _daily_count += len(dtr)
        # Send to S3
        try:
            send_prices_parquet(dtr, _part)
        except Exception as e:
            logger.warning("Issues storing in S3!")
            logger.error(e)
            continue
    logger.info("Found {} daily prices".format(_daily_count))

def send_prices_parquet(data, _part):
    """ Generate local parquet and send it to AWS S3

        Params:
        -----
        data : pd.DataFrame
            All needed data to generate parquet
        _part : int
            Partition number
    """
    # Format data
    data['date'] = data.date.astype(dtype=np.int32)
    def compute_discount(x):
        _disc = 100.0 * (x['price_original'] - x['price']) / x['price_original'] if x['price_original'] > 0.0 else 0.0
        return _disc
    if 'discount' not in data.columns:
        data['discount'] = data.apply(compute_discount, 1)
    data['discount'] = data.discount.astype(dtype=np.float32)
    data['lat'] = data.lat.astype(dtype=np.float32)
    data['lng'] = data.lng.astype(dtype=np.float32)
    data['price'] = data.price.astype(dtype=np.float32)
    data['price_original'] = data.price_original.astype(dtype=np.float32)
    data['promo'] = data['promo'].fillna('')
    data['retailer'] = data['source']
    data['item_uuid'] = ''
    data = data.drop(['source', 'url', 'currency'], axis=1, errors='ignore')
    # Iterate by source
    for gk, gdf in data.groupby(['date', 'retailer']):
        parq_fname = "data/{}_{}_{}_{}.parquet"\
            .format(*gk, _part, random.randint(1,9999))
        # write local parquet file
        gdf.to_parquet(parq_fname)
        # Send to S3
        write_pandas_parquet_to_s3(parq_fname, 'byprice-prices', gk[1], gk[0], _part)
        

def write_pandas_parquet_to_s3(parq_file, bucket_name, retailer, _date, _part):
    """ Send to AWS S3 given a parquet local file

        Params:
        -----
        parq_file: str
            Name of the Parquet file
        bucket_name: str
            Name of S3 Bucket
        retailer: str
            Retailer key
        _date: int
            Date in (YYYYMMDD) format
        _part: int
            Partition number
    """
    # init S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    key_name = "{}/retailer={}/date={}/geop_{}_{}_{}.parquet".format(BUCKET_DIR,retailer, _date, retailer, _date, _part)
    with open(parq_file, 'rb') as f:
       object_data = f.read()
       s3.put_object(Body=object_data, Bucket=bucket_name, Key=key_name)
       logger.info("Correctly uploaded {} {} - {} ".format(retailer, _date, _part))
    os.remove(parq_file)


@with_context
def daily_backups(_day):
    """ Perform daily stats

        Params:
        -----
        _day : datetime.date
            Querying date
    """
    # Retrieve daily data
    backup_daily_data(_day)

def start():
    """ Start Method for `flask script --name=<script>` command
    """ 
    logger.info("Starting Create Dumps! Loading in `byprice-prices/{}`"
                .format(BUCKET_DIR))
    date = datetime.date.today() 
    logger.info("Running for: {}".format(date))
    # Call to perform stats
    daily_backups(date)
    logger.info("Finished creating daily backups ({})".format(date))


if __name__ == '__main__':
    """ Main Method for to run as:
        `python -m app.scripts.create_backups YYYY-MM-DD`
    """ 
    logger.info("Starting Create Dumps! Loading in `byprice-prices/{}`"
                .format(BUCKET_DIR))
    if len(sys.argv) < 2:
        raise Exception("Missing date to perform stats (YYYY-MM-DD)!")
    date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    logger.info("Running for: {}".format(date))
    # Call to perform stats
    daily_backups(date)
    logger.info("Finished creating daily backups ({})".format(date))
