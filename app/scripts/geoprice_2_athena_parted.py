"""  Task to execute and generate parquet files from Cassandra tables
    and insert them into S3 for Athena Usage.

    Execution help:
    python -m app.scripts.geoprice_2_athena --help
"""
import sys
import argparse
import datetime
import calendar
import itertools
import pandas as pd
import numpy as np
from cassandra import ConsistencyLevel
import tqdm
from config import *
from app.consumer import with_context
import numpy as np
from app.models.price import Price
from ByHelpers import applogger
from app.utils.simple_cassandra import SimpleCassandra
from app.utils.helpers import get_all_stores
import boto3

# Logger
#applogger.create_logger()
logger = applogger.get_logger()
LIMIT = 100
BUCKET_DIR= 'dev/price_retailer'

def cassandra_args():
    """ Parse Cassandra related arguments
        to migrate from.

        Returns:
        -----
        conf : dict
            Configuration parameters to migrate from
    """
    parser = argparse\
        .ArgumentParser(description='Configures C* params for migration.')
    parser.add_argument('--from_cassandra_hosts', help='Source Cassandra Contact points')
    parser.add_argument('--from_cassandra_port', help='Source Cassandra Port', type=int)
    parser.add_argument('--from_cassandra_keyspace', help='Source Cassandra Keyspace')
    parser.add_argument('--from_cassandra_user', help='Source Cassandra User')
    parser.add_argument('--from_cassandra_password', help='Source Cassandra Password')
    parser.add_argument('--date', help='Migration date  (YYYY-MM-DD)')
    args = dict(parser.parse_args()._get_kwargs())
    # Validation of variables
    def date_from_str(strdate):
        """ Parse Date from Str (YYYY-MM-DD)
        """
        return datetime.datetime\
                .strptime(str(strdate), '%Y-%m-%d')\
                .date()
    # Cassandra From
    if not args['from_cassandra_hosts']:
        args['from_cassandra_hosts'] = ['0.0.0.0']
    else:
        args['from_cassandra_hosts'] = args['from_cassandra_hosts'].split(',')
    if not args['from_cassandra_port']:
        args['from_cassandra_port'] = 9042
    if not args['from_cassandra_keyspace']:
        raise Exception("Missing Source Keyspace to start migration")
    if not args['date']:
        raise Exception("Missing Date to apply migration!")
    args['date'] = date_from_str(args['date'])
    return args


def fetch_day_prices(day, _part, limit, conf, stores):
    """ Query data from passed keyspace

        Params:
        -----
        day : datetime.date
            Query Date 
        _part: int
            Partition 
        limit : int
            Limit of prices to retrieve
        conf: dict
            Cassandra Cluster config params
        stores: pd.DataFrame
            Stores DF
        
        Returns:
        -----
        data : pd.DataFrame
            Prices data
    """
    # Connect to C*
    cdb = SimpleCassandra({
        'CONTACT_POINTS': conf['from_cassandra_hosts'],
        'KEYSPACE': conf['from_cassandra_keyspace'],
        'PORT': conf['from_cassandra_port'],
        'USER': conf['from_cassandra_user'],
        'PASSWORD': conf['from_cassandra_password']
    })
    logger.info("Connected to C* - {}!".format(cdb.session.keyspace))
    # Define CQL query
    cql_query = """SELECT * 
        FROM price_by_date_parted
        WHERE date = %s
        AND part = %s
    """
    # Limit statement
    if limit:
        cql_query += ' LIMIT {}'.format(limit)
    # Format vars
    day = int(day.isoformat().replace('-',''))
    dtr = pd.DataFrame()
    try:
        tr = cdb.query(cql_query,
            (day, _part),
            timeout=200,
            consistency=ConsistencyLevel.ONE)
        if tr: 
            # Generate DFs
            dtr = pd.DataFrame(tr)
            dtr['product_uuid'] = dtr.product_uuid.astype(str)
            dtr['store_uuid'] = dtr.store_uuid.astype(str)
            dtr = pd.merge(dtr, 
                stores[['store_uuid', 'source', 
                        'zip', 'city','state', 'lat','lng']], 
                on='store_uuid', how='left')
            dtr['source'] = dtr['source'].fillna('')
        logger.info("""Got {} prices in {} - {}""".format(len(dtr), day, _part))
    except Exception as e:
        logger.error(e)
        logger.warning("Could not retrieve {} - {}".format(day, _part))
    # Drop connection with C*
    cdb.close()
    return dtr


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
    data = data.drop(['part', 'source', 'url', 'currency'], axis=1)
    # Iterate by source
    for gk, gdf in data.groupby(['date', 'retailer']):
        parq_fname = "data/{}_{}_{}.parquet".format(*gk, _part)
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


def day_migration(day, limit, conf, stores):
    """ Retrieves all data available requested day
        from Prices KS and inserts it into 
        GeoPrice KS.

        Params:
        -----
        day : datetime.date
            Day to execute migration
        limit : int, optional, default=None
            Limit of data to apply migration from
        conf : dict
            Dict with Cassandra Configuration to migrate from
        stores : pd.DataFrame
            DF of stores
    """
    logger.debug("Retrieving info for migration on ({})".format(day))
    # Retrieve data from AWS Geoprice KS (geoprice.price_by_date_parted)
    for _part in range(1, 21):
        data = fetch_day_prices(day, _part, limit, conf, stores)
        if data.empty:
            logger.debug("No prices to migrate in {} - {}!".format(day, _part))
            continue
        send_prices_parquet(data, _part)
    logger.info("Finished populating tables")


if __name__ == '__main__':
    logger.info("Starting Migration script [Parted table] (AWS Geoprice KS -> GCP GeoPrice KS)")
    # Parse C* and PSQL args
    cassconf = cassandra_args()
    # Retrieve products from Geolocation
    stores = get_all_stores()
    # Now call to migrate day's data
    logger.info("Executing migration for {}".format(cassconf['date']))
    # Apply migration
    day_migration(cassconf['date'], LIMIT, cassconf, stores)
    logger.info("Finished executing ({}) migration".format(cassconf['date']))
