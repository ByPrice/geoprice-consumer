import sys
import os
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
from app.models.price import Price
from app.utils.catalogue import Catalogue
from ByHelpers import applogger
from app.utils.simple_cassandra import SimpleCassandra
from app.utils.helpers import get_all_stores
import json
import boto3

# Logger
#applogger.create_logger()
logger = applogger.get_logger()
LIMIT = 100
BUCKET_DIR= 'dev/price_retailer'

# Connect to C*
cdb = None

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


def fetch_day_prices(day, _ret, limit, conf, stores, prods_by_uuid):
    """ Query data from passed keyspace

        Params:
        -----
        day : datetime.date
            Query Date 
        _ret: str
            Retailer key 
        limit : int
            Limit of prices to retrieve
        conf: dict
            Cassandra Cluster config params
        stores: pd.DataFrame
            Stores DF
        prods_by_uuid: dict
            Mapping from item_uuid to product_uuid

        Returns:
        -----
        data : pd.DataFrame
            Prices data
    """

    logger.info("Connected to C*!")
    # Define CQL query
    cql_query = """SELECT date, time, item_uuid,
            store_uuid, price, price_original, promo
        FROM price_retailer
        WHERE date = %s
        AND retailer = %s
    """
    # Limit statement
    if limit:
        cql_query += ' LIMIT {}'.format(limit)
    # Format vars
    day = int(day.isoformat().replace('-',''))
    dtr = pd.DataFrame()
    try:
        tr = cdb.query(cql_query,
            (day, _ret),
            timeout=200,
            consistency=ConsistencyLevel.ONE)
        if tr: 
            # Generate DFs
            dtr = pd.DataFrame(tr)
            dtr['item_uuid'] = dtr.item_uuid.astype(str)
            # Add product UUID
            def find_puuid(z):
                if z in prods_by_uuid:
                    return prods_by_uuid[z]
                with open(BASE_DIR+'/data/.missing_in_catalogue.txt', 'a') as mf:
                    mf.write('{{"missing_item_uuid": "{}",  "in_retailer": "{}"}}\n'.format(z, _ret))
                return None
            dtr['product_uuid'] = dtr.item_uuid.apply(find_puuid)
            # Remove empty products
            dtr = dtr[dtr.product_uuid.notnull()]
            dtr['product_uuid'] = dtr['product_uuid'].astype(str)
            dtr['store_uuid'] = dtr.store_uuid.astype(str)
            dtr['url'] = ''
            dtr = pd.merge(dtr, 
                stores[['store_uuid', 'source', 'zip', 
                    'city','state', 'lat','lng']], 
                on='store_uuid', how='left')
            dtr['source'] = dtr['source'].fillna('')
            logger.info("""Got {} prices in {} - {}""".format(len(dtr), day, _ret))
            
        logger.info("""Got {} prices cleaned in {} - {}""".format(len(dtr), day, _ret))
    except Exception as e:
        logger.error(e)
        logger.warning("Could not retrieve {} - {}".format(day, _ret))
    return dtr


def send_prices_parquet(data):
    """ Generate local parquet and send it to AWS S3

        Params:
        -----
        data : pd.DataFrame
            All needed data to generate parquet
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
    data = data.drop(['source', 'url', 'currency'], axis=1)
    # Iterate by source
    for gk, gdf in data.groupby(['date', 'retailer']):
        parq_fname = "data/{}_{}_all.parquet".format(*gk)
        # write local parquet file
        gdf.to_parquet(parq_fname)
        # Send to S3
        write_pandas_parquet_to_s3(parq_fname, 'byprice-prices', gk[1], gk[0])
        

def write_pandas_parquet_to_s3(parq_file, bucket_name, retailer, _date):
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
    """
    # init S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    key_name = "{}/retailer={}/date={}/geop_{}_{}_all.parquet".format(BUCKET_DIR,retailer, _date, retailer, _date)
    with open(parq_file, 'rb') as f:
       object_data = f.read()
       s3.put_object(Body=object_data, Bucket=bucket_name, Key=key_name)
       logger.info("Correctly uploaded {} {} ".format(retailer, _date))
    os.remove(parq_file)
@with_context 
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
    # Retrieve data from AWS Prices KS (prices.price_by_retailer)
    _rets = ["kelloggs_walmart", "kelloggs_chedraui", "farmapronto"]
    for _ret in _rets:
        _prods = get_prods_by_ret(_ret)
        data = fetch_day_prices(day, _ret, limit, conf, stores, _prods)
        if data.empty:
            logger.debug("No prices to migrate in {} - {}!".format(day, _ret))
            continue
        print(data.head())
        sys.exit()
        send_prices_parquet(data)
    logger.info("Finished populating tables")


def get_prods_by_ret(_ret):
    """ Try to read cache from local file otherwise 
        create csv tmp file from web service
        
        Params:
        -----
        _ret: str
            Retailer key
        
        Returns:
        ----
        dict
            Mapping from item UUID to product UUID
    """
    if '.{}.csv'  not in os.listdir(BASE_DIR + '/data'):
        _cat = Catalogue(SRV_CATALOGUE, SRV_PROTOCOL)
        tmpj = _cat.get_by_source(_ret, ['item_uuid', 'product_uuid'])
        # Add manually found
        with open(BASE_DIR + '/data/.missing_found.json', 'r') as mfj:
            mtmp = json.loads(mfj.read())
            mtmp = [{'item_uuid': _x['missing_item_uuid'], 
                    'product_uuid': _x['assigned_product_uuid']} \
                for _x in mtmp if _x['in_retailer'] == _ret]
            tmpj += mtmp
        # Write tmp file
        pd.DataFrame(tmpj).to_csv(BASE_DIR + '/data/.{}.csv'.format(_ret))
    # Read, format and return
    tmpdf = pd.read_csv(BASE_DIR + '/data/.{}.csv'.format(_ret))
    return {
        t['item_uuid'] : t['product_uuid'] \
            for t in tmpdf.to_dict(orient='records') if t['item_uuid']
    }


if __name__ == '__main__':
    logger.info("Starting Migration script (AWS Geoprice KS -> GCP GeoPrice KS)")
    # Parse C* and PSQL args
    cassconf = cassandra_args()
    # Connect to C*
    cdb =  SimpleCassandra({
        'CONTACT_POINTS': cassconf['from_cassandra_hosts'],
        'KEYSPACE': cassconf['from_cassandra_keyspace'],
        'PORT': cassconf['from_cassandra_port']
    })
    # Retrieve products from Geolocation
    stores = get_all_stores()
    # Now call to migrate day's data
    logger.info("Executing migration for {}".format(cassconf['date']))
    # Apply migration
    day_migration(cassconf['date'], LIMIT, cassconf, stores)
    # Drop connection with C*
    cdb.close()
    logger.info("Finished executing ({}) migration".format(cassconf['date']))