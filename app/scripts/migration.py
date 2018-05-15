import sys
import argparse
import ast
import json
import datetime
from uuid import UUID
import pandas as pd
import requests
from config import *
import app.utils.db as _db
from app.utils import applogger
from app.utils.simple_cassandra import SimpleCassandra

# Logger
applogger.create_logger('migration-'+APP_NAME)
logger = applogger.get_logger()


def cassandra_args():
    """ Parse Cassandra related arguments
        to migrate from.

        Returns:
        -----
        conf : dict
            Configuration parameters to migrate from
    """
    parser = argparse\
        .ArgumentParser(description='Configures C* params to read from.')
    parser.add_argument('--cassandra_hosts', help='Cassandra Contact points')
    parser.add_argument('--cassandra_port', help='Cassandra Port', type=int)
    parser.add_argument('--cassandra_keyspace', help='Cassandra Keyspace')
    parser.add_argument('--date', help='Migration date')
    args = dict(parser.parse_args()._get_kwargs())
    # Validation of variables
    if not args['cassandra_hosts']:
        args['cassandra_hosts'] = ['0.0.0.0']
    else:
        args['cassandra_hosts'] = args['cassandra_hosts'].split(',')
    if not args['cassandra_port']:
        args['cassandra_port'] = 9042
    if not args['cassandra_keyspace']:
        args['cassandra_keyspace'] = 'prices'
    if args['date']:
        try:
            args['date'] = datetime\
                .datetime\
                .strptime(str(args['date']), '%Y-%m-%d')\
                .date()
        except:
            logger.error("Wrong arg: Date must be in format [YYYY-MM-DD]")
            sys.exit()
    else:
        args['date'] = datetime.date.today()
    return args


def fetch_day_prices(day, limit, cassconf, batch=100):
    """ Query data from passed keyspace

        Params:
        -----
        day : datetime.date
            Query Date 
        limit : int
            Limit of prices to retrieve
        cassconf: dict
            Cassandra Cluster config params
        batch : int
            Batch size of queries
        
        Returns:
        -----
        data : pd.DataFrame
            Prices data
    """
    # Connect to C*
    cdb = SimpleCassandra({'CONTACT_POINTS': cassconf['cassandra_hosts'],
        'KEYSPACE': cassconf['cassandra_keyspace'],
        'PORT': cassconf['cassandra_port']})
    logger.info("Connected to C* !")
    # Define CQL query
    cql_query = """SELECT * 
        FROM price_item
        WHERE item_uuid = %s
        AND time >= %s
        AND time < %s"""
    data, _prods, page = [], [], 1
    # Loop over batches
    while True:
        _amount = len(data)
        logger.info("{} prices retrieved".format(_amount))
        # Limit statement
        if limit:
            if _amount > limit:
                break
        # Query for item_uuids
        try:
            _url = SRV_CATALOGUE+'/product/by/puuid?keys=&p={}&ipp={}&cols={}'\
                                .format(page, batch, 'item_uuid')
            items = requests.get(_url)
            logger.debug('Status code: {}'.format(items.status_code))
            if items.status_code != 200:
                raise Exception("Catalogue Srv having not working")
            items = items.json()['products']
            if len(items) == 0:
                raise Exception('Finished retrieving items!')
            _prods += items
        except Exception as e:
            logger.error(e)
            break
        # For each item query prices
        for _i in items:
            try:
                if not _i['item_uuid']:
                    continue
                r = cdb.query(cql_query,
                    (UUID(_i['item_uuid']),
                    day, day + datetime.timedelta(days=1)),
                    timeout=50)
                data += list(r)
            except Exception as e:
                logger.error(e)
    # Drop connection with C*
    cdb.close()
    # Generate DFs
    data = pd.DataFrame(data)
    if data.empty:
        return pd.DataFrame()
    data.item_uuid = data.item_uuid.astype(str)
    data.store_uuid = data.store_uuid.astype(str)
    data.rename(columns={'retailer': 'source'}, inplace=True)
    data.to_csv("price_data.csv")
    _prods = pd.DataFrame(_prods)
    _prods.item_uuid = _prods.item_uuid.astype(str)
    _prods.to_csv("prod_data.csv")
    return pd.merge(data, _prods,
        on=['item_uuid', 'source'], how='left')
    

def populate_geoprice_tables(val):
    pass


def day_migration(day, limit=None, cassconf={}):
    """ Retrieves all data available requested day
        from Prices KS and inserts it into 
        GeoPrice KS.

        Params:
        -----
        day : datetime.date
            Day to execute migration
        limit : int, optional, default=None
            Limit of data to apply migration from
        cassconf : dict
            Dict with Cassandra Configuration to migrate from
    """
    logger.info("Retrieving info for migration on ({})".format(day))
    # Retrieve data from Prices KS (prices.price_item)
    data = fetch_day_prices(day, limit, cassconf)
    if data.empty:
        logger.info("No prices to migrate!")
        return
    logger.info("Found {} prices".format(len(data)))
    logger.debug(data.head(5))
    for j, d in data.iterrows():
        # Populate each table in new KS
        populate_geoprice_tables(d)
        logger.info("{}%  Populated"\
            .format(round(100.0 * j / len(data), 2)))
    logger.info("Finished populating tables")

if __name__ == '__main__':
    logger.info("Starting Migration! (Prices (KS) -> GeoPrice (KS)")
    # Parse C* args
    cassconf = cassandra_args()
    logger.info(cassconf)
    # Format vars
    _day = cassconf['date']
    del cassconf['date']
    # Now call to migrate day's data
    day_migration(_day, limit=100, cassconf=cassconf)
    logger.info("Finished executing ({}) migration".format(_day))

