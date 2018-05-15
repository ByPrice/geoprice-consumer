import sys
import argparse
import ast
import json
import datetime
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


def fetch_day_prices(day, limit, cassconf):
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
    data = fetch_day_prices(day, limit, {})

if __name__ == '__main__':
    logger.info("Starting Migration! (Prices (KS) -> GeoPrice (KS)")
    # Parse C* args
    cassconf = cassandra_args()
    logger.info(cassconf)
    # Format vars
    _day = cassconf['date']
    del cassconf['date']
    # Now call to migrate day's data
    day_migration(_day, limit=None, cassconf=cassconf)
    logger.info("Finished executing ({}) migration".format(_day))

