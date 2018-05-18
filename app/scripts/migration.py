import sys
import argparse
import ast
import json
import datetime
import itertools
from uuid import UUID
from multiprocessing import Pool
import pandas as pd
import requests
from pygres import Pygres
from config import *
import app.utils.db as _db
from app.consumer import with_context
from app.models.price import Price
from app.utils import applogger
from app.utils.simple_cassandra import SimpleCassandra

# Logger
#applogger.create_logger()
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
    parser.add_argument('--pg_host', help='Catalogue Postgres Host')
    parser.add_argument('--pg_port', help='Catalogue Postgres Port', type=int)
    parser.add_argument('--pg_db', help='Catalogue Postgres DB')
    parser.add_argument('--pg_user', help='Catalogue Postgres User')
    parser.add_argument('--pg_password', help='Catalogue Postgres Password')
    parser.add_argument('--from', help='Migration Starting date')
    parser.add_argument('--until', help='Migration Ending date')
    parser.add_argument('--date', help='Migration unique date')
    parser.add_argument('--workers', help='Number of Workers', type=int)
    args = dict(parser.parse_args()._get_kwargs())
    # Validation of variables
    # Cassandra
    if not args['cassandra_hosts']:
        args['cassandra_hosts'] = ['0.0.0.0']
    else:
        args['cassandra_hosts'] = args['cassandra_hosts'].split(',')
    if not args['cassandra_port']:
        args['cassandra_port'] = 9042
    if not args['cassandra_keyspace']:
        args['cassandra_keyspace'] = 'prices'
    # Catalogue
    pg_default = {'pg_host': 'localhost', 'pg_port':5432,
        'pg_db':'catalogue', 'pg_user':'postgres',
        'pg_password': 'postgres'}
    for k in pg_default:
        if not args[k]:
            args[k] = pg_default[k]
    def date_from_str(strdate):
        """ Parse Date from Str (YYYY-MM-DD)
        """
        return datetime.datetime\
                .strptime(str(strdate), '%Y-%m-%d')\
                .date()
    args['historic_on'] = True \
        if (args['from'] and args['until'])\
        else False
    # Date
    date_fields = ['date', 'from', 'until']
    for df in date_fields:
        if args[df]:
            try:
                args[df] = date_from_str(args[df])
            except:
                logger.error("Wrong arg: {} must be in format [YYYY-MM-DD]"\
                    .format(df.capitalize()))
                sys.exit()
        else:
            args[df] = datetime.date.today()
    return args


def fetch_all_prods(conf, limit):
    """ Retrieve all products from Catalogue DB

        Params:
        -----
        conf : dict
            PSQL configuration
        limit : int
            Limit number

        Returns:
        -----
        prods : pd.DataFrame
            Product info
    """
    # Connect to Catalogue PSQL DB
    psqlconf = {x.replace('pg', 'SQL').upper() : y \
        for x,y in conf.items() if 'pg' in x}
    catdb = Pygres(psqlconf)
    logger.info("Connected to Catalogue DB!")
    products, page, batch = [],  1, 10000
    while True:
        # Limit statement
        if limit:
            if len(products) > limit:
                break
        # Query to get all items by paginating
        try:
            qry = """ SELECT product_uuid, item_uuid, source,
            gtin FROM product ORDER BY source DESC
            OFFSET {} LIMIT {}
            """.format((page-1)*batch, batch)
            tmp = catdb.query(qry).fetch()
            if len(tmp) == 0:
                raise Exception("Finished retrieving products!")
            products += tmp
            page += 1
        except Exception as e:
            logger.error(e)
            break
        logger.debug("Fetching {} products...".format(len(products)))
    # Close connection
    prods = pd.DataFrame(products).dropna()
    # Filter results from GS1, Nielsen and Mara
    prods[~prods['source'].isin(['mara', 'gs1', 'nielsen'])]
    prods['product_uuid'] = prods['product_uuid'].astype(str)
    prods['item_uuid'] = prods['item_uuid'].astype(str)
    logger.info("Finished retrieving product info: {}".format(len(prods)))
    if prods.empty:
        logger.error("Did not found any products!")
        sys.exit()
    return prods


def fetch_day_prices(_prods, day, limit, conf):
    """ Query data from passed keyspace

        Params:
        -----
        _prods : pd.DataFrame
            Products info
        day : datetime.date
            Query Date 
        limit : int
            Limit of prices to retrieve
        cassconf: dict
            Cassandra Cluster config params
        
        Returns:
        -----
        data : pd.DataFrame
            Prices data
    """
    # Connect to C*
    cdb = SimpleCassandra({
        'CONTACT_POINTS': conf['cassandra_hosts'],
        'KEYSPACE': conf['cassandra_keyspace'],
        'PORT': conf['cassandra_port']
    })
    logger.info("Connected to C* !")
    # Define CQL query
    cql_query = """SELECT * 
        FROM price_item
        WHERE item_uuid = %s
        AND time >= %s
        AND time < %s
    """
    # Item IDs
    item_ids = _prods.dropna().item_uuid.tolist()
    data = []
    # Loop over prod_ids
    for j, _i in enumerate(item_ids):
        _amount = len(data)
        logger.info("Fetching {} in {}, for now {} prices retrieved"\
            .format(_i, day, _amount))
        # Limit statement
        if limit:
            if _amount > limit:
                logger.info("Limit {} has been reached!".format(limit))
                break
        # For each item query prices
        try:
            if not _i:
                continue
            r = cdb.query(cql_query,
                (UUID(_i),
                day, day + datetime.timedelta(days=1)),
                timeout=100)
            data += list(r)
            logger.info("{} % Retrieved"\
                .format(round(100.0*j/len(item_ids), 2)))
        except Exception as e:
            logger.error(e)
            logger.warning("Could not retrieve {}".format(_i, day))
    # Drop connection with C*
    cdb.close()
    logger.info("""Finished retreiving prices and closed connection with C*.""")
    # Generate DFs
    data = pd.DataFrame(data)
    if data.empty:
        return pd.DataFrame()
    data['item_uuid'] = data.item_uuid.astype(str)
    data['store_uuid'] = data.store_uuid.astype(str)
    data.rename(columns={'retailer': 'source'}, inplace=True)
    return pd.merge(data, _prods,
        on=['item_uuid', 'source'], how='left')
    
def format_price(val):
    """ Format price to convert into scraper-like

        Params: 
        -----
        val : dict
            Query values
        
        Returns:
        -----
        formatted : dict
            Formatted price element
    """
    # Reformat
    val.update({
        'retailer': val['source'],
        'currency': 'MXN',
        'date': str(val['time']),
        'location': {
            'store':[
                val['store_uuid']
            ],
            'zip': [val['zip']],
            'city': [val['city']],
            'state': [val['state']],
            'country': 'Mexico',
            "coords" : [
                {
                    "lat" : float(val['lat']),
                    "lng" : float(val['lng'])
                }
            ]
        }
    })
    logger.debug("Formatted {}".format(val['product_uuid']))
    return val


def populate_geoprice_tables(val):
    """ Populate all tables in GeoPrice KS
        
        Params:
        -----
        val : dict
            Price value to insert
    """
    price_val = format_price(val)    
    price = Price(price_val)
    if price.save_all():
        logger.info("Loaded tables for: {}".format(val['product_uuid']))

@with_context
def day_migration(*args):
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
    day, limit, conf, prods = args[0][0], args[0][1], args[0][2], args[0][3]
    logger.info("Retrieving info for migration on ({})".format(day))
    # Retrieve data from Prices KS (prices.price_item)
    data = fetch_day_prices(prods, day, limit, conf)
    if data.empty:
        logger.info("No prices to migrate!")
        return
    logger.info("Found {} prices".format(len(data)))    
    for j, d in data.iterrows():
        # Verify values
        if not d.product_uuid:
            print(d.to_dict())
            continue
        # Populate each table in new KS
        populate_geoprice_tables(d.to_dict())
        logger.info("{}%  Populated"\
            .format(round(100.0 * j / len(data), 2)))
    logger.info("Finished populating tables")


def get_daterange(_from, _until):
    """ Generate a daterange from 
        2 given limits

        Params:
        -----
        _from : datetime.date
            Starting Date
        _until : datetime.date
            Ending Date
        
        Returns:
        -----
        daterange : list
            List of dates within limits
    """
    daterange = [_from]
    # In case limits are not correct, send only from date
    if _until < _from:
        return daterange
    while True:
        _from += datetime.timedelta(days=1)
        if _from > _until:
            break
        daterange.append(_from)
    return daterange


if __name__ == '__main__':
    logger.info("Starting Migration script (Prices KS -> GeoPrice KS)")
    # Parse C* and PSQL args
    cassconf = cassandra_args()
    # Retrieve products from Catalogue
    prods = fetch_all_prods(cassconf, None)
    # Verify if historic is applicable
    if cassconf['historic_on']:
        # Format vars
        _workers = cassconf['workers'] if cassconf['workers'] else 3
        daterange = get_daterange(cassconf['from'], cassconf['until'])
        with Pool(_workers) as pool:
            # Call to run migration over all dates
            pool.map(day_migration,
                itertools.product(daterange, [2000], [cassconf],[prods]))
    else:
        # Format vars
        _day = cassconf['date']
        # Now call to migrate day's data
        day_migration(_day, 2000, cassconf, prods)
        logger.info("Finished executing ({}) migration".format(_day))
