import sys
import argparse
import datetime
import calendar
import itertools
import pandas as pd
import numpy as np
from pygres import Pygres
from cassandra import ConsistencyLevel
import tqdm
from config import *
from app.consumer import with_context
from app.models.price import Price
from ByHelpers import applogger
from app.utils.simple_cassandra import SimpleCassandra
from app.utils.helpers import get_all_stores

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
        .ArgumentParser(description='Configures C* params for migration.')
    parser.add_argument('--migration_type, help='Migration type')
    parser.add_argument('--from_cassandra_hosts', help='Source Cassandra Contact points')
    parser.add_argument('--from_cassandra_port', help='Source Cassandra Port', type=int)
    parser.add_argument('--from_cassandra_keyspace', help='Source Cassandra Keyspace')
    parser.add_argument('--from_cassandra_user', help='Source Cassandra User')
    parser.add_argument('--from_cassandra_password', help='Source Cassandra Password')
    parser.add_argument('--to_cassandra_hosts', help='Destination Cassandra Contact points')
    parser.add_argument('--to_cassandra_port', help='Destination Cassandra Port', type=int)
    parser.add_argument('--to_cassandra_keyspace', help='Destination Cassandra Keyspace')
    parser.add_argument('--to_cassandra_user', help='Destination Cassandra User')
    parser.add_argument('--to_cassandra_password', help='Destination Cassandra Password')
    parser.add_argument('--date', help='Migration unique date')
    args = dict(parser.parse_args()._get_kwargs())
    # Validation of variables
    if not args['migration_type']:
        raise Exception("Missing migration type!")
    def date_from_str(strdate):
        """ Parse Date from Str (YYYY-MM-DD)
        """
        return datetime.datetime\
                .strptime(str(strdate), '%Y-%m-%d')\
                .date()
    # Cassandra From
    if args['migration_type'] == 'producer':
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
    # Cassandra To
    elif args['migration_type'] == 'producer':
        if not args['to_cassandra_hosts']:
            args['to_cassandra_hosts'] = ['0.0.0.0']
        else:
            args['to_cassandra_hosts'] = args['to_cassandra_hosts'].split(',')
        if not args['to_cassandra_port']:
            args['to_cassandra_port'] = 9042
        if not args['to_cassandra_keyspace']:
            raise Exception("Missing Destination Keyspace to start migration")
    else:
        raise Exception("Migration Type selected not allowed")
    return args


def fetch_day_prices(day, limit, conf):
    """ Query data from passed keyspace

        Params:
        -----
        day : datetime.date
            Query Date 
        limit : int
            Limit of prices to retrieve
        conf: dict
            Cassandra Cluster config params
        
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
    logger.info("Connected to C*!")
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
    r = []
    for _part in range(1,21):
        try:
            tr = cdb.query(cql_query,
                (day, _part),
                timeout=200,
                consistency=ConsistencyLevel.ONE)
        except Exception as e:
            logger.error(e)
            logger.warning("Could not retrieve {}".format(day, _part))
    # Drop connection with C*
    cdb.close()
    logger.info("""Got {} prices prices in {}""".format(len(r), day))
    # Generate DFs
    data = pd.DataFrame(r)
    del r
    if data.empty:
        return pd.DataFrame()
    data['product_uuid'] = data.product_uuid.astype(str)
    data['store_uuid'] = data.store_uuid.astype(str)
    # TODO
    # Add source to the dataframe
    return data


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
                    "lat" : float(val['lat']) if val['lat'] else 19.432609,
                    "lng" : float(val['lng']) if val['lng'] else -99.133203
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
    logger.debug("Formatted price info..")
    try:
        if type(price.product_uuid) is float and np.isnan(price.product_uuid):
            raise Exception("Product UUID needs to be generated!")
    except Exception as e:
        return False
    logger.info("Saving All...")
    if price.save_all_batch():
        logger.debug("Loaded tables for: {}".format(val['product_uuid']))


@with_context
def day_migration(*args):
    """ Retrieves all data available requested day
        from Prices KS and inserts it into 
        GeoPrice KS.

        Params:
        -----
        day : datetime.date
            Day to execute migration
        ret : str
            Retailer
        limit : int, optional, default=None
            Limit of data to apply migration from
        cassconf : dict
            Dict with Cassandra Configuration to migrate from
        prods : list
            List of Products info
    """
    day, ret, limit, conf, prods = args[0][0], args[0][1], args[0][2], args[0][3], args[0][4]
    logger.debug("Retrieving info for migration on ({}-{})".format(day, ret))
    # Retrieve data from Prices KS (prices.price_item)
    data = fetch_day_prices(prods, ret, day, limit, conf)
    if data.empty:
        logger.debug("No prices to migrate in {}-{}!".format(ret, day))
        return
    data_aux = data[["store_uuid", "lat", "lng"]].drop_duplicates(subset="store_uuid")
    data_aux["lat"] =[lat if lat else 19.432609 for lat in data_aux.lat]
    data_aux["lng"] = [lng if lng else -99.133203 for lng in data_aux.lng]
    del(data_aux["lat"])
    del (data_aux["lng"])
    data = data.merge(data_aux, on="store_uuid", how="left")
    logger.info("Found {} prices".format(len(data)))

    for j, d in tqdm.tqdm(data.iterrows()):
        # Populate each table in new KS
        #logger.info("[1] Populating...")
        populate_geoprice_tables(d.to_dict())
        logger.debug("{}%  Populated"\
            .format(round(100.0 * j / len(data), 2)))
    logger.info("Finished populating tables")


@with_context
def stats_migration(*args):
    """ Retrieves all data available requested day
        from Prices KS and inserts it into
        GeoPrice KS.

        Params:
        -----
        day : datetime.date
            Day to execute migration
        ret : str
            Retailer
        limit : int, optional, default=None
            Limit of data to apply migration from
        cassconf : dict
            Dict with Cassandra Configuration to migrate from
        prods : list
            List of Products info
    """
    day, conf, df_aux = args[0][0], args[0][1], args[0][2]
    logger.debug("Retrieving stats on ({})".format(day))
    # Retrieve data from Prices KS (prices.price_item)
    fetch_day_stats(day, conf, df_aux)


if __name__ == '__main__':
    logger.info("Starting Migration script (Prices KS -> GeoPrice KS)")
    # Parse C* and PSQL args
    cassconf = cassandra_args()
    # Retrieve products from Geolocation
    stores = get_all_stores()
    # Now call to migrate day's data
    logger.info("Executing Alone migration for {}"\
        .format(cassconf['date']))
    # Apply migration
    
    logger.info("Finished executing ({}) migration".format(daterange))
