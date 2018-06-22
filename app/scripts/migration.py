import sys
import argparse
from app.utils import geohash
import datetime
import calendar
import itertools
from multiprocessing import Pool
import pandas as pd
import numpy as np
from pygres import Pygres
from cassandra import ConsistencyLevel
import tqdm
from config import *
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
    if not args.get('cassandra_keyspace2'):
        args['cassandra_keyspace2'] = 'stats'
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
    # Query to get all items 
    try:
        qry = """ SELECT product_uuid, item_uuid, source,
        gtin FROM product WHERE source NOT IN ('gs1', 'mara', 'nielsen')
        """
        if limit:
            qry += ' LIMIT {}'.format(limit)
        prods = pd.read_sql(qry, catdb.conn)
    except Exception as e:
        logger.error(e)
        logger.error("Did not found any products!")
        sys.exit()
    # Close connection
    prods['product_uuid'] = prods['product_uuid'].astype(str)
    prods['item_uuid'] = prods['item_uuid'].astype(str)
    prods.fillna('', inplace=True)
    logger.info("Finished retrieving product info: {}".format(len(prods)))
    if prods.empty:
        logger.error("Did not found any products!")
        sys.exit()
    return prods

def fetch_day_prices(_prods, ret, day, limit, conf):
    """ Query data from passed keyspace

        Params:
        -----
        _prods : pd.DataFrame
            Products info
        ret : str
            Retailer key 
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
    logger.info("Connected to C*!")
    # Define CQL query
    cql_query = """SELECT * 
        FROM price_retailer
        WHERE retailer = %s
        AND date = %s
    """
    # Limit statement
    if limit:
        cql_query += ' LIMIT {}'.format(limit)
    # For each item query prices
    try:
        # Format vars
        day = int(day.isoformat().replace('-',''))
        r = cdb.query(cql_query,
            (ret, day),
            timeout=200,
            consistency=ConsistencyLevel.ONE)
    except Exception as e:
        r = []
        logger.error(e)
        logger.warning("Could not retrieve {}".format(day))
    # Drop connection with C*
    cdb.close()
    logger.info("""Got {} prices prices from {} in {}""".format(len(r), ret, day))
    # Generate DFs
    data = pd.DataFrame(r)
    del r
    if data.empty:
        return pd.DataFrame()
    data['item_uuid'] = data.item_uuid.astype(str)
    data['store_uuid'] = data.store_uuid.astype(str)
    data.rename(columns={'retailer': 'source'}, inplace=True)
    return pd.merge(data, _prods,
        on=['item_uuid', 'source'], how='left')


def fetch_day_stats(day, conf, df_aux):
    """ Query data from passed keyspace

        Params:
        -----
        _prods : pd.DataFrame
            Products info
        ret : str
            Retailer key
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
        'KEYSPACE': conf['cassandra_keyspace2'],
        'PORT': conf['cassandra_port']
    })
    logger.info("Connected to C*!")
    timestamp1 = calendar.timegm(day.timetuple())
    day_aux = datetime.datetime.utcfromtimestamp(timestamp1)
    date1 = str(day_aux)
    date2 = str(day_aux + datetime.timedelta(hours=2))
    #date3 = str(day_aux + datetime.timedelta(hours=12))
    #date4 = str(day_aux + datetime.timedelta(hours=18))
    #date5 = str(day_aux + datetime.timedelta(hours=24))


    # Define CQL query
    cql_query = """
    SELECT item_uuid, retailer, toDate(time), avg_price, datapoints, max_price, min_price, mode_price, std_price    
        FROM stats_by_retailer
        WHERE time >= minTimeuuid(%s)
        AND time < minTimeuuid(%s) 
        ALLOW FILTERING
    """
    try:
        r = cdb.query(cql_query, (date1, date2),
            size=2000,
            timeout=200,
            consistency=ConsistencyLevel.ONE)
    except Exception as e:
        r = []
        logger.error(e)
        logger.warning("Could not retrieve {}".format(day))

    # Drop connection with C*
    cdb.close()
    logger.info("""Got {} prices prices in {}""".format(len(r), day))
    # Generate DFs
    data = pd.DataFrame(r)
    del r
    if data.empty:
        return pd.DataFrame()
    data = df_aux.merge(data, on=["item_uuid", "retailer"], how="inner")
    del(data["item_uuid"])
    del(data["time"])
    data["date"] = int(str(date1).replace('-', ''))
    print(data.head())
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
            'geohash': val['geohash'],
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
        # logger.error("Product invalid: {}".format(e))
        # logger.warning(val)
        # log missing items
        with open('missing_items.csv', 'a') as _file:
            _file.write('{},{}\n'\
                .format(price_val['item_uuid'],
                        price_val['retailer']))
        return False
    #logger.info("[2] Saving All...")
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
    data_aux['geohash'] = [geohash.encode(float(row.lat), float(row.lng)) for index, row in data_aux.iterrows()]
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
    # Retrieve products from Catalogue, retailers and workers
    prods = fetch_all_prods(cassconf, None)
    retailers = list(set(prods['source'].tolist()))
    _workers = cassconf['workers'] if cassconf['workers'] else 3
    # Verify if historic is applicable
    if cassconf['historic_on']:
        # Format vars
        daterange = get_daterange(cassconf['from'], cassconf['until'])
        logger.info("Executing Historic migration from {} to {} with {} workers"\
            .format(cassconf['from'], cassconf['until'], _workers))
    else:
        # Format vars
        daterange = [cassconf['date']]
        # Now call to migrate day's data
        logger.info("Executing Alone migration for {} with {} workers"\
            .format(daterange, _workers))
    # Call Multiprocessing for async queries
    # with Pool(_workers) as pool:
    #     # Call to run migration over all dates
    #     pool.map(day_migration,
    #         itertools.product(daterange, retailers, [None], [cassconf], [prods]))
    with Pool(_workers) as pool:
        pool.map(stats_migration, itertools.product(daterange, [cassconf], [prods[["item_uuid", "product_uuid", "source"]].rename(columns={"source": "retailer"})]))
    logger.info("Finished executing ({}) migration".format(daterange))
