import sys
import argparse
from app.utils import geohash
import calendar
import itertools
from multiprocessing import Pool
import numpy as np
import tqdm
import uuid
from config import *
from app.consumer import with_context
from app.models.price import Price
from app.models.stats import Stats
from app.utils import applogger
from app.utils.simple_cassandra import SimpleCassandra
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import datetime
import time_uuid as tid
from uuid import uuid1, uuid4
import requests
import pandas as pd
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra import ReadTimeout
from pygres import Pygres
import gtin
import os


SPARK_POSTGRESQL_JAR = os.getenv("SPARK_POSTGRESQL_JAR", "/opt/spark/jars/postgresql-42.1.1.jar")
CASSANDRA_SEEDS = os.getenv('CASSANDRA_SEEDS')
SPARK_MASTER_IP = os.getenv('SPARK_MASTER_IP')
APP_NAME = os.getenv('APP_NAME')
CASSANDRA_PRICES_KEYSPACE = os.getenv('CASSANDRA_PRICES_KEYSPACE')


# Logger
# applogger.create_logger()
logger = applogger.get_logger()

conf = SparkConf().setAppName(APP_NAME) \
    .set("spark.cassandra.connection.host", CASSANDRA_SEEDS) \
    .set("spark.executor.memory", "3g") \
    .setMaster(SPARK_MASTER_IP)

sc = SparkContext(conf=conf).getOrCreate()

sql = SQLContext(sc)


def getting_args():
    """ Parse Cassandra related arguments
        to migrate from.

        Returns:
        -----
        conf : dict
            Configuration parameters to migrate from
    """
    parser = argparse \
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
    pg_default = {'pg_host': 'localhost', 'pg_port': 5432,
                  'pg_db': 'catalogue', 'pg_user': 'postgres',
                  'pg_password': 'postgres'}
    for k in pg_default:
        if not args[k]:
            args[k] = pg_default[k]

    def date_from_str(strdate):
        """ Parse Date from Str (YYYY-MM-DD)
        """
        return datetime.datetime \
            .strptime(str(strdate), '%Y-%m-%d') \
            .date()

    args['historic_on'] = True \
        if (args['from'] and args['until']) \
        else False
    # Date
    date_fields = ['date', 'from', 'until']
    for df in date_fields:
        if args[df]:
            try:
                args[df] = date_from_str(args[df])
            except:
                logger.error("Wrong arg: {} must be in format [YYYY-MM-DD]" \
                             .format(df.capitalize()))
                sys.exit()
        else:
            args[df] = datetime.date.today()
    return args


def fetch_all_prods(conf):
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
    psqlconf = {x.replace('pg', 'SQL').upper() : y \
        for x,y in conf.items() if 'pg' in x}
    db = Pygres(psqlconf)
    qry = """ 
    	SELECT product_uuid, item_uuid, source,gtin 
    	FROM product WHERE source NOT IN ('gs1', 'mara', 'nielsen')
    """
    _prods = pd.read_sql(qry, db.conn)
    return _prods


def fetch_day_prices(prods_pandas, ret, day, limit, conf):
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
    # Define CQL query


    try:
        _prods = sql.createDataFrame(prods_pandas)
        pr_ret = sql.read.format("org.apache.spark.sql.cassandra") \
            .options(keyspace=CASSANDRA_PRICES_KEYSPACE, table="price_retailer").load()

        day = int(day.isoformat().replace('-', ''))
        data = pr_ret.filter((pr_ret['retailer'] == ret) & (pr_ret['date'] == day))

    except Exception as e:
        logger.error("Error while fetching {} [{}] prices: {}".format(day, ret, e))
        return False

    # Drop connection with C*
    if len(data.head(1)) == 0:
        logger.warning("Prices weren't found on {} [{}]".format(day, ret))
        return False

    data = data.withColumnRenamed('retailer', 'source')

    return data.join(_prods, ['item_uuid', 'source'], how='left')


def fetch_day_stats(day, conf, df_aux, item_uuids, retailer):
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
    try:
        cdb = SimpleCassandra({
        'CONTACT_POINTS': conf['cassandra_hosts'],
        'KEYSPACE': conf['cassandra_keyspace2'],
        'PORT': conf['cassandra_port']
    })
    except Exception as e:
        logger.warning("Error with simple cassandra {} {}".format(retailer, day))
        logger.error("Error while connecting to cassandra on stats: {}".format(e))
        time.sleep(50)
        return fetch_day_stats(day, conf, df_aux, item_uuids, retailer)

    if isinstance(cdb.session, bool) and cdb.session is False:
        logger.error("Error with {} on {}".format(retailer, day))
        logger.error("Cannot conect to cassandra!!! Trying again in 50 secs")
        time.sleep(50)
        return fetch_day_stats(day, conf, df_aux, item_uuids, retailer)

    timestamp1 = calendar.timegm(day.timetuple())
    day_aux = datetime.datetime.utcfromtimestamp(timestamp1)
    date1 = str(day_aux)
    date2 = str(day_aux + datetime.timedelta(hours=24))
    # Define CQL query
    cql_query = """
    SELECT item_uuid, retailer, toDate(time) as date, avg_price, datapoints, max_price, min_price, mode_price, std_price    
        FROM stats_by_retailer
        WHERE item_uuid in {}
        AND retailer=%s
        AND time >= minTimeuuid(%s)
        AND time < minTimeuuid(%s) 
    """
    if len(item_uuids) > 1:
        cql_query = cql_query.format(str(item_uuids).replace("'", ""))
    else:
        cql_query = cql_query.format(str(item_uuids).replace("'", "").replace(",", ""))
    try:
        r = cdb.query(cql_query, (retailer, date1, date2),
                      timeout=200,
                      consistency=ConsistencyLevel.ONE)
    except Exception as e:
        logger.warning("Could not retrieve {}".format(day))
        logger.error("Error while exectuting query on stats: {}".format(e))
        return pd.DataFrame()

    # Drop connection with C*
    cdb.close()
    # Generate DFs
    data = pd.DataFrame(r)
    del r
    if not data.empty:
        data['item_uuid'] = data['item_uuid'].astype(str)
        data = df_aux.merge(data, on=["item_uuid", "retailer"], how="inner")
        del (data["item_uuid"])
        return data
    else:
        return pd.DataFrame()


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
            'store': [
                val['store_uuid']
            ],
            'zip': [val['zip']],
            'city': [val['city']],
            'state': [val['state']],
            'country': 'Mexico',
            'geohash': val['geohash'],
            "coords": [
                {
                    "lat": float(val['lat']) if val['lat'] else 19.432609,
                    "lng": float(val['lng']) if val['lng'] else -99.133203
                }
            ]
        }
    })
    logger.debug("Formatted {}".format(val['product_uuid']))
    return val


def populate_geoprice_tables(val, day):
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
        logger.debug(e)
        with open('missing_items/{}.csv'.format(str(day)), 'a') as _file:
            _file.write('{},{},{}\n' \
                        .format(price_val['item_uuid'],
                                price_val['retailer'],
                                day))
        return False
    if price.save_all_batch():
        logger.debug("Loaded tables for: {}".format(val['product_uuid']))


@with_context
def day_migration(day, ret, limit, conf, prods):
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
    logger.info("Day migration {} {}...".format(day, ret))
    # Retrieve data from Prices KS (prices.price_item)
    data = fetch_day_prices(prods, ret, day, limit, conf)
    if len(data.head(1)) == 0:
        logger.warning("No prices to migrate in {}-{}!".format(ret, day))
    else:
        data_aux = data.select(["store_uuid", "lat", "lng"]).dropDuplicates(['store_uuid'])
        data_aux = data_aux.na.fill(19.432609, subset=['lat'])
        data_aux = data_aux.na.fill(-99.133203, subset=['lng'])
        geohash_encode = F.udf(geohash.encode, StringType())
        data_aux = data_aux.withColumn('geohash', geohash_encode(data_aux.lat, data_aux.lng))

        columns_to_drop = ['lat', 'lng']
        data_aux = data_aux.drop(*columns_to_drop)

        data = data.join(data_aux, ["store_uuid"], how="left")
        logger.info("Found {} prices".format(len(data)))

        for j, d in data.iterrows():
            populate_geoprice_tables(d.to_dict(), day)
            logger.debug("{}%  Populated" \
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
    logger.info("----------------- Stats on {} -----------------".format(day))
    df_stats_list = []
    for index, df_retailer in df_aux.groupby("retailer"):
        retailer = list(df_retailer.retailer.drop_duplicates())[0]
        logger.info("Getting items from retailer: {} on {}".format(retailer, day))
        df_retailer = df_retailer.reset_index()
        del (df_retailer["index"])
        for aux in range(0, len(df_retailer), 50):
            logger.debug("Getting items from {} to {}".format(aux, aux + 50))
            item_uuids = tuple(item_uuid for item_uuid in df_retailer.iloc[aux: aux + 50].item_uuid if
                               item_uuid and item_uuid != "None")
            logger.debug("Appending items from {} to {}".format(aux, aux + 50))
            df_stats_list.append(fetch_day_stats(day, conf, df_retailer, item_uuids, retailer))
    df_stats = pd.concat(df_stats_list)
    if not df_stats.empty:
        if Stats.save_stats(df_stats.drop_duplicates()):
            logger.info("Finished stats on {}".format(day))
        else:
            logger.warning("Stats on {} coundnt be finished".format(day))
    else:
        logger.warning("No stats on {}".format(day))


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
    args_ = getting_args()
    # Retrieve products from Catalogue, retailers and workers
    prods_pandas = fetch_all_prods(args_)
    retailers = list(set(prods_pandas['source'].tolist()))

    # Verify if historic is applicable
    if args_['historic_on']:
        # Format vars
        daterange = get_daterange(args_['from'], args_['until'])
        logger.info("Executing Historic migration from {} to {}" \
                    .format(args_['from'], args_['until']))
    else:
        # Format vars
        daterange = [args_['date']]
        # Now call to migrate day's data
        logger.info("Executing migration for date {}s" \
                    .format(daterange))

    # Call Multiprocessing for async queries
    logger.info("Calling day_migration ...")
    day_migration(daterange, retailers, None, args_, prods_pandas)

    logger.info("Calling pool stats_migration")
    prods = prods_pandas.drop_duplicates("product_uuid")
    prods["item_uuid"] = prods["item_uuid"].astype(str)
    stats_migration(daterange, args_,prods[["item_uuid", "product_uuid", "source"]].rename(columns={"source": "retailer"}))
    logger.info("Finished executing ({}) migration".format(daterange))
