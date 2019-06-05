"""
    Script that computes stats and save them to `stats_by_product` tabl.

  References for cassandra driver batch statements
  (Group batch loads to be between 1-100kB and to have the 
  same partition key )
  [https://docs.datastax.com/en/drivers/python/3.2/_modules/cassandra/query.html#BatchStatement]
  [https://stackoverflow.com/questions/22920678/cassandra-batch-insert-in-python]
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
from app.models.price import Price
from app.utils.helpers import get_all_stores
from uuid import UUID
from tqdm import tqdm

# Logger
logger = applogger.get_logger()


def stats_args():
    """ Parse Create Stats Script args

        Returns:
        -----
        conf : dict
            Configuration parameters to migrate from
    """
    parser = argparse\
        .ArgumentParser(description='Aggregates Daily data in C* ({}.stats_by_product)'
                        .format(CASSANDRA_KEYSPACE))
    parser.add_argument('--date', help='Migration date')
    args = dict(parser.parse_args()._get_kwargs())
    # Validation of variables
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


def get_daily_data(_day):
    """ Query for a certain date data 
        from `price_by_date_parted` (PUUID, price, date)

        Params:
        -----
        _day : datetime.date
            Querying date

        Returns:
        -----
        daily : pd.DataFrame
            Table of daily prices data 
    """
    logger.info('Successfully connected to C*')
    daily = []
    stores = get_all_stores()
    cass_qry = """SELECT product_uuid, price, date, source
        FROM price_by_store WHERE date = %s AND store_uuid = %s
    """
    _day = int(_day.isoformat().replace('-', ''))
    for _st in tqdm(stores.store_uuid.tolist(), desc="Store Prices"):
        try:
            q = g._db.query(cass_qry, (_day, UUID(_st)), timeout=200)
            if not q:
                continue
            daily += list(q)
        except Exception as e:
            logger.error("Cassandra Connection error: " + str(e))
            continue
    # Format Data
    if not daily:
        return pd.DataFrame()
    daily = pd.DataFrame(daily)
    #daily['product_uuid'] = daily['product_uuid'].astype(str)
    logger.info("Found {} daily prices".format(len(daily)))
    # Return
    return daily


def aggregate_daily(daily):
    """ Aggregate data to compute statistics
        and batch load it in to C* table

        Params:
        -----
        daily : pd.DataFrame
            Daily prices data
        max_batch : int
            Max batch size to load into C*
    """
    def _mode(x):
        try:
            return stats.mode(x)[0][0]
        except:
            return np.median(x)
    # Aggregate data to compute mean, std, max, min, etc.
    aggr_stats = daily.groupby(['product_uuid', 'date']).price\
        .agg([('max_price', 'max'),
              ('avg_price', 'mean'),
              ('min_price', 'min'),
              ('datapoints', 'count'),
              ('std_price', 'std'),
              ('mode_price', lambda x: _mode(x))
              ])
    aggr_stats.fillna(0.0, inplace=True)
    aggr_stats.reset_index(inplace=True)
    # Add retailer
    aggr_stats = pd.merge(
        aggr_stats,
        daily[['product_uuid', 'source']].drop_duplicates('product_uuid'),
        on='product_uuid',
        how='left'
    )
    # Load each element into C*
    for elem in tqdm(aggr_stats.to_dict(orient='records'), desc="Writing.."):
        Price.save_stats_by_product(elem)
    # Disply metrics
    logger.info("Stored {} daily prices".format(len(aggr_stats)))
    logger.info("Prices had the following distribution:\n{}"
                .format(aggr_stats.datapoints.describe()))


@with_context
def daily_stats(_day):
    """ Perform daily stats

        Params:
        -----
        _day : datetime.date
            Querying date
        max_batch : int
            Max batch size to load into C*
    """
    # Retrieve daily data
    daily = get_daily_data(_day)
    # Aggregate data and load into C* table
    if daily.empty:
        logger.warning("No prices available!!")
        return
    aggregate_daily(daily)

def start():
    """ Start Method for `flask script --name=<script>` command
    """ 
    logger.info("Starting Create Stats! Loading in `{}.stats_by_product`"
                .format(CASSANDRA_KEYSPACE))
    date = datetime.date.today() 
    logger.debug(date)
    # Call to perform stats
    daily_stats(date)
    logger.info("Finished creating daily stats ({})".format(date))


if __name__ == '__main__':
    """ Main Method for to run as:
        `python -m app.scripts.create_stats YYYY-MM-DD`
    """ 
    logger.info("Starting Create Stats! Loading in `{}.stats_by_product`"
                .format(CASSANDRA_KEYSPACE))
    if len(sys.argv) < 2:
        raise Exception("Missing date to perform stats (YYYY-MM-DD)!")
    date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    logger.info("Running for: {}".format(date))
    # Call to perform stats
    daily_stats(date)
    logger.info("Finished creating daily stats ({})".format(date))
