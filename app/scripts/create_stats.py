"""Script that computes stats and save them to `stats_by_product` tabl.

  References for cassandra driver batch statements
  (Group batch loads to be between 1-100kB and to have the 
  same partition key )
  [https://docs.datastax.com/en/drivers/python/3.2/_modules/cassandra/query.html#BatchStatement]
  [https://stackoverflow.com/questions/22920678/cassandra-batch-insert-in-python]
"""
import argparse
import datetime
from uuid import UUID
import pandas as pd
import numpy as np
from scipy import stats
from flask import g
from config import *
from app.utils import applogger
from app.consumer import with_context
from app.models.price import Price

# Logger
applogger.create_logger('stats-'+APP_NAME)
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
        from `price_by_date` (PUUID, price, date)

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
    # Query data
    cass_qry = """SELECT product_uuid, price, date
    FROM price_by_date WHERE date = %s 
    """
    _day = int(_day.isoformat().replace('-', ''))
    daily = g._db.query(cass_qry, (_day,), timeout=20)
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
    # Load each element into C*
    from tqdm import tqdm
    for elem in tqdm(aggr_stats.to_dict(orient='records')):
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
    aggregate_daily(daily)

def start():
    logger.info("Starting Create Stats! Loading in `{}.stats_by_product`"
                .format(CASSANDRA_KEYSPACE))
    date = datetime.date.today()
    logger.debug(date)
    # Call to perform stats
    daily_stats(date)
    logger.info("Finished creating daily stats ({})".format(date))
