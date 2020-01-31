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
import math

# Logger
logger = applogger.get_logger()
# Number of Batches to separate data
NUM_BATCHES = 10

def get_daily_data(_day, rets=[]):
    """ Query for a certain date data 
        from `price_by_date_parted` (PUUID, price, date)

        Params:
        -----
        _day : datetime.date
            Querying date

        Returns:
        -----
        _tfiles : list
            List of TMP file paths to generate stats
    """
    logger.info('Successfully connected to C*')
    if rets:
        stores = get_all_stores(rets)
    else:
        stores = get_all_stores()
    cass_qry = """SELECT product_uuid, price, date, source
        FROM price_by_store WHERE date = %s AND store_uuid = %s
    """
    # Fetch Data from a Day before for todays Aggregates
    _day = int((_day - datetime.timedelta(days=1))\
                .isoformat().replace('-', ''))
    _daily_count, st_list, _tfiles = 0, stores.store_uuid.tolist(), []
    # Generate N tmp files depending on the param
    for _j in range(0, len(stores), NUM_BATCHES):
        daily = []
        for _st in tqdm(st_list[_j:_j+NUM_BATCHES], desc="Store Prices"):
            try:
                q = g._db.query(cass_qry, (_day, UUID(_st)), timeout=200)
                if not q:
                    continue
                daily += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                continue
        if not daily:
            continue
        # Generate TMP csv file  
        _tfile_name = BASE_DIR+'/data/tmp_{}_{}.csv'.format(_day, _j)
        pd.DataFrame(daily).to_csv(_tfile_name)
        _daily_count += len(daily)
        _tfiles.append(_tfile_name)
        logger.info("Created: " + _tfile_name)
    logger.info("Found {} daily prices".format(_daily_count))
    # Return
    return _tfiles


def aggregate_daily(daily_files, _day):
    """ Aggregate data to compute statistics
        and batch load it in to C* table

        Params:
        -----
        daily : list
            Daily prices tmp files 
        day : datetime.date
            Date to set in aggregates
    """
    def _mode(x):
        try:
            return stats.mode(x)[0][0]
        except:
            return np.median(x)
    # File name acumulator
    agg_files = []
    # Compute and reduce number of rows
    for dfile in daily_files:
        daily = pd.read_csv(dfile, low_memory=False)
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
        # Write agg file
        fagg_name = dfile.replace('.csv','_agg.csv')
        aggr_stats.to_csv(fagg_name)
        # Accumulate agg files
        agg_files.append(fagg_name)
    logger.info("Computed all mini batch aggregates!")
    # Read all agg files and compute
    daily_aggs = pd.concat([pd.read_csv(af, low_memory=False) for af in agg_files])
    def compute_overall_stats(z):
        """ Calculate stats of stats
        """
        def _mode(x):
            try:
                return stats.mode(x)[0][0]
            except:
                return np.median(x)
        return pd.Series({
            "max_price": z.max_price.max(),
            "min_price": z.min_price.min(),
            "avg_price": ((z.avg_price * z.datapoints).sum() / z.datapoints.sum()) \
                if z.datapoints.sum() else 0.0,
            "datapoints": z.datapoints.sum(),
            "std_price": math.sqrt((z.std_price * z.std_price * z.datapoints).sum() / z.datapoints.sum()) \
                if z.datapoints.sum() else 0.0,
            "mode_price": _mode(z.mode_price.tolist())
        })
    # Aggregate data to compute max
    all_aggr_stats = daily_aggs\
        .groupby(['product_uuid', 'date'])\
        .apply(compute_overall_stats)\
        .fillna(0.0)\
        .reset_index()
    # Aggregate data, add retailer
    all_aggr_stats = pd.merge(
        all_aggr_stats,
        daily_aggs[['product_uuid', 'source']].drop_duplicates('product_uuid'),
        on="product_uuid",
        how="left"
    )
    # Cast
    all_aggr_stats['datapoints'] = all_aggr_stats['datapoints'].astype(int)
    all_aggr_stats['product_uuid'] = all_aggr_stats['product_uuid'].apply(lambda y: UUID(y))
    # Set the date passed
    all_aggr_stats['date'] = int(_day.strftime('%Y%m%d'))
    # Load each element into C*
    for elem in tqdm(all_aggr_stats.to_dict(orient='records'), desc="Writing.."):
        Price.delete_stats_by_product(elem)
        Price.save_stats_by_product(elem)
        
    # Disply metrics
    logger.info("Stored {} daily prices".format(len(all_aggr_stats)))
    logger.info("Prices had the following distribution:\n {}"
                .format(all_aggr_stats.datapoints.describe()))
    # Delete all temp_fils
    for taf in agg_files:
        os.remove(taf)
    for taf in daily_files:
        os.remove(taf)

@with_context
def daily_stats(_day):
    """ Perform daily stats

        Params:
        -----
        _day : datetime.date
            Querying date
    """
    # Retrieve daily data
    daily_files = get_daily_data(_day)
    # Aggregate data and load into C* table
    if not daily_files:
        logger.warning("No prices available!!")
        return
    aggregate_daily(daily_files, _day)

@with_context
def daily_ret_stats(_day, rets):
    """ Perform daily stats of defined retailers

        Params:
        -----
        _day : datetime.date
            Querying date
        rets: list
            Retailer Keys
    """
    # Retrieve daily data
    daily_files = get_daily_data(_day, rets)
    # Aggregate data and load into C* table
    if not daily_files:
        logger.warning("No prices available!!")
        return
    aggregate_daily(daily_files, _day)

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


def retailers_start(_date, _rets):
    """ Start Method for 
        `flask stats_retailer --date=<DATE> --rets=<RETS>` 
        command
    """ 
    logger.info("Starting Create Stats! Loading in `{}.stats_by_product`"
                .format(CASSANDRA_KEYSPACE))
    try:
        date = datetime.datetime.strptime(_date, '%Y-%m-%d').date()
    except:
        raise Exception("Date format incorrect, needed: (YYYY-MM-DD)")
    logger.info("Running for: {}".format(date))
    rets = [{'key': _r} for _r in _rets.split(',')]
    if not rets and not isinstance(rets, list):
        raise Exception("Wrong rets parameter, needed:  (ret1,ret2) ")
    logger.info("Running for: {}".format(rets))
    # Call to perform stats
    daily_ret_stats(date, rets)
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
