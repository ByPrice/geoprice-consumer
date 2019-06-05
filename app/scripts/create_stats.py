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
# Number of Batches to separate data
NUM_BATCHES = 100

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
        _tfiles : list
            List of TMP file paths to generate stats
    """
    logger.info('Successfully connected to C*')
    stores = get_all_stores()
    cass_qry = """SELECT product_uuid, price, date, source
        FROM price_by_store WHERE date = %s AND store_uuid = %s
    """
    _day = int(_day.isoformat().replace('-', ''))
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


def aggregate_daily(daily_files):
    """ Aggregate data to compute statistics
        and batch load it in to C* table

        Params:
        -----
        daily : list
            Daily prices tmp files 
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
            "avg_price": (z.avg_price * z.datapoints).mean(),
            "datapoints": z.datapoints.sum(),
            "std_price": (z.std_price * z.std_price * z.datapoints).mean(),
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
    all_aggr_stats['datapoints'] = all_aggr_stats['datapoints'].astype(int)
    # Load each element into C*
    for elem in tqdm(all_aggr_stats.to_dict(orient='records'), desc="Writing.."):
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
        max_batch : int
            Max batch size to load into C*
    """
    # Retrieve daily data
    daily_files = get_daily_data(_day)
    # Aggregate data and load into C* table
    if not daily_files:
        logger.warning("No prices available!!")
        return
    aggregate_daily(daily_files)

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
