from app import g
from app import errors
from ByHelpers import applogger
from config import *
import app as geoprice
import json
import datetime
import pandas as pd
from collections import OrderedDict
from functools import wraps
from app.utils.helpers import tuplize
from tqdm import tqdm
from app.models.geo_dump import Dump

# Logger
logger = applogger.get_logger()

# Aux vars
SOURCES = [] if not TASK_ARG_CREATE_DUMPS else TASK_ARG_CREATE_DUMPS.split(",")

def with_context(original_function):
    """ Flask Context decorator for inside execution
    """
    @wraps(original_function)
    def new_function(*args,**kwargs):
        # Declare Flask App context
        ctx = geoprice.app.app_context()
        # Init Ctx Stack 
        ctx.push()
        logger.debug('AppContext is been created')
        # Connect db
        geoprice.build_context(
            services=['geolocation', 'catalogue']
        )
        logger.debug('Connected to redis')
        original_function(*args,**kwargs)
        # Teardown context
        ctx.pop()
        return True
    return new_function

def get_prices(product_uuids, retailer_keys):
    """ Get the prices per products,
        aggregate the prices per retailer 

        Params:
        -----
        product_uuids: list 
            List of `product_uuid` 
        retailer_keys: iterator 
            Retailer keys
        
        Returns:
        -----
        dict
            Results from Stats computed
        
        `e.g.`
        >>> {
            "walmart": {
                "max": 13.0,
                "avg": 12.4,
                "min": 11.0
            }
        }
    """
    result = {}
    # Set dates for today and yesterday to compute stats
    _dates =  tuple([
        int(datetime.datetime.utcnow().strftime("%Y%m%d")),
        int(
            (datetime.datetime.utcnow() \
                - datetime.timedelta(days=1)).strftime("%Y%m%d")
        )
    ])
    qry = """SELECT product_uuid, price, 
                source as retailer, time
             FROM price_by_product_date 
            WHERE product_uuid IN {} 
            AND date IN {}
        """.format(
            tuplize(product_uuids, is_uuid=True), 
            _dates
        )
    rows = g._db.query(
                    qry,
                    size=2000,
                    timeout=40
    )
    df = pd.DataFrame(rows)
    # Get the stats per retailer
    for ret in retailer_keys:
        # For empty DF
        if df.empty:
            p_max, p_min, p_avg = "-", "-","-"
            result[ret] = {
                "max" : p_max,
                "min" : p_min,
                "avg" : p_avg
            }
            continue
        # Get stats of the retailer
        ret_df = df[ df['retailer'] == ret ]
        if not ret_df.empty:
            p_avg = ret_df['price'].mean()
            p_max = ret_df['price'].max()
            p_min = ret_df['price'].min()
        else:
            p_max, p_min, p_avg = "-", "-","-"
        result[ret] = {
            "max" : p_max,
            "min" : p_min,
            "avg" : p_avg
        }
    return result



def get_stats(products, retailers):
    """ Compute stats from given items

        Params:
        ----
        products: list  
            List of products with attributes (name, item_uuid, product_uuid)
        retailers: dict
            Dict of retailer keys with retailer values
    """
    result = []
    i = 0
    # Products DF
    prods_df = pd.DataFrame(products)
    # Get stats for every item and every retailer
    for iuuid, gdf in prods_df.groupby('item_uuid'):
        try:
            # Get stats per retailer
            _prices = get_prices(
                gdf.product_uuid.drop_duplicates().tolist(),
                retailers.keys()
            )
            # Aggregate
            _item = OrderedDict()
            _item['gtin'] = gdf['gtin'].fillna('').sort_values(ascending=False).tolist()[0]
            _item['name'] = gdf['name'].fillna('').sort_values(ascending=False).tolist()[0]
            for r_key, r_name in retailers.items():
                _item[r_key+'_max'] = _prices[r_key]['max']
                _item[r_key+'_min'] = _prices[r_key]['min']
                _item[r_key+'_avg'] = _prices[r_key]['avg']
            result.append(_item)
        except Exception as e:
            logger.error(e)
            logger.warning("Issues adding result for in item: " + str(iuuid))
            continue
    return result 







@with_context
def start():
    """ Build the dataframe
        - Get retailers
        - Get general catalogue
        - Get catalogue of all retailers
        - Get prices for every item
    """
    logger.info("Starting dump script, saving file to: " + BUCKET + "/" + ENV.lower())
    logger.info("Getting retailers")
    retailers = { r['key'] : r['name'] for r in g._geolocation.get_retailers()}
    logger.info(len(retailers))
    
    # Loop the sources of data we want as base for the table
    for src in SOURCES:
        logger.info("Getting total items for {}".format(src))
        total_items = g._catalogue.get_by_source(src, ['item_uuid', 'gtin'])
        logger.info(len(total_items))
        total_products = g._catalogue.get_product_details(
            [_ti['item_uuid'] for _ti in total_items if _ti['item_uuid']],
            cols=['item_uuid', 'gtin'],
            loop_size=50
        )
        logger.info(len(total_products))
        # Build stats
        logger.info("Building stats for " + src)
        stats = get_stats(total_products, retailers)
        # Build dataframe and save to s3
        dframe = pd.DataFrame(stats)
        Dump.df_to_s3(dframe, src)
        Dump.df_to_s3_historic(dframe, src)
    logger.info("Finished creating dumps!")
