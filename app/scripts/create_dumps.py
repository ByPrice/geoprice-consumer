from app import g
from app import errors
from ByHelpers import applogger
from config import *
import requests
from collections import defaultdict
import json
import datetime
import pandas as pd
from pandas import DataFrame, Series
from collections import OrderedDict
import boto3
from io import StringIO

dd=lambda:defaultdict(dd)
logger = applogger.get_logger()


DATA_DIR = BASE_DIR+"/data/"
ROWS_SAVE = 5000
now = datetime.datetime.utcnow()
then = now - datetime.timedelta(hours=32)
SOURCES = [] if not config.TASK_ARG_CREATE_DUMPS else config.TASK_ARG_CREATE_DUMPS.split(",")
BUCKET='geoprice'

template = {
    "gtin" : [],
    "name" : [],
    "retailer" : [],
    "price_avg" : [],
    "price_max" : [],
    "price_min" : []
}

def get_retailers():
    # Get all retailers
    resp = requests.get(SRV_PROTOCOL + "://" + SRV_GEOLOCATION + "/retailer/all")
    print(resp.text)
    retailers = resp.json()
    rets = { r['key'] : r['name'] for r in retailers }
    return rets

def get_catalogue(ret):
    # Query all catalogue of retailer to items
    resp = requests.get(SRV_PROTOCOL + "://" + SRV_CATALOGUE + "/item/source/"+ret)
    catalogue = resp.json()
    return catalogue


def get_total_items(source=SOURCE):
    # Get the items of ims
    resp = requests.get(SRV_PROTOCOL + "://" + SRV_ITEM + "/item/source/" + source)
    catalogue_source = resp.json()
    return catalogue_provider


def get_prices(item_uuid, retailers):
    """ Get the prices per retailer,
        aggregate the prices per branch 
        obj[<ret>] = {
            "max" : "",
            "min" : "",
            "avg" : ""
        }
    """
    result = {}
    qry = "select * from price_item where item_uuid = {} and time > '{}' and time < '{}'".format(item_uuid, then.strftime("%Y-%m-%d %I:%M:%S"), now.strftime("%Y-%m-%d %I:%M:%S"))
    rows = g._db.query(
        qry,
        size=2000,
        timeout=40)
    df = DataFrame(rows)

    # Get the stats per retailer
    for ret in retailers:
        # Get stats of the retailer
        if not df.empty:
            ret_df = df[ df['retailer'] == ret ]
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



def get_stats(items=None, retailers=None):

    result = []
    i = 0

    # Get stats for every item and every retailer
    for it in items:
        try:

            # Get stats per retailer
            prices = get_prices(it['item_uuid'], retailers.keys())

            # Aggregate
            item = OrderedDict()
            item['gtin'] = it['gtin']
            item['name'] = it['name']
            for r_key, r_name in retailers.items():
                item[r_key+'_max'] = prices[r_key]['max']
                item[r_key+'_min'] = prices[r_key]['min']
                item[r_key+'_avg'] = prices[r_key]['avg']

            result.append(item)

        except Exception as e:
            print(e)
            print("An error occured in item: " + str(it))
            continue

        # Save the dataframe
        i+=1
        print("Elem: {}".format(i))
        if i >= ROWS_SAVE:
            save_df(result)
            i=0

    return result     


def save_df(result):
    """ Save the dataframe
    """
    dframe = DataFrame(result)
    print("Saving dataframe...")
    dframe.to_csv(DATA_DIR+"tmp_stats_aggregate.csv", encoding="utf-8")


def df_to_s3(df, source):
    """ Save dataframe directly to s3
    """
    filename = now.strftime("%Y")+"/"+now.strftime("%Y%m")+"/"+now.strftime("%Y%m%d")+"/"+source
    bucket = BUCKET
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3 = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
        )
        s3.put_object(Bucket=bucket,Key=filename, Body=csv_buffer.getvalue())
        return True

    except Exception as e:
        logger.error("Could not save file to s3!")
        logger.error(e)
        return False


@with_context
def start(source):
    """ Build the dataframe
        - Get retailers
        - Get general catalogue
        - Get catalogue of all retailers
        - Get prices for every item
    """
    logger.info("Starting dump script, saving file to: "+BUCKET)
    logger.info("Getting retailers")
    retailers = get_retailers()
    logger.info(len(retailers))
    
    # Loop the sources of data we want as base for the table
    for src in SOURCES:

        logger.info("Getting total items for {}".format(src))
        total_items = get_total_items(src)
        logger.info(len(total_items))

        # Build stats
        logger.info("Building stats")
        stats = get_stats(total_items, retailers)

        # Build dataframe and save to s3
        dframe = DataFrame(stats)
        df_to_s3(dframe, src)

        dframe.to_csv(DATA_DIR+"stats_aggregate.csv", encoding="utf-8")
          

if __name__=='__main__':
    query_regions()