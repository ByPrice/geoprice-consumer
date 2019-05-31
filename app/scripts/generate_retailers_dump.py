""" 
    Get catalogue of retailer and prices of today
"""
from flask import g
from app import errors, logger
from config import *
import requests
from collections import defaultdict
import json
import datetime
import pandas as pd
from pandas import DataFrame, Series
from collections import OrderedDict

# Aux vars
dd=lambda:defaultdict(dd)
DATA_DIR = BASE_DIR+"/data/"
PROVIDER = 'ims'
ROWS_SAVE = 5000
now = datetime.datetime.utcnow()
then = now - datetime.timedelta(hours=32)

template = {
    "item_uuid" : [],
    "gtin" : [],
    "name" : [],
    "retailer" : [],
    "price_avg" : [],
    "price_max" : [],
    "price_min" : []
}

def get_prices(item_uuid, retailers, date=None):
    """ Get the prices per retailer,
        aggregate the prices per branch 
        obj[<ret>] = {
            "max" : "",
            "min" : "",
            "avg" : ""
        }
    """
    if date != None:
        now = datetime.datetime.strptime(date, "%Y-%m-%d")
        then = now - datetime.timedelta(hours=32)
    else:
        now = datetime.datetime.utcnow()
        then = now - datetime.timedelta(hours=32)

    result = {}
    qry = """
        select * from price_item 
        where item_uuid = {} and time > '{}' and time < '{}'
    """.format(
        item_uuid, 
        then.strftime("%Y-%m-%d %I:%M:%S"), 
        now.strftime("%Y-%m-%d %I:%M:%S")
    )
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
            p_max, p_min, p_avg = None, None, None
       
        result[ret] = {
            "max" : p_max,
            "min" : p_min,
            "avg" : p_avg
        }
    
    return result



def get_stats(items=None, retailers=None, date=None):
    """ Build the dataframe info querying 
        prices of every retailer

        Params:
        -----
        items: list
            List of items
        retailers: list
            List of retailers
        date: str
            Date in (YYYY-MM-DD) format (default: NoneType)
    """
    # Aux vars
    result = []
    i = 0
    # Get stats for every item and every retailer
    for it in items:
        try:
            # Get stats per retailer
            prices = get_prices(
                it['item_uuid'], 
                retailers.keys(),
                date
            )
            # Aggregate
            item = OrderedDict()
            item['item_uuid'] = it['item_uuid']
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


def build_dump(data_source, date=None):
    """ Build the dump
        - Get retailers
        - Get general catalogue
        - Get catalogue of all retailers
        - Get prices for every item
        - Writes CSV files in Data Directory

        Params:
        -----
        data_source: str
            Data Source or retailer to generate
        date: str
            Date in (YYYY-MM-DD) format (default: NoneType)
    """
    logger.info("Starting dump script, saving file at: "+DATA_DIR)
    logger.info("Getting retailers")
    retailers = g._geolocation.get_retailers()
    logger.info(len(retailers))
    logger.info("Getting total items")
    total_items = g._catalogue.get_by_source(data_source, ['item_uuid', 'gtin'])
    logger.info(len(total_items))

    # Build stats
    logger.info("Building stats")
    stats = get_stats(total_items, retailers, date)

    # Build dataframe
    dframe = DataFrame(stats)
    logger.info("Setting new index")
    dframe.set_index('gtin',inplace=True)
    
    # Name
    name = data_source+"_stats_aggregate.csv"
    if date != None:
        name = "{}_stats_aggregate_{}.csv".format(
            data_source,
            date.replace("-","")
        )

    logger.info("Saving dataframe")
    dframe.to_csv(DATA_DIR+name, encoding="utf-8")
        


if __name__=='__main__':
    query_regions()