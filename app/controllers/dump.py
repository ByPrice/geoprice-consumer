# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, applogger
from app.models.response import download_dataframe
from app.models.item import Item
from app.models.price import Price
from config import *
import requests
import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import os.path
import datetime
import json
from collections import OrderedDict

# Blueprint instance
mod = Blueprint('dump', __name__)
# Logger
logger = applogger.get_logger()
DATA_DIR = BASE_DIR+"/data/"

@mod.route('/')
def dump_status():
    """ Dump blueprint status endpoint
    """
    return jsonify({'status':'ok', 'module': 'dump'})

@mod.route('/download')
def dump_download():
    """ Download dump
    """
    # Data source
    data_source = request.args.get("data_source","ims")
    
    fmt = request.args.get("format","csv")
    rets = request.args.get("retailers",None)
    fname = DATA_DIR+data_source+"_stats_aggregate.csv"

    # Count the times downloaded
    with open(DATA_DIR+'/downloads.json','r') as file:
        count_file = json.load(file)

    count_file['count'] += 1
    with open(DATA_DIR+'/downloads.json','w') as file:
        file.write(json.dumps(count_file))

    # Check if file exists
    if not os.path.isfile(fname):
        logger.error("File not found: {}".format(fname))
        raise errors.AppError("no_file","File does not exist",4008)

    # Get all retailers from geo
    logger.info("Requesting all retailers")
    resp = requests.get("http://"+SRV_GEOLOCATION+"/retailer/all")
    total_rets = resp.json()
    retailer_names = { r['key'] : r['name'] for r in total_rets }

    # Get the requested retailers
    if rets:
        retailers = rets.split(",")
    else:
        retailers = retailer_names.keys()

    # Adjust dataframe
    logger.info("Reading csv file")
    _df = pd.read_csv(fname)
    cols = ['gtin','name']
    for ret in retailers:
        cols.append(ret+"_max")
        cols.append(ret+"_min")
        cols.append(ret+"_avg")

    df = _df[cols]

    # Rename the columns
    logger.info("Renaming columns")
    for key in retailers:
        r_name = retailer_names[key]
        logger.info("Renaming {} -> {}".format(key+"_max", r_name+" (max)"))
        df.rename(columns={
            key+"_max" : r_name+" (max)",
            key+"_min" : r_name+" (min)",
            key+"_avg" : r_name+" (avg)",
        }, inplace=True)

    # Drop rows without prices
    df.set_index('gtin',inplace=True)
    result_df = df.dropna(thresh=3).replace(np.nan, '-')

    logger.info("Building output")
    return download_dataframe(result_df, fmt=fmt, name="prices_retailers_"+datetime.datetime.utcnow().strftime("%Y-%m-%d"))


@mod.route('/catalogue', methods=['GET'])
def dump_catalogue():
    """
        Get the entire catalogue of a retailer and download it
    """
    logger.debug("Start retreiving catalogue")
    retailer = request.args.get("retailer", None)
    retailer_name = request.args.get("retailer_name",retailer)
    store_uuid = request.args.get("store_uuid",None)
    store_name = request.args.get("store_name","Default")
    fmt = request.args.get("fmt","csv")
    data_source = request.args.get("data_source",None)
    
    try:
        hours = int(request.args.get("hours",32))
    except:
        hours = 32
    if 'extras' in request.args:
        extras = request.args.get("extras").split(',')
    else:
        extras =[]

    # If not retailer or store, raise app error
    if not retailer or not store_uuid:
        raise errors.AppError("dump_error","Missing parameters in request")

    # Get all the items
    logger.debug("Getting total items from {}".format(data_source))
    items = Item.get_total_items(
        data_source=data_source, 
        extras=extras
    )
    _uuids = {i['item_uuid'] : i for i in items}

    # Get all the prices of the retailer
    logger.debug("Got {} total items".format(len(items)))
    logger.debug("Getting prices from C* form the las {} hours".format(hours))
    catalogue = Price.get_by_store(
        retailer=retailer, 
        store_uuid=store_uuid, 
        hours=hours
    )

    # Only the items that are permitted
    valid = []
    logger.debug("Got {} prices".format(len(catalogue)))
    logger.debug("Looping through catalogue")
    for c in catalogue:
        try:
            tmp = _uuids[c['item_uuid']]
            ord_d = OrderedDict([
                ("gtin" , _uuids[c['item_uuid']]['gtin']),
                ("name" , _uuids[c['item_uuid']]['name']),
                ("price" , c['price']),
                ("price_original" , c['price_original']),
                ("discount" , (c['price']-c['price_original'])),
                ("promo" , c['promo']),
                ("retailer" , retailer_name),
                ("store" , store_name),
                ("name" , _uuids[c['item_uuid']]['name']),
            ])
            for ex in extras:
                ord_d.update([(ex, _uuids[c['item_uuid']][ex] )])
            valid.append(ord_d)
        except:
            pass
    
    # Build dataframe
    df = DataFrame(valid)
    print(df.head())
    return download_dataframe(
        df, 
        fmt=fmt, 
        name="catalogue_{}_{}".format(retailer, datetime.datetime.utcnow().strftime("%Y-%m-%d"))
    )
