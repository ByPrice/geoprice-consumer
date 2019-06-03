# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response, g
from app import errors, logger
from app.models.response import download_dataframe
from app.models.item import Item
from app.models.price import Price
from config import *
import requests
import pandas as pd
import numpy as np
import os.path
import datetime
import json
from collections import OrderedDict
from app.models.geo_dump import Dump

# Blueprint instance
mod = Blueprint('geo_dump', __name__)
DATA_DIR = BASE_DIR+"/data/"

@mod.route('/')
def dump_status():
    """ Dump blueprint status endpoint
    """
    logger.info("Geo Dump route initial endpoint")
    return jsonify({'status':'ok', 'module': 'Geo dump'})

@mod.route('/download')
def dump_download():
    """ Download dump

        Request Params:
        -----
        data_source : Type of Download given the source catalogue
        format : Downloadable format (csv |  excel)
        retailers : Comma Separated Retailers 
    """
    logger.info("Starting to download dumps.. ")
    # Data source
    data_source = request.args.get("data_source","ims")
    # Define Dump format
    fmt = request.args.get("format", "csv")
    rets = request.args.get("retailers", None)
    fname = data_source + "_stats_aggregate.csv"

    # Count the times downloaded
    with open(DATA_DIR+'/downloads.json','r') as file:
        count_file = json.load(file)
    count_file['count'] += 1
    with open(DATA_DIR+'/downloads.json','w') as file:
        file.write(json.dumps(count_file))
    # Get all retailers from geo
    logger.info("Requesting all retailers")
    total_rets = g._geolocation.get_retailers()
    retailer_names = { r['key'] : r['name'] for r in total_rets }
    # Get the requested retailers
    if rets:
        retailers = rets.split(",")
    else:
        retailers = retailer_names.keys()

    # Adjust dataframe
    logger.info("Reading csv file from S3")
    _df = Dump.get_recent_from_s3(fname)
    if _df.empty:
        raise errors.AppError("no_file", "No available dump file found!")
    cols = ['gtin','name']
    for ret in retailers:
        cols.append(ret+"_max")
        cols.append(ret+"_min")
        cols.append(ret+"_avg")
    df = _df[cols].copy()

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
    df.set_index('gtin', inplace=True)
    result_df = df.dropna(thresh=3).replace(np.nan, '-')
    logger.info("Building output CSV")
    return download_dataframe(result_df, fmt=fmt, name="prices_retailers_"+datetime.datetime.utcnow().strftime("%Y-%m-%d"))


@mod.route('/catalogue', methods=['GET'])
def dump_catalogue():
    """ Get the entire catalogue of a retailer and download it
    """
    logger.info("Start retrieving catalogue")
    retailer = request.args.get("retailer", None)
    retailer_name = request.args.get("retailer_name",retailer)
    store_uuid = request.args.get("store_uuid",None)
    store_name = request.args.get("store_name", "Default")
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
    if not retailer and not store_uuid:
        raise errors.AppError("dump_error",
            "Missing parameters in request")

    # Get all the items
    logger.debug("Getting total items from {}".format(data_source))
    items = g._catalogue.get_by_source(
        data_source=data_source, 
        cols=extras
    )
    _uuids = {i['product_uuid'] : i for i in items}

    # Get all the prices of the retailer
    logger.debug("Got {} total items".format(len(items)))
    logger.debug("Getting prices from C* form the las {} hours".format(hours))
    catalogue = Price.get_by_store(
        store_uuid, 
        hours
    )

    # Only the items that are permitted
    valid = []
    logger.debug("Got {} prices".format(len(catalogue)))
    logger.debug("Looping through catalogue")
    for c in catalogue:
        try:
            tmp = _uuids[c['product_uuid']]
            ord_d = OrderedDict([
                ("gtin" , tmp['gtin']),
                ("name" , tmp['name']),
                ("price" , c['price']),
                ("price_original" , c['price_original']),
                ("discount" , (c['price']-c['price_original'])),
                ("promo" , c['promo']),
                ("retailer" , retailer_name),
                ("store" , store_name)
            ])
            for ex in extras:
                ord_d.update([(ex, tmp[ex] )])
            valid.append(ord_d)
        except:
            pass
    
    # Build dataframe
    df = pd.DataFrame(valid)
    logger.info("Serving catalogue")
    return download_dataframe(
        df, 
        fmt=fmt, 
        name="catalogue_{}_{}".format(retailer, datetime.datetime.utcnow().strftime("%Y-%m-%d"))
    )
