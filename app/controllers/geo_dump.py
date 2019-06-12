# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response, g, stream_with_context
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
DATA_DIR = BASE_DIR+"/data"

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
    cols = ['gtin','name', 'item_uuid']
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
    logger.info("Building output")
    if fmt == 'json':
        # Transform to dict
        result_df.reset_index(inplace=True)
        table_head = list(result_df.columns)
        table_body = [ list(v) for v in list(result_df.values) ]
        logger.info("Serving JSON")
        return jsonify({"columns":table_head,"records":table_body})
    # If direct download reomve Item uuid
    result_df.drop('item_uuid', axis=1, inplace=True)
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
        cols=extras+['item_uuid'],
        qsize=2000
    )
    items_ret = g._catalogue.get_by_source(
        data_source=retailer, 
        cols=extras+['item_uuid', 'gtin'],
        qsize=2000
    )
    # Fetch UUIDS only with existing Item  UUID
    _uuids = set(i['item_uuid'] for i in items if i['item_uuid'])
    _uuids_ret = {i['product_uuid'] : i for i in items_ret}

    # Get all the prices of the retailer
    logger.debug("Got {} total items".format(len(items)))
    logger.debug("Getting prices from C* form the last {} hours".format(hours))
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
            tmp = _uuids_ret[c['product_uuid']]
            # Filter to not return products from outside the data source
            if tmp['item_uuid'] not in _uuids:
                continue
            # Format
            ord_d = OrderedDict([
                ("gtin" , tmp['gtin']),
                ("item_uuid" , tmp.get('item_uuid', None)),
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
        except Exception as e:
            logger.error(e)
    
    # Build dataframe
    df = pd.DataFrame(valid)
    logger.info("Serving catalogue - {} prods".format(len(df)))
    if fmt == 'json':
        # Transform to dict
        table_head = list(df.columns)
        table_body = [ list(v) for v in list(df.values) ]
        logger.info("Serving JSON")
        return jsonify({"columns":table_head,"records":table_body})
    # If direct download, drop item_uuid
    df.drop('item_uuid', axis=1, inplace=True)
    return download_dataframe(
        df, 
        fmt=fmt, 
        name="catalogue_{}_{}".format(retailer, datetime.datetime.utcnow().strftime("%Y-%m-%d"))
    )

@mod.route('/items', methods=['POST'])
def dump_items():
    """ Endpoint to query items and stores by daterange and filters:
        Price by Item IDs and Stores retrieval

        @Params:
        - (dict) : Form data with following structure
        {
            "filters": [
                {"item": "452iub4-54o3iu6b3o4b-46i54362"},
                {"item": "452iub4-54o3iu6b3o4b-46i54362"},
                {"store": "452iub4-54o3iu6b3o4b-46i54362"},
                {"store": "452iub4-54o3iu6b3o4b-46i54362"}
            ],
            "retailers": {
                "chedraui": "Chedraui",
                "walmart": "Walmart",
            },
            "date_start": "2017-10-01",
            "date_end": "2018-01-12",
            "interval": "day" // "month", "week" or "day"
        }

        Returns:
        - <dict>

    """
    params = request.get_json()
    # Params validation
    if 'filters' not in params:
            raise errors.AppError(40003, "Filters param Missing!", 400)
    if 'retailers' not in params:
        raise errors.AppError(40003, "Retailers param Missing!", 400)
    if 'date_start' not in params:
        raise errors.AppError(40003, "Start Date param Missing!", 400)
    if 'date_end' not in params:
        raise errors.AppError(40003, "End Date param Missing!", 400)
    if 'interval' not in params:
        # In case interval is not explicit, set to day
        params['interval'] = 'day' 
    # Fetch Prices
    prices =  Dump.get_compare_by_store(
        params['filters'],
        params['retailers'],
        params['date_start'],
        params['date_end'],
        params['interval']
    )
    def generate():
        yield '['
        for i,row in enumerate(prices):
            if i+1 < len(prices):
                yield json.dumps(row)+","
            else:
                yield json.dumps(row)
        yield ']'
    logger.info("Serving dump items!")
    return Response(
            stream_with_context(generate()), 
            content_type='application/json'
        )