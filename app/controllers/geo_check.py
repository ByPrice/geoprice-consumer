import json
from app import errors, logger
from flask import Blueprint, g, request, jsonify
from flask_cors import CORS, cross_origin
import requests
from config import SRV_GEOLOCATION
import datetime
import uuid

mod = Blueprint("geo_check", __name__)

@mod.route('/')
def get_check_bp():
    """ Check route initial endpoint
    """
    logger.info("Check route initial endpoint")
    return jsonify({'status': 'ok', 'msg' : 'Geo Check'})

@mod.route('/stores/<retailer>')
@cross_origin(origin="*",methods=['GET','POST'])
def check_stores(retailer):
    """ Check stores with prices from a 
        given retailer
            @Params:
                - retailer
            @Response:
                - stores: list of stores with prices    

        TODO: Make it Work            
    """
    logger.debug("Start checking stores...")
    # Get the list of active stores from geolocation
    resp = requests.get("http://"+SRV_GEOLOCATION+"/store/retailer?key={}".format(retailer))
    stores = resp.json()
    logger.debug("Got {} total stores".format(len(stores)))

    # Time
    now = datetime.datetime.utcnow()
    then = now - datetime.timedelta(days=3)

    if not stores:
        raise errors.AppError("price_geo_error","Could not get stores from geolocation service")

    # For every store, get at least one record for the day
    valid_stores = []
    for store in stores:
        # Get one store
        rows = g._db.query("""
            select * from price_store 
            where retailer = %(retailer)s
            and store_uuid = %(store_uuid)s
            and time > %(time)s
            limit 1
        """, {
            "retailer" : retailer,
            "store_uuid" : uuid.UUID(store['uuid']),
            "time" : then.strftime("%Y-%m-%d")
        })
        if rows:
            valid_stores.append(store)

    return jsonify(valid_stores)