import json
from app import errors, logger
from flask import Blueprint, g, request, jsonify
from flask_cors import CORS, cross_origin
from app.models.geo_check import Check

mod = Blueprint("geo_check", __name__)

@mod.route('/')
def get_check_bp():
    """ Check route initial endpoint
    """
    logger.info("Check route initial endpoint")
    return jsonify({'status': 'OK', 'msg' : 'Geo Check'})


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
    logger.info("Start checking stores...")
    valid_stores = Check.valid_stores(retailer)
    logger.info("Serving checked stores")
    return jsonify(valid_stores)