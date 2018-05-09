# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app.models.product import Product
from app import errors, logger
import datetime

mod = Blueprint('product',__name__)

@mod.route('/one')
def get_one():
    """
        Testing connection method
    """
    logger.debug("Testing connection with one product")
    prod = Product.get_one()
    if not prod:
        raise errors.ApiError("invalid_request", "Could not fetch data from Cassandra")
    return jsonify(prod)

@mod.route('/bystore', methods=['GET'])
def get_today_prices_bystore():
    """
        Get prices from an specific item by day, and closest stores
    """
    logger.debug('Getting prices from uuid...')
    if 'uuid' in request.args:
        item_uuid = request.args.get('uuid') 
    else:
        raise errors.ApiError("invalid_request", "UUID parameter missing")

    # Get default Location in case of not sending the correct one
    lat = float(request.args.get('lat')) if 'lat' in request.args else 19.431380
    lng = float(request.args.get('lng')) if 'lng' in request.args else -99.133486
    radius = float(request.args.get('r')) if 'r' in request.args else 10.0

    logger.debug("Item UUID: "+ str(item_uuid))
    prod = Product.get_by_store(item_uuid, lat, lng, radius)
    if not prod:
        #raise errors.ApiError("invalid_request", "Wrong UUID parameter")
        logger.warning("Wrong UUID parameter")
        return jsonify([])
    logger.debug("Response prices:")
    #logger.debug(prod)
    logger.info(len(prod))
    return jsonify(prod)


@mod.route('/bystore/history', methods=['GET'])
def get_history_prices_bystore():
    """
        Get prices from an specific item for the past period of time, and closest stores
    """
    if 'uuid' in request.args:
        item_uuid = request.args.get('uuid') 
    else:
        raise errors.ApiError("invalid_request", "UUID parameter missing")

    # Get default Location in case of not sending the correct one
    lat = float(request.args.get('lat')) if 'lat' in request.args else 19.431380
    lng = float(request.args.get('lng')) if 'lng' in request.args else -99.133486
    radius = float(request.args.get('r')) if 'r' in request.args else 10.0
    period = float(request.args.get('days')) if 'days' in request.args else 7.0

    logger.debug("Item UUID: "+ str(item_uuid))
    prod = Product.get_history_by_store(item_uuid, lat, lng, radius, period)
    if not prod:
        raise errors.ApiError("invalid_request", "Wrong UUID parameter")
    return jsonify(prod)


@mod.route('/ticket', methods=['POST'])
def get_ticket_bystore():
    """
        Get prices from an especific ticket from N number of items by day and closest stores
    """
    logger.info("Fetching Ticket values")
    params = request.get_json()
    if 'uuids' not in params.keys():
        raise errors.ApiError("invalid_request", "UUIDs parameters missing")
    lat = float(params['lat']) if 'lat' in params.keys() else 19.431380
    lng = float(params['lng']) if 'lng' in params.keys() else -99.133486
    radius = float(params['r']) if 'r' in params.keys() else 10.0
    max_rets = int(params['max']) if 'max' in params.keys() else len(params['uuids'])
    exclude = params['exclude'] if 'exclude' in params.keys() else []
    # Call function to obtain ticket from all medicines
    logger.debug(params['uuids'])
    ticket = Product.generate_ticket(params['uuids'], lat, lng, radius, max_rets, exclude)
    if not ticket:
        raise errors.ApiError("invalid_request", "Could not optimize those Items")
    return jsonify(ticket)

@mod.route('/catalogue', methods=['GET'])
def get_all_by_store():
    """
        Get the prices of all items of certain store
        Params:
            * r - # Retailer Key
            * sid  -  # Store UUID
    """
    logger.info("Fetching Prices per Store")
    params = request.args
    if 'sid' not in params or 'r' not in params:
        raise errors.ApiError("invalid_request", "Retailer or Store UUID missing")
    logger.debug(str(params['r']))
    catalogue = Product.get_store_catalogue(params['r'], params['sid'])
    if not catalogue:
        raise errors.ApiError("invalid_request", "Store Catalogue not reachable")
    return jsonify(catalogue)

@mod.route('/count_by_store', methods=['GET'])
def count_by_store():
    """
        Get the prices of all items of certain store
        Params:
            * r - # Retailer Key
            * sid  -  # Store UUID
            * date_start - # Start Date
            * date_end - # End Date
    """
    logger.info("Fetching Prices per Store in all Retailers")
    params = request.args
    if 'sid' not in params or 'r' not in params or 'date_start' not in params or 'date_end' not in params:
        raise errors.ApiError("invalid_request", "Retailer or Store UUID missing")
    logger.debug(str(params['r']))
    count = Product.get_count_by_store(params['r'], params['sid'], params['date_start'], params['date_end'])
    if not count:
        raise errors.ApiError("invalid_request", "Store Catalogue not reachable")
    return jsonify(count)

@mod.route('/count_by_store_hours', methods=['GET'])
def count_by_store_hours():
    """
        Get the prices of all items of certain store
        Params:
            * r - # Retailer Key
            * sid  -  # Store UUID
            * last_hours - # Last hours
    """
    logger.info("Fetching Prices per Store in all Retailers in last hours")
    params = request.args
    if 'sid' not in params or 'r' not in params or 'last_hours' not in params:
        raise errors.ApiError("invalid_request", "Retailer or Store UUID missing")
    logger.debug(str(params['r']))
    count = Product.get_count_by_store_24hours(params['r'], params['sid'], params['last_hours'])
    if not count:
        raise errors.ApiError("invalid_request", "Store Catalogue not reachable")
    return jsonify(count)


@mod.route('/byfile', methods=['GET'])
def get_today_prices_by_file():
    """
        Get prices CSV by day, and specific store
        @Params:
         - sid     :  Store uuid
         - ret     :  Retailer
         - stn     :  Store name
    """
    logger.debug('Getting prices CSV from uuids...')
    if 'sid' in request.args:
        store_uuid = request.args.get('sid') 
    else:
        raise errors.ApiError("invalid_request", "Store UUID parameter missing")
    if 'ret' in request.args:
        retailer = request.args.get('ret') 
    else:
        raise errors.ApiError("invalid_request", "Retailer key parameter missing")
    if 'stn' in request.args:
        store_name = request.args.get('stn') 
    else:
        raise errors.ApiError("invalid_request", "Store Name parameter missing")

    prod = Product.get_st_catag_file(store_uuid, store_name, retailer)
    if not prod:
        #raise errors.ApiError("invalid_request", "Wrong UUID parameter")
        logger.warning("No prices in Selected Store")
        raise errors.ApiError("no_prods", "No products in selected store")
    logger.debug("Response file")
    return Response(
        prod,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename={}_{}.csv".format(retailer.upper(), store_name)})




@mod.route('/retailer', methods=['GET'])
def get_prices_by_ret():
    """
        Get Today's prices from an specific retailer and product
        
        @Params:
         - retailer : (str) Retailer Key
         - item_uuid : (str) Item UUID
         - export : (bool, optional) Exporting flag

        @Returns:
         - (flask.Response)  # if export: Mimetype else: JSON
    """
    logger.info("Fetching product' prices by Ret ")
    # Verify Request Params
    params = request.args
    if 'retailer' not in params:
        raise errors.ApiError("invalid_request", "Retailer key missing")
    retailer = params.get('retailer')
    if 'item_uuid' not in params:
        raise errors.ApiError("invalid_request", "Item UUID missing")
    item_uuid = params.get('item_uuid')
    if 'export' in params:
        export = params.get('export')
    else:
        export = False
    # Call function to fetch prices
    prod = Product.get_prices_by_retailer(retailer, item_uuid, export)
    if not prod:
        logger.error("No prices available for this combination.")
        raise errors.ApiError("no_prods",
                              "No products with that Retailer and item combination.")
    if export:
        # Return a Mimetype Response
        return Response(
            prod,
            mimetype="text/csv",
            headers={"Content-disposition":
                 "attachment; filename={}_{}.csv".format(retailer.upper(), item_uuid)})
    else:
        # Return a JSONified Response
        return jsonify(prod)


@mod.route('/compare/details', methods=['POST'])
def compare_retailer_item():
    """
        Compare prices from a fixed pair retailer-item
        with additional pairs
        
        @Request:
        {
            "date": "2017-11-01",
            "fixed_segment" : {
                "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                "retailer": "chedraui"
                },
            "added_segments": [
                { 
                    "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                    "retailer": "walmart"
                },
                {
                    "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                    "retailer": "soriana"
                }
            ]
        }

        @Returns:
         - (flask.Response) JSONified response
    """
    logger.info("Comparing pairs Ret-Item")
    # Verify Params
    params = request.get_json()
    if 'fixed_segment' not in params:
        raise errors.ApiError("invalid_request", "Fixed Segment missing")
    if 'added_segments' not in params:
        raise errors.ApiError("invalid_request", "Added Segments missing")
    if not isinstance(params['fixed_segment'], dict):
        raise errors.ApiError("invalid_request", "Wrong Format: Fixed Segment")
    if not isinstance(params['added_segments'], list):
        raise errors.ApiError("invalid_request", "Wrong Format: Added Segments")
    if 'date' in params:
        try:
            _date = datetime.datetime(*[int(d) for d in params['date'].split('-')])
        except Exception as e:
            logger.error(e)
            raise errors.ApiError("invalid_request", "Wrong Format: Date")
    else:
        _date = datetime.datetime.utcnow()
    # Call function to fetch prices
    prod = Product.get_pairs_ret_item(params['fixed_segment'],
                            params['added_segments'],
                            _date)
    if not prod:
        logger.error("Not able to fetch prices.")
        raise errors.ApiError("no_prods",
                              "No products with that Retailer and item combination.")
    return jsonify(prod)

@mod.route('/compare/history', methods=['POST'])
def compare_store_item():
    """
        Compare prices from a fixed pair store-item
        in time with additional pairs
        
        @Request:
        {
            "date_ini": "2017-12-01",
            "date_fin": "2017-12-07",
            "interval": "day",
            "fixed_segment" : {
                "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                "retailer": "chedraui",
                'store_uuid': 'e02a5370-7b09-11e7-855a-0242ac110005',
                'name': 'CHEDRAUI SELECTO UNIVERSIDAD'
                },
            "added_segments": [
                { 
                    "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                    "retailer": "walmart",
                    'store_uuid': '16faeaf4-7ace-11e7-9b9f-0242ac110003', \
                    'name': 'Walmart Universidad'
                },
                {
                    "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
                    "retailer": "soriana",
                    'store_uuid': '8c399b5e-7b04-11e7-855a-0242ac110005', \
                    'name': 'Soriana Plaza delta-Soriana Hiper'
                }
            ]
        }

        @Returns:
         - (flask.Response) JSONified response
    """
    logger.info("Comparing pairs Ret-Item")
    # Verify Params
    params = request.get_json()    
    # Existance verif
    if 'fixed_segment' not in params:
        raise errors.ApiError("invalid_request", "Fixed Segment missing")
    if 'added_segments' not in params:
        raise errors.ApiError("invalid_request", "Added Segments missing")
    # Datatype verif
    if not isinstance(params['fixed_segment'], dict):
        raise errors.ApiError("invalid_request", "Wrong Format: Fixed Segment")
    if not isinstance(params['added_segments'], list):
        raise errors.ApiError("invalid_request", "Wrong Format: Added Segments")
    # Dates verif
    if ('date_ini' not in params) or ('date_fin' not in params):
        raise errors.ApiError("invalid_request", "Missing Format: Dates")
    if 'interval' in params:
        if params['interval'] not in ['day','week','month']:
            raise errors.ApiError("invalid_request", "Wrong interval type")
    else:
        params['interval'] = 'day'
    # Call function to fetch prices
    prod = Product.get_pairs_store_item(params['fixed_segment'],
                            params['added_segments'],
                            params)
    if not prod:
        logger.error("Not able to fetch prices.")
        raise errors.ApiError("no_prods",
                              "No products with that Retailer and item combination.")
    return jsonify(prod)

