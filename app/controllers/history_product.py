# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app.models.history_product import Product
from app.models.task import asynchronize, Task
from app import errors, applogger
import datetime

mod = Blueprint('history_product',__name__)

# Logger
logger = applogger.get_logger()

@mod.route('/')
def get_hproduct_bp():
    """ History Product route initial endpoint
    """
    logger.info("History Product route initial endpoint")
    return jsonify({'status': 'ok', 'msg' : 'History Product'})

@mod.route('/one')
def get_one():
    """
        Testing connection method
    """
    logger.debug("Testing connection with one product")
    prod = Product.get_one()
    if not prod:
        raise errors.AppError("invalid_request", "Could not fetch data from Cassandra")
    return jsonify(prod)


@mod.route('/bystore', methods=['GET'])
def get_today_prices_bystore():
    """ Get prices from an specific 
        item by day, and closest stores
    """
    logger.debug('Getting prices from uuid...')
    item_uuid, product_uuid = None, None
    # Validate UUIDs
    if 'uuid' in request.args:
        item_uuid = request.args.get('uuid')
        logger.debug("Item UUID: "+ str(item_uuid))
    elif 'puuid' in request.args:
        product_uuid = request.args.get('puuid')
        logger.debug("Product UUID: "+ str(product_uuid))
    else:
        raise errors.AppError(80002, "Request UUID parameters missing")
    # Get default Location in case of not sending the correct one
    lat = float(request.args.get('lat')) if 'lat' in request.args else 19.431380
    lng = float(request.args.get('lng')) if 'lng' in request.args else -99.133486
    radius = float(request.args.get('r')) if 'r' in request.args else 10.0
    # Retrieve prices
    prod = Product.get_by_store(item_uuid,
        product_uuid, lat, lng, radius)
    if not prod:
        '''logger.warning("No prices in queried {} {}!".format('item' if item_uuid else 'product',\
            item_uuid if item_uuid else product_uuid))'''
        return jsonify([])
    logger.info('Found {} prices'.format(len(prod)))
    logger.debug("Response prices:")
    logger.debug(prod[:1] if len(prod) > 1 else [])
    return jsonify(prod)

@mod.route('/bystore/history', methods=['GET'])
def get_history_prices_bystore():
    """ Get prices from an specific item 
        for the past period of time, 
        and closest stores.
    """
    item_uuid, product_uuid = None, None
    # Validate UUIDs
    if 'uuid' in request.args:
        item_uuid = request.args.get('uuid')
        logger.debug("Item UUID: "+ str(item_uuid))
    elif 'puuid' in request.args:
        product_uuid = request.args.get('puuid')
        logger.debug("Product UUID: "+ str(product_uuid))
    else:
        raise errors.AppError(80002, "Request UUID parameters missing")
    # Get default prior amount of days
    period = int(request.args.get('days')) if 'days' in request.args else 7
    # Call to fetch prices
    prod = Product.get_history_by_store(item_uuid, product_uuid, period)
    if not prod:
        '''logger.warning("No prices in queried {} {}!".format('item' if item_uuid else 'product',\
            item_uuid if item_uuid else product_uuid))'''
        return jsonify({})
    logger.info('Found {} metrics'.format(len(prod['history'])))
    return jsonify(prod)

@mod.route('/ticket', methods=['POST'])
def get_ticket_bystore():
    """ Get prices from an especific ticket 
        from N number of items by day and closest stores
    """
    logger.info("Fetching Ticket values")
    item_uuids, product_uuids = [], []
    params = request.get_json()
    if 'uuids' in params:
        item_uuids = params['uuids']
        logger.debug("Item UUIDs: {}".format(item_uuids))
    elif 'puuids' in params:
        product_uuids = params['puuids']
        logger.debug("Item UUIDs: {}".format(product_uuids))
    else:
        raise errors.AppError(80002, "Request UUIDs parameters missing")
    # Retrieve optional params
    lat = float(params['lat']) if 'lat' in params.keys() else 19.431380
    lng = float(params['lng']) if 'lng' in params.keys() else -99.133486
    radius = float(params['r']) if 'r' in params.keys() else 10.0
    # Call function to obtain ticket from all medicines
    ticket = Product.generate_ticket(item_uuids,
        product_uuids, lat, lng, radius)
    if not ticket:
        raise errors.AppError(80005, "Could not generate results for those Items")
    return jsonify(ticket)

@mod.route('/catalogue', methods=['GET'])
def get_all_by_store():
    """ Get the prices of all items of certain store
        Params:
            * r - # Retailer Key
            * sid  -  # Store UUID

        TODO: Make it Work
    """
    logger.info("Fetching Prices per Store")
    # Params validation
    params = request.args.to_dict()
    if 'sid' not in params or 'r' not in params:
        raise errors.AppError(80002, "Retailer or Store UUID parameters missing")
    logger.debug(params)
    catalogue = Product\
        .get_store_catalogue(params['r'],
                            params['sid'])
    if not catalogue:
        raise errors.AppError(80005, "Issues fetching store results")
    return jsonify(catalogue)


@mod.route('/count_by_store/submit', methods=['POST'])
@asynchronize(Product.count_by_store_task)
def count_by_store():
    logger.info("Submited Count by store task...")
    return jsonify({
        'status':'ok', 
        'module': 'task',
        'task_id' : request.async_id
    })



@mod.route('/count_by_store_hours', methods=['GET'])
def count_by_store_hours():
    """ Get the prices of all items 
        of certain store for the past X hours
    """
    logger.info("Fetching Prices per Store in last X hours")
    params = request.args.to_dict()
    _needed = set({'r','sid', 'last_hours'})
    if not _needed.issubset(params.keys()):
        raise errors.AppError(80002, "Hours, Retailer or Store UUID parameters missing")
    logger.debug(params)
    count = Product\
        .get_count_by_store_24hours(params['r'],
            params['sid'], params['last_hours'])
    if not count:
        raise errors.AppError(80005,  "Issues fetching store results")
    return jsonify(count)


@mod.route('/byfile', methods=['GET'])
def get_today_prices_by_file():
    """ Get prices CSV by specific store and past 
        48hrs
    """
    logger.debug('Getting prices in CSV ...')
    params = request.args.to_dict()
    _needed = set({'ret','sid', 'stn'})
    if not _needed.issubset(params.keys()):
        raise errors.AppError(80002, "Name, Retailer or Store UUID parameters missing")
    prod = Product\
        .get_st_catalog_file(params['sid'],
            params['stn'], params['ret'])
    if not prod:
        logger.warning("No prices in Selected Store")
        raise errors.AppError(80009, "No prices in selected Store")
    logger.info("Serving CSV file ...")
    return Response(
        prod, mimetype="text/csv",
        headers={
            "Content-disposition":
                "attachment; filename={}_{}.csv"\
                    .format(params['ret'].upper(),
                            params['stn'])}
        )

@mod.route('/retailer/submit', methods=['POST'])
@asynchronize(Product.start_retailer_task)
def get_prices_by_ret():
    """ Get Today's prices from an 
        specific retailer and products
    """
    '''logger.info("Fetching product' prices by Retailer ")
    # Verify Request Params
    item_uuid, prod_uuid = None, None
    params = request.args.to_dict()
    if 'retailer' not in params:
        raise errors.AppError(80002, "Retailer parameter missing")
    if 'item_uuid' not in params:
        if 'prod_uuid' not in params:
            raise errors.AppError(80002, "Item/Product UUID parameter missing")
        prod_uuid = params['prod_uuid']
    else:
        item_uuid = params['item_uuid']    
    export = params['export'] if 'export' in params else False
    logger.debug(params)
    # Call function to fetch prices
    prod = Product\
        .get_prices_by_retailer(params['retailer'],
            item_uuid, prod_uuid, export)
    if not prod:
        raise errors.AppError(80009,
            "No prices in selected Retailer-Product pair")
    if export:
        _fname = item_uuid if item_uuid else prod_uuid
        # Return a Mimetype Response
        return Response(
            prod,
            mimetype="text/csv",
            headers={"Content-disposition":
                 "attachment; filename={}_{}.csv"\
                    .format(retailer.upper(), _fname)})
    else:
        # Return a JSONified Response
        return jsonify(prod)'''

    logger.info("Submited History Product Retailer task...")
    return jsonify({
        'status': 'ok', 
        'module': 'history_product',
        'task_id' : request.async_id
    })

@mod.route('/compare/details/submit', methods=['POST'])
@asynchronize(Product.start_compare_details_task)
def compare_retailer_item():
    """ Compare prices from a fixed pair retailer-item
        with additional pairs
    """
    logger.info("Submited History Product Retailer task...")
    return jsonify({
        'status': 'ok', 
        'module': 'history_product_compare_details',
        'task_id' : request.async_id
    })

@mod.route('/compare/stores/submit', methods=['POST'])
@asynchronize(Product.start_compare_stores_task)
def compare_stores_item():
    """
        Compare prices from a fixed pair retailer-stores
        
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
            ],
            "territory": [
                "17128984-7ace-11e7-9b9f-0242ac110003", 
                "197591da-7ace-11e7-9b9f-0242ac110003"
            ]
        }

        @Returns:
         - (flask.Response) JSONified response
    """
    logger.info("Submited History Product Compare Stores task...")
    return jsonify({
        'status': 'ok', 
        'module': 'history_product_compare_stores',
        'task_id' : request.async_id
    })


@mod.route('/compare/history/submit', methods=['POST'])
@asynchronize(Product.compare_store_item_task)
def counmpare_hist():
    logger.info("Submited Compare store item task...")
    return jsonify({
        'status':'ok', 
        'module': 'history_product_compare',
        'task_id' : request.async_id
    })


@mod.route('/stats', methods=['GET'])
def get_stats_by_item():
    """ Today's max, min & avg price 
        from an specific item_uuid  or product_uuid
    """
    logger.info("Fetching product stats by item")
    # Validate UUIDs
    item_uuid, product_uuid = None, None
    if 'item_uuid' in request.args:
        item_uuid = request.args.get('item_uuid')
        logger.debug("Item UUID: "+ str(item_uuid))
    elif 'prod_uuid' in request.args:
        product_uuid = request.args.get('prod_uuid')
        logger.debug("Product UUID: "+ str(product_uuid))
    else:
        raise errors.AppError(80002,
            "Request UUID parameters missing")
    # Call function to fetch prices
    prod = Product.get_stats(item_uuid, product_uuid)
    return jsonify(prod)


@mod.route('/count_by_store_engine', methods=['GET'])
def get_count_by_store_engine():
    logger.info("Fetching counts by store")
    # Validation Params
    params = request.args
    Product.validate_count_engine(params)
    result = Product.get_count_by_store_engine(
            params['retailer'],
            params['store_uuid'],
            params['date']
    )
    return jsonify(result)


@mod.route('/count_by_retailer_engine', methods=['GET'])
def get_count_by_retailer_engine():
    """ Get Count by engine
    """
    logger.info("Fetching counts by retailer")
    # Validate params
    params = request.args
    Product.validate_count_ret_eng_params(params)
    # Parse and start task
    prod = Product.count_by_retailer_engine(
        params['retailer'],
        params['date']
    )
    return jsonify(prod)

