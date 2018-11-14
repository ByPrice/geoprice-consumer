# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app.models.stats import Stats
from app.utils import errors, logger
import datetime

mod = Blueprint('stats',__name__)

@mod.route('/')
def get_stats_bp():
    """ Stats route initial endpoint
    """
    logger.info("Stats route initial endpoint")
    return jsonify({'msg' : 'Stats Route'})

def jsonfier(prod):
    logger.debug('Constructing JSON response...')
    if not prod:
        if isinstance(prod,list):
            raise errors.AppError(80011,
                'No products available with those filters!', 200)
        raise errors.AppError(80009, "No prices available!")
    try:
        jp = jsonify(prod)
    except Exception as e:
        logger.error(e)
        raise errors.AppError(89999, 'Internal Error', 400)
    logger.debug('Sending JSON response...')
    return jp

def actual_file(prod):
    if not prod:
        raise errors.AppError(80009, "No prices available!")
    prod_csv = Stats.convert_csv_actual(prod)
    return Response(
        prod_csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=actuales.csv"})

def market_file(prod):
    if not prod:
        raise errors.AppError(80009, "No prices available!")
    prod_csv = Stats.convert_csv_market(prod)
    return Response(
        prod_csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=mercados.csv"})

@mod.route('/current', methods=['POST'])
def get_current():
    """ Get item aggregated prices by filters
    """
    logger.info("Fetching needed filters...")
    filters = request.get_json()
    if 'filters' not in filters:
        raise errors.AppError(80004, "Filters params missing")
    if not filters['filters']:
        raise errors.AppError(80004, "Filters params missing")
    prod = Stats\
        .get_actual_by_ret(filters['filters'])
    if 'export' in filters:
        if filters['export']:
            return actual_file(prod)
    return jsonfier(prod)

@mod.route('/compare', methods=['POST'])
def compare():
    """ Get item avg prices by filters 
        compared to all others
    """
    logger.info("Fetching needed by filters...")
    params = request.get_json()
    if not params:
        raise errors.AppError(80002,"No params in request")
    prod = Stats.get_comparison(params)
    if 'export' in params:
        if params['export']:
            return market_file(prod)
    return jsonfier(prod)


@mod.route('/history', methods=['POST'])
def get_history():
    """ Get item avg prices by filters
        for charts rendering
    """
    logger.info("Fetching needed by filters...")
    params = request.get_json()
    if not params:
        raise errors.AppErrorr(80002,"No params in request")
    prod = Stats.get_historics(params)
    return jsonfier(prod)


@mod.route('/category', methods=["POST"])
def get_category_count():
    """ Get price average of all products 
        inside a category and its product's count.
    """
    logger.info("Fetching category counts...")
    params = request.get_json()
    if not params:
        raise errors.AppError(80002,"No params in request")
    if 'filters' not in params:
        raise errors.AppError(80004, "Filters params missing")
    cat_count = Stats.get_count_by_cat(params['filters'])
    return jsonfier(cat_count)


@mod.route('/stats/<uuid>', methods=['GET'])
def get_stats_by_uuid(uuid):
    """ Today's max, min & avg price
        from an specific item_uuid  or product_uuid
    """
    logger.info("Fetching stats by uuid")
    if 'stats' in request.args:
        stats = request.args.get('stats').split(",")
        stats = [stat.lower() for stat in stats if stat.lower() in ["max", "min", "avg"]]
        if not stats:
            logger.warning("Wrong Stats parameters!")
            stats = ["avg"]
    else:
        stats = ["avg"]
    # Call function to fetch prices
    json_ = Stats.stats_by_uuid(uuid, stats)
    return jsonify(json_)

@mod.route('/exists/<uuid>', methods=['GET'])
def get_exists_by_uuid(uuid):
    """ Exists or not Price for specific item_uuid  or product_uuid
    """
    # Call function to fetch prices
    exists = Stats.exists_by_uuid(uuid)
    return jsonify({
        "exists": exists,
        "uuid": uuid
    })