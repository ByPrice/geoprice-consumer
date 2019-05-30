# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app.models.stats import Stats
from app import errors, logger
import datetime

mod = Blueprint('stats', __name__)


@mod.route('/')
def get_stats_bp():
    """ Stats route initial endpoint
    """
    logger.info("Stats route")
    return jsonify({'statis': 'ok', 'module': 'stats'})


def jsonfier(prod):
    """ TODO: Reuse code if useful, otherwiser to clean later
    """
    logger.debug('Constructing JSON response...')
    if not prod:
        if isinstance(prod, list):
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
    """ TODO: Reuse code if useful, otherwiser to clean later
    """
    if not prod:
        raise errors.AppError(80009, "No prices available!")
    prod_csv = Stats.convert_csv_actual(prod)
    return Response(
        prod_csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                     "attachment; filename=actuales.csv"})


def market_file(prod):
    """ TODO: Reuse code if useful, otherwiser to clean later
    """
    if not prod:
        raise errors.AppError(80009, "No prices available!")
    prod_csv = Stats.convert_csv_market(prod)
    return Response(
        prod_csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                     "attachment; filename=mercados.csv"})


@mod.route('/current/submit', methods=['POST'])
def get_current():
    """
        Controller to get item avg prices by filters

        {
        "filters" : [
            { "category" : "9406" },
            { "retailer" : "superama" },
            { "retailer" : "ims" },
            { "item" : "08cdcbaf-0101-440f-aab3-533e042afdc7" }
        ],
        "export":true
        }

        # TODO: Make it work async, but without `export`
    """
    logger.debug("Fetching needed filters...")
    filters = request.get_json()
    if not filters['filters']:
        raise errors.AppError(10000, "Not filters requested!")
    prod = Stats.get_actual_by_ret(filters['filters'])
    if 'export' in filters:
        if filters['export']:
            return actual_file(prod)
    return jsonfier(prod)


@mod.route('/compare', methods=['POST'])

def compare():
    """
        Controller to get item avg prices by filters compared to all others
        {
            "client": "walmart",
            "date_start" : "2017-08-08",
            "date_end" : "2017-08-10",
            "filters" : [
                { "category" : "9406" },
                { "retailer" : "superama" },
                { "retailer" : "ims" },
                { "item" : "08cdcbaf-0101-440f-aab3-533e042afdc7" }
            ],
            "ends": true,
            interval: "day",
            "export": true
        }
        # TODO: Make it work async, but without `export`
    """
    logger.debug("Fetching needed by filters...")
    params = request.get_json()
    if not params:
        raise errors.AppError(10000, "Not filters requested!")
    prod = Stats.get_comparison(params)
    if 'export' in params:
        if params['export']:
            return market_file(prod)
    return jsonfier(prod)


@mod.route('/direct_compare', methods=['POST'])

def direct_compare():
    """
        Controller to get price average of all products inside a category and count
        Params:
        {
            "filters":[{"item":"08cdcbaf-0101-440f-aab3-533e042afdc7"},
                        {"item":"08cdcbaf-0101-440f-aab3-533e042afdc7"},
                        {"retailer":"walmart"}]
        }
        # TODO: Make it work async, but without `export`
    """
    logger.debug("Direct compare data...")
    params = request.get_json()
    if not params:
        raise errors.AppError(10010, "No parameters passed!")
    if 'filters' not in params:
        raise errors.AppError(10011, "No filters param passed!")

    logger.info("Filters: {}".format(params['filters']))
    cat_count = Stats.get_matched_items(params)
    print(cat_count)
    return jsonfier(cat_count)


@mod.route('/history', methods=['POST'])
def get_actual():
    """
        Controller to get item avg prices by filters for charts rendering

        {
            "date_start" : "2017-08-08",
            "date_end" : "2017-08-10",
            "filters" : [
                { "category" : "9406" },
                { "retailer" : "superama" },
                { "retailer" : "ims" },
                { "item" : "08cdcbaf-0101-440f-aab3-533e042afdc7" }
            ],
            interval: "day",
            "export": true
        }
        # TODO: Make it work async, but without `export`
    """
    # Set Python datetime to JS timetamp
    """
    dt = tuple(int(x) if i!= 1 else int(x)+1\
        for i,x in enumerate(d.isoformat().split('-')))+(0,0)
    d_js = datetime.datetime(*dt)
    ts_js = (d_js - datetime.datetime(1970, 1, 1,0,0))\
            datetime.timedelta(seconds=1)*1000
    """
    logger.debug("Fetching needed by filters...")
    params = request.get_json()
    if not params:
        raise errors.AppError(10000, "Not filters requested!")
    prod = Stats.get_historics(params)
    if 'export' in params:
        if params['export']:
            return market_file(prod)
    return jsonfier(prod)


@mod.route('/category', methods=["POST"])
def get_category_count():
    """
        Controller to get price average of all products inside a category and count
        Params:
        {
            "filters": [
                {"item": "98440d28-64be-4994-8244-2b2aa57b0c1a"},
                {"item": "56e67b35-d27e-4cac-9e91-533e0578b59c"},
                {"item": "3a8b8a6f-82df-4bbd-84bf-3d291f0a3b29"},
                {"item": "decd74df-6a9d-4614-a0e3-e02fe13d1542"},
                {"item": "62ec9ad5-2c26-483e-8413-83499d5eef04"},
                {"retailer": "f_ahorro"},
                {"retailer": "la_comer"}
            ]
        }
        # TODO: Make it work async
    """
    logger.debug("Fetching category counts...")
    params = request.get_json()
    if not params:
        raise errors.AppError(10010, "No parameters passed!")
    if 'filters' not in params:
        raise errors.AppError(10011, "No filters param passed!")
    cat_count = Stats.get_count_by_cat(params['filters'])
    return jsonfier(cat_count)
