# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from app.models.geo_alert import Alert
from app import errors, logger
import datetime

mod = Blueprint('geo_alert',__name__)


@mod.route('/')
def get_alert_bp():
    """ Geo Alert route initial endpoint
    """
    logger.info("Geo Alert route initial endpoint")
    return jsonify({'status': 'ok', 'msg' : 'Geo Alert'})

@mod.route('/prices', methods=['POST'])
def check_prices_today():
	"""
		Verify if an especific request of N number of items from today and the prior day with retailer exclusions
		Params:
		{
			'uuids' : ['2h354iu23h5423i5uh23i5', '30748123412057g1085h5oh3'],
			'retailers' : ['walmart','chedraui'],
			'today' : '2017-09-20'
		}
	"""
	logger.info('Geo Alert prices endpoint.')
	params = request.get_json()
	if 'uuids' not in params:
		raise errors.AppError("invalid_request", "UUIDs parameters missing")
	if 'retailers' not in params:
		raise errors.AppError("invalid_request", "Retailers parameters missing")
	if 'today' not in params:
		params['today'] = datetime.date.today().__str__()
	logger.debug('Params correct...')
	prices = Alert.prices_vs_prior(params)
	return jsonify(prices)


@mod.route('/price_compare', methods=['POST'])
def check_prices_compare():
	"""
		Get price date for alerts, from the given stores, items
		and stores. Returns only the items that are off the variation
		in the items and stores specified.

		@Payload:
			- stores <list (uuids, retailer)>
			- alerts <list >
			- retailers <list (str)>
			- date <str (%Y-%m-%d)>
				: date to get the prices from

        TODO: Make it Work
	"""
	logger.debug('price compare endpoint...')
	params = request.get_json()
	if 'date' not in params:
		params['date'] = (datetime.datetime.utcnow()+datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
	logger.debug('Params correct...')
	logger.debug(params)

	prices = Alert.get_price_compare(params)

	return jsonify(prices)


@mod.route('/geolocated', methods=['POST'])
def check_prices_geolocated():
	"""
		Get price date for alerts, from the given stores, items
		and stores. Returns only the items that are off the variation
		in the items and stores specified.

		@Payload:
			- stores <list (uuids, retailer)>
			- items <list <tuple (uuid, price)>>
			- retailers <list (str)>
			- date <str (%Y-%m-%d)>
				: date to get the prices from
			- variation
			- variation_type

        TODO: Make it Work
	"""
	logger.debug('alert geolocated endpoint...')
	params = request.get_json()
	if 'retailers' not in params:
		raise errors.AppError("invalid_request", "Retailers parameters missing")
	if 'items' not in params:
		raise errors.AppError("invalid_request", "items parameters missing")
	if 'date' not in params:
		params['date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d')
	logger.debug('Params correct...')

	try:
		prices = Alert.get_geolocated(params)
		for i,p in enumerate(prices):
			prices[i]['day'] = p['time'].strftime("%Y-%m-%d")

	except:
		raise errors.AppError('server_serror',"Alerts geolocation failed")
	return jsonify(prices)
