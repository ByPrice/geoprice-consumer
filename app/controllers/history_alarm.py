# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app.models.history_alarm import Alarm
from app import errors, logger
import datetime

mod = Blueprint('history_alarm',__name__)

@mod.route('/')
def get_alarm_bp():
    """ Alarm route initial endpoint
    """
    logger.info("Alarm route initial endpoint")
    return jsonify({'status': 'ok', 'msg' : 'History Alarm'})

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
		# TODO: Verify it works directly from POST method
	"""
	logger.debug('Alarm prices endpoint...')
	params = request.get_json()
	if 'uuids' not in params:
		raise errors.AppError(80004, "UUIDs parameters missing")
	if 'retailers' not in params:
		raise errors.AppError(80004, "Retailers parameters missing")
	if 'today' not in params:
		params['today'] = datetime.datetime.utcnow()
	logger.debug(params)
	prices = Alarm.prices_vs_prior(params)
	return jsonify(prices)

