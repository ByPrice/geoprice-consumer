# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response, stream_with_context
from app.models.promos import Promos
from app import errors, logger
import datetime

mod = Blueprint('promos',__name__)

@mod.route('/')
def main():
    """ Service information endpoint
    """
    return jsonify({
        'service' : 'ByPrice Promos v1',
        'author' : 'ByPrice Dev Team',
        'date' : datetime.datetime.utcnow()
    })

@mod.route('/daily', methods=['GET'])
def generate_promos_by_day_stream():
    """
		Return the promos applying that day, limited by the parameters
		Params:
		{
			'day' : '2017-09-20'
			'num_promos' : 20
		}
	"""
    logger.info("Promos daily stream endpoint!")
    if not request.args:
        raise errors.AppError(80002,"No params in request")
    if 'day' not in request.args:
        raise errors.AppError(80004, "Day params missing")
    else:
        try:
            datetime.datetime.strptime(request.args['day'], '%Y-%m-%d')
        except:
            raise errors.AppError(80012, "Incorrect data format, should be YYYY-MM-DD")
    if 'num_promos' not in request.args or int(request.args['num_promos']) < 1:
        num_promos = 0
    else:
        num_promos = int(request.args['num_promos'])

    def generate():
        return Promos.get_cassandra_promos_by_day(request.args['day'], num_promos)
    return Response(stream_with_context(generate()), content_type='application/json')
