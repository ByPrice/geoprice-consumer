# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
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
def get_promos_by_day():
    """
		Return the promos applying that day, limited by the parameters
		Params:
		{
			'day' : '2017-09-20'
			'ip' : 1,
			'ipp' : 500
		}
	"""
	
    if not request.args:
        raise errors.AppError(80002,"No params in request")
    if 'day' not in request.args:
        raise errors.AppError(80004, "Day params missing")
    else:
        try:
            datetime.datetime.strptime(request.args['day'], '%Y-%m-%d')
        except:
            raise errors.AppError(80012, "Incorrect data format, should be YYYY-MM-DD")
    if 'ip' not in request.args or int(request.args['ip']) < 1:
        ip = 1
    else:
        ip = int(request.args['ip'])
    if 'ipp' not in request.args or int(request.args['ipp']) < 1:
        ipp = 500
    else:
        ipp = int(request.args['ipp'])    
    
    logger.info("Promos by day endpoint "+request.args['day']+", "+str(ip)+", "+str(ipp))
    promos = Promos.get_cassandra_promos_by_day(request.args['day'], ip, ipp)
    return jsonify(promos)
