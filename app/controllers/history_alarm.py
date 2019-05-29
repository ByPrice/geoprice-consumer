# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, logger
from app.models.history_alarm import Alarm
from app.models.task import Task, asynchronize
import datetime

mod = Blueprint('history_alarm',__name__)

@mod.route('/')
def get_alarm_bp():
    """ Alarm route initial endpoint
    """
    logger.info("Alarm route initial endpoint")
    return jsonify({'status': 'ok', 'msg' : 'History Alarm'})

@mod.route('/prices', methods=['POST'])
@asynchronize(Alarm.start_task)
def check_prices_today():
    logger.info("Submited History price alarm task...")
    return jsonify({
        'status':'ok', 
        'module': 'task',
        'task_id' : request.async_id
    })
