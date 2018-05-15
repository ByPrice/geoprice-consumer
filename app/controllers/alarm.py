# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app.models.alarm import Alarm
from app import errors, logger
import datetime

mod = Blueprint('alarm',__name__)

@mod.route('/')
def get_alarm_bp():
    """ Alarm route initial endpoint
    """
    logger.info("Alarm route initial endpoint")
    return jsonify({'msg' : 'Alarm Route'})