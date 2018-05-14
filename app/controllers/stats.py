# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, logger
import datetime

mod = Blueprint('stats',__name__)

@mod.route('/')
def get_stats():
    """ Stats route initial endpoint
    """
    logger.info("Stats route initial endpoint")
    #prod = Product.get_one()
    #if not prod:
    #    raise errors.AppError("invalid_request", "Could not fetch data from Cassandra")
    return jsonify({'ok' : 'BP running'})