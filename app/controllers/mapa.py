# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, applogger
from app.models.mapa import Map
from app.models.task import Task, asynchronize
import datetime

# Blueprint instance
mod = Blueprint('mapa', __name__)
# Logger
logger = applogger.get_logger()

@mod.route('/')
def status():
    """ Mapa blueprint status endpoint
    """
    return jsonify({'status':'ok', 'module': 'mapa'})


@mod.route('/submit',methods=['POST'])
@asynchronize(Map.start_task)
def map():
    logger.info("Submited Map task...")
    return jsonify({
        'status':'ok', 
        'module': 'task',
        'task_id' : request.async_id
    })