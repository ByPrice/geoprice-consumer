# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, applogger
from app.models.geo_mapa import Map
from app.models.task import Task, asynchronize
import datetime

# Blueprint instance
mod = Blueprint('geo_mapa', __name__)
# Logger
logger = applogger.get_logger()

@mod.route('/')
def status():
    """ Geo Mapa blueprint status endpoint
    """
    logger.info("Geo Mapa Route")
    return jsonify({'status':'ok', 'module': 'Geo Mapa'})


@mod.route('/submit',methods=['POST'])
@asynchronize(Map.start_task)
def submit_map():
    """ Endpoint to submit task:
        Price by Item IDs and Stores retrieval within a timeframe

        Params:
        -----
        - (dict) : Form data with following structure
        >>> {
            "filters": [
                {"item_uuid": "452iub4-54o3iu6b3o4b-46i54362"},
                {"item_uuid": "452iub4-54o3iu6b3o4b-46i54362"},
                {"retailer": "walmart"} 
            ],
            "retailers": {
                "chedraui": "Chedraui",
                "walmart": "Walmart",
            },
            "date_start": "2017-10-01",
            "date_end": "2018-01-12",
            "interval": "day" // "month", "week" or "day"
        }

        Returns:
        -----
        - (JSON)  Status of the submitted task
        >>> {
            "status": "IN PROGRESS",
            "progress": 0,
            "response_type": "json",
            "task_id": "234534h56uip-245n234ob65h-2435",
            "method": "prices_history"
        }
    """
    logger.info("Submited Geo Map task...")
    return jsonify({
        'status': 'ok', 
        'module': 'geo_mapa',
        'task_id' : request.async_id
    })