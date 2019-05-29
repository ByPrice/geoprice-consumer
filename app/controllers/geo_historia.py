# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, logger
from app.models.geo_historia import Historia
from app.models.task import Task, asynchronize

# Blueprint instance
mod = Blueprint('geo_historia', __name__)

@mod.route('/')
def status():
    """ Geo History blueprint status endpoint
    """
    logger.info("Historia route")
    return jsonify({'status':'ok', 'module': 'Geo Historia'})


@mod.route('/submit', methods=['POST'])
@asynchronize(Historia.filters_task)
def submit_history():
    """ Endpoint to submit task:
        Price by Item UUIDs and Stores retrieval within a timeframe

        Params:
        -----
        - (dict) : Form data with following structure
        >>> {
            "filters": [
                {"item": "452iub4-54o3iu6b3o4b-46i54362"},
                {"item": "452iub4-54o3iu6b3o4b-46i54362"},
                {"store": "452iub4-54o3iu6b3o4b-46i54362"},
                {"store": "452iub4-54o3iu6b3o4b-46i54362"}
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
    logger.info('Submitting Geo History task...')  
    # Call to submit task
    return jsonify({
        'status': 'ok',
        'task_id': request.async_id,
        "method": 'geo_historia'
        })
