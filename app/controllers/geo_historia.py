# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, logger
from app.models.geo_historia import Historia

# Blueprint instance
mod = Blueprint('historia', __name__)


@mod.route('/')
def status():
    """ Geo History blueprint status endpoint
    """
    logger.info("Historia route")
    return jsonify({'status':'ok', 'module': 'Geo Historia'})


@mod.route('/submit/', methods=['POST'])
@mod.route('/submit', methods=["POST"])
def submit_history():
    """ Endpoint to submit task:
        Price by Item IDs and Stores retrieval within a timeframe

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
    logger.info('Submitting Prices History task...')
    # Verify params
    params = request.get_json()
    if not params:
        raise errors.AppError(40002, "Params Missing!", 400)
    if 'filters' not in params:
        raise errors.AppError(40003, "Filters param Missing!", 400)
    if 'retailers' not in params:
        raise errors.AppError(40003, "Retailers param Missing!", 400)
    if 'date_start' not in params:
        raise errors.AppError(40003, "Start Date param Missing!", 400)
    if 'date_end' not in params:
        raise errors.AppError(40003, "End Date param Missing!", 400)
    if 'interval' not in params:
        # In case interval is not explicit, set to day
        params['interval'] = 'day'        
    # Call to submit task
    task = prices_history.apply_async(args=(params,))
    return jsonify({
        'msg': 'Processing...',
        'status': 'ok',
        'task_id': task.id,
        "method": 'prices_history'
        })
