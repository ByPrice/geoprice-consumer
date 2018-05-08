# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, logger
import datetime

mod = Blueprint('task',__name__)

@mod.route('/start', methods=['POST'])
def task_start():
    """ Endpoint to post a new task
    """
    # Submit job task
    task = job_task.apply_async()
    return jsonify({
        'task_id':task.id,
        'status': 'RUNNING',
        'progress': 0,
        'msg': 'Processing...'
        }), 202

@app.route('/status/<task_id>', methods=['GET'])
def task_status(task_id):
    """ Endpoint to consult tasks status

        Query Params:
        -----
        - m : (str) Task Method
    """
    if 'm' in request.args:
        meth = request.args.get('m')
    else:
        meth = 'job_task'
    # Query task status from Result backend
    task = eval("{}.AsyncResult('{}')".format(meth, task_id))
    # In case of failure build Bad response
    if task.state == 'FAILURE':
        # something went wrong in the background job
        return jsonify({
            'status': task.state,
            'progress': 100,
            'task_id': task.id,
            'msg': str(task.info),  # this is the exception raised
        }), 200
    else:
        try:
            with open('states/{}'.format(task_id), 'r') as _f:
                _prog = int(_f.read())
        except Exception as ex_:
            logger.error(ex_)
            _prog = 0
        logger.info('Progress: '+str(_prog))
        return jsonify({
            'task_id':task.id,
            'status': task.state,
            'progress': _prog,
            'msg': 'Processing...' if str(_prog) != '100' else 'Processed!'
        })