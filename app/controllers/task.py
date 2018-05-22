# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response
from app import errors, logger
import datetime
from app.models.task import Tasks

mod = Blueprint('task',__name__)

@mod.route('/start/<task_name>', methods=['POST'])
def task_start(task_name):
    """ Endpoint to post a new task
        @Params:
            - params
    """
    try:
        params = request.get_json()
        exec("form celery_tasks import "+task_name+" as task_name")
        task = task_name.apply_async(args=(params,))

    except Exception as e:
        logger.error("Error starting the task")
        logger.error(e)

    return jsonify({
        'task_id':task.id,
        'msg': 'Task started',
        'text': 'RUNNING'
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

