# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, Response, stream_with_context
from app import logger
from app.utils.errors import TaskError, AppError
import datetime
from app.models.task import Task, asynchronize
import importlib
from app.celery_app import *
from celery.task.control import revoke
import time

mod = Blueprint('task',__name__)

@mod.route('/')
def main():
    return jsonify({'status':'ok', 'module': 'task'})


@mod.route('/start/<task_name>', methods=['POST'])
def task_start(task_name):
    """ Endpoint to post a new task
        @Params:
            - params
    """
    logger.info("Starting task ...")
    try:
        # Get method to be executed from board module
        action = getattr(
            importlib.import_module(
                "app.celery_app"
            ), 
            "{}_task".format(task_name)
        )
    except Exception as e:
        logger.error(e)
        raise AppError("Invalid task", 501)   

    params = request.get_json()
    if not params:
        raise Exception("Invalid params!")

    try:
        # Start async task
        celery_task = action.apply_async(args=(params,))
    except Exception as e:
        logger.error("Error starting the task")
        logger.error(e)
        raise AppError('task_error',e)

    # Set task name to backend
    task = Task(celery_task.id)
    task.name = task_name
    task.progress = 0

    return jsonify({
        'task_id':celery_task.id,
        'msg': 'Celery Task started',
        'text': 'RUNNING'
    }), 202


@mod.route('/status/<task_id>', methods=['GET'])
def task_status(task_id):
    """ Endpoint to consult tasks status by task_id
    """
    logger.info("Consulting task status ...")
    task = Task(task_id)
    if not task.status:
        raise AppError("task_error","Task not found!")

    _status = task.status
    _status['celery_status'] = None
    try:
        logger.info("Getting result")
        celery_task = celery_app.AsyncResult(task_id)
        #celery_task = eval("{}_task.AsyncResult('{}')".format(task.name, task_id))
        _status['celery_status'] = celery_task.state        
    except Exception as e:
        logger.error(e)
    return jsonify(_status)


@mod.route('/result/stream/<task_id>', methods=['GET'])
@mod.route('/result/<task_id>', methods=['GET'])
def task_result(task_id):
    """ Endpoint to query tasks result
    """
    logger.info("Retrieving task result ...")
    # Check if streaming response is requested
    rule = str(request.url_rule)
    logger.debug(rule)
    stream = True if 'stream' in rule else False
    # Getting task status
    task = Task(task_id)
    if not task.status:
        raise AppError("task_error","Task not found!")

    data = None
    if hasattr(task,'result'):
        data = task.result['data'] if 'data' in task.result else None

    resp = {
        "status" : task.status ,
        "result" : data
    }
    if stream:
        # Generator
        string_resp = json.dumps(resp)
        generate = lambda: [ (yield x) for x in string_resp]
        return Response(
            stream_with_context(
                generate()
            ), 
            content_type='application/json'
        )
    else:
        return jsonify(resp)


@mod.route('/cancel/<task_id>', methods=['GET'])
def task_cancel(task_id):
    """ Endpoint to consult tasks status by task_id
    """
    logger.info("Cancelling task ...")
    task = Task(task_id)
    if not task.status:
        raise AppError("task_error","Task not found!")
    try:
        logger.info("Getting result")
        celery_task = celery_app.AsyncResult(task_id)    
    except Exception as e:
        logger.error(e)
    if celery_task.state == 'SUCCESS' or celery_task.state == 'FAILURE':
        return jsonify(task.status)
    # Celery status
    try:
        logger.info("Getting result")
        task.progress = -1
        revoke(task_id, terminate=True)
    except Exception as e:
        logger.error(e)
    logger.debug(task.status)
    logger.debug(celery_task.state)
    status = task.status
    status['celery_status'] = celery_task.state

    return jsonify(status)

 