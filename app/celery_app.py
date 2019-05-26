"""
Celery app declaration module.
"""
import os
import json
import datetime
import config
from ByHelpers import applogger
from app.utils.errors import TaskError
from app.models.task import Task
import app as geoprice
from functools import wraps
from . import db, errors
from celery import Celery
from celery.task.control import revoke
from celery.signals import worker_process_init, \
    worker_process_shutdown

# Logger
logger = applogger.get_logger()

# Celery configuration
c_config = {}

broker_url = 'redis://:{password}@{host}:{port}/{db_number}'\
    .format(
        password=config.CELERY_PASSWORD,
        host=config.CELERY_HOST,
        port=config.CELERY_PORT,
        db_number=config.CELERY_REDIS_DB
    )
result_backend_url = 'redis://:{password}@{host}:{port}/{db_number}'\
    .format(
        password=config.CELERY_PASSWORD,
        host=config.CELERY_HOST,
        port=config.CELERY_PORT,
        db_number=config.CELERY_REDIS_DB
    )
    
# Initialize Celery
celery_app = Celery(
    config.APP_NAME, 
    broker=broker_url, 
    backend=result_backend_url
)
celery_app.conf.update(c_config)
celery_app.conf.enable_utc = True
celery_app.conf.accept_content = ['json','pickle']
celery_app.conf.result_serializer = 'json'
celery_app.conf.task_serializer = 'json'

def with_context(original_function):
    """ Flask Context decorator for inside execution
    """
    @wraps(original_function)
    def new_function(*args,**kwargs):
        # Declare Flask App context
        ctx = geoprice.app.app_context()
        # Init Ctx Stack 
        ctx.push()
        logger.debug('AppContext is been created')
        # Connect db
        geoprice.build_context()
        logger.debug('Connected to redis')
        original_function(*args,**kwargs)
        # Teardown context
        ctx.pop()
        return True
    return new_function


@celery_app.task(bind=True, serializer='pickle')
@with_context
def main_task(self, func, params):
    # Fetch Task ID
    task_id = self.request.id
    logger.info('Verifying Task:'+str(task_id))
    logger.info('Received params: {}'.format(params))

    # Get the state
    task = Task(task_id)
    task.progress = 0

    # Execute the passed function
    print("Starting task...")
    try:
        result = func(task_id,params)
        print("Got result from task...")
    except TaskError as te:
        logger.error(te)
        task.progress = -1
        revoke(task_id, terminate=True)
        raise Exception("Couldn't complete task...")
    except Exception as e:
        logger.error(e)
        task.progress = -1
        revoke(task_id, terminate=True)
        raise Exception("Couldn't complete task...")

    # Setting result
    task.progress=100
    task.result = result


@celery_app.task(bind=True)
@with_context
def test_task(self, params):
    """ Celery test task
    """
    # Fetch Task ID
    task_id = self.request.id
    logger.info('Verifying Task:'+str(task_id))
    logger.info('Received params: {}'.format(params))

    # Get the state
    task = Task(task_id)
    task.progress = 0

    # Make an operation
    import time
    time.sleep(2)
    logger.info('Processing task')
    task.progress = 50
    
    # Save result
    time.sleep(10)
    logger.info('Getting task result')
    task.progress = 100
    task.result = {
        "msg" : "Task completed successfully",
        "data" : [ "Default text" for i in range(1000) ]
    }