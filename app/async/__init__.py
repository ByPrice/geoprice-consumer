# -*- coding: utf-8 -*-
import json
import datetime
import os
from app.utils import applogger
from app.models.task import Task, TASK_STAGE
import config
import app as geoprice
from celery import Celery
from functools import wraps
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
        db_number=config.REDIS_DB
    )
result_backend_url = 'redis://:{password}@{host}:{port}/{db_number}'\
    .format(
        password=config.CELERY_PASSWORD,
        host=config.CELERY_HOST,
        port=config.CELERY_PORT,
        db_number=config.REDIS_DB
    )
c_config['celery_broker_url'] = broker_url
c_config['celery_result_backend'] = result_backend_url

# Initialize Celery
celery_app = Celery(config.APP_NAME, broker=c_config['celery_broker_url'])
celery_app.conf.update(c_config)
celery_app.conf.enable_utc = True
celery_app.conf.celery_accept_content = ['json']
celery_app.conf.celery_result_serializer = 'json'
celery_app.conf.celery_task_serializer = 'json'


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
        geoprice.get_db()
        geoprice.get_redis()
        logger.debug('Connected to cassandra and redis')
        original_function(*args,**kwargs)
        # Teardown context
        ctx.pop()
        return True
    return new_function


@celery_app.task(bind=True)
def test_task(self):
    """ Celery test task
    """
    # Fetch Task ID
    task_id = self.request.id
    logger.info('Verifying Task:'+str(task_id))

    # Get the state
    task = Task()
    task.progress = 0

    # Make an operation
    import time
    time.sleep(2)
    task.progress = 50
    
    # Save result
    time.sleep(2)
    task.progress = 100
    task.result = {
        "msg" : "Task completed successfully",
        "data" : [ "Default text" for i in range(1000) ]
    }
     

@celery_app.task(bind=None)
def get_price_map(self):
    """ Async task for getting prices_map
    """
    from . import price_map
    price_map.run()
    

@celery_app.task(bind=True)
@with_context
def prices_history(self, params):
    """ Async task for geting price history 
    """
    # Get Task ID
    _task_id = self.request.id
    logger.info("Executing Historic Task: %s", _task_id)
    # Call Historia method
    try:
        Historia.grouped_by_retailer(_task_id,
                            params['filters'],
                            params['retailers'],
                            params['date_start'],
                            params['date_end'],
                            params['interval'])
    except Exception as e:
        logger.error(e)
        logger.warning('Could not fetch Historic prices!')
        return {'progress': 100, 'total': 100, 'status': 'Failed'}
    return {'progress': 100, 'total': 100, 'status': 'Completed'}
