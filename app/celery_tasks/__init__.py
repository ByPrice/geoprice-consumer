# -*- coding: utf-8 -*-
import json
import datetime
import os
from app.utils import applogger
from app.models.task import Task
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
    time.sleep(2)
    logger.info('Getting task result')
    task.progress = 100
    task.result = {
        "msg" : "Task completed successfully",
        "data" : [ "Default text" for i in range(1000) ]
    }
     

@celery_app.task(bind=None)
@with_context
def price_map(self, params):
    """ Async task for getting prices_map
    """
    from . import price_map
    # Get Task ID
    _task_id = self.request.id
    logger.info("Executing Map Task: %s", _task_id)

    if not params:
        raise errors.AppError(40002, "Params Missing!", 400)
    if 'filters' not in params:
        raise errors.AppError(40003, "Filters param Missing!", 400)
    if 'retailers' not in params:
        raise errors.AppError(40003, "Retailers param Missing!", 400)
    if 'date_start' not in params:
        params['date_start'] = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    if 'date_end' not in params:
        params['date_end'] = datetime.date.today().isoformat()
    if 'interval' not in params:
        params['interval']
        
    try:
        prices_map.run(
            _task_id,
            params['filters'],
            params['retailers'],
            params['date_start'],
            params['date_end'],
            params['interval']
        )
    except Exception as e:
        logger.error(e)
        logger.warning('Could not fetch Map prices!')
        return False

    return True
    


@celery_app.task(bind=True)
@with_context
def price_history(self, params):
    """ Async task for geting price history 
        - Create new task
        - Pass parameters
    """
    from . import price_history
    # Get Task ID
    _task_id = self.request.id
    logger.info("Executing Map Task: %s", _task_id)

    # Params validation
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

    # Call Historia method
    try:
        price_history.run(
            _task_id,
            params['filters'],
            params['retailers'],
            params['date_start'],
            params['date_end'],
            params['interval']
        )
    except Exception as e:
        logger.error(e)
        logger.warning('Could not fetch Historic prices!')
        return False

    return True
