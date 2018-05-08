# -*- coding: utf-8 -*-
from . import applogger
from .models.mapa import Mapa
from .models.historia import Historia
import config
import json
import app as flask_app
from app.celery_app import celery_app
from functools import wraps

# Logger
logger = applogger.get_logger()

def with_context(original_function):
    """ Flask Context decorator for inside execution
    """
    @wraps(original_function)
    def new_function(*args,**kwargs):
        # Declare Flask App context
        ctx = flask_app.app.app_context()
        # Init Ctx Stack 
        ctx.push()
        logger.debug('AppContext is been created')
        # Connect db
        flask_app.get_db()
        logger.debug('Added DB!')
        logger.debug('Executing function....')
        original_function(*args,**kwargs)
        # Teardown context
        ctx.pop()
        return True
    return new_function

@celery_app.task(bind=True)
def job_task(self):
    """ Celery task to submit Test Job
    """
    # Fetch Task ID
    task_id = self.request.id
    logger.info('Verifying Task:'+str(task_id))
    # Create Task State File and Update value
    with open('states/{}'.format(task_id), 'w') as _f:
        _f.write(str(0))
    # Execute task
    _t_list = []
    for i in range(1, 101):
        print('Computing {}...'.format(i))
        # Compute Test Operation
        for j in range(1000):
            _t_list.append({str(i*j): j})
        # Save into state file
        with open('states/{}'.format(task_id), 'w') as _f:
            _f.write(str(i))
    


    # Write in redis
    save_status()

    


    # Write Result into dump file
    with open('dumps/{}'.format(task_id), 'w') as _f:
        _f.write(json.dumps(_t_list))
    return {'current': 100, 'total': 100, 'status': 'Task completed!'}

@celery_app.task(bind=True)
@with_context
def prices_map(self, params):
    """ Celery task to execute and obtain all data
        from given filters for Mapping deploy.

        Params:
        -----
        - params : (dict) Retailers, Filters and Exports

        Returns:
        -----
        (dict) Task Finishing Status
    """
    # Get Task ID
    _task_id = self.request.id
    logger.info("Executing Map Task: %s", _task_id)
    # Call Mapa method
    try:
        Mapa.grouped_by_store(_task_id,
                              params['filters'],
                              params['retailers'],
                              params['date_start'],
                              params['date_end'],
                              params['interval']
                              )
    except Exception as e:
        logger.error(e)
        logger.warning('Could not fetch Map prices!')
        return {'progress': 100, 'total': 100, 'status': 'Failed'}
    return {'progress': 100, 'total': 100, 'status': 'Completed'}

@celery_app.task(bind=True)
@with_context
def prices_history(self, params):
    """ Celery task to execute and obtain all data
        from given filters for Historic Deployment

        Params:
        -----
        - params : (dict) Retailers, Filters, Dates and Exports

        Returns:
        -----
        (dict) Task Finishing Status
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

