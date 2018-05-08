"""
Celery app declaration module.
"""
import os
import json
import datetime
import config
from app import applogger
from . import db, errors
from celery import Celery
from celery.signals import worker_process_init, \
    worker_process_shutdown

# Logger
logger = applogger.get_logger()

# Celery configuration
c_config = {}
c_config['CELERY_BROKER_URL'] = 'redis://{host}:{port}/0'\
                                    .format(host=config.CELERY_HOST,
                                            port=config.CELERY_PORT)
c_config['CELERY_RESULT_BACKEND'] = 'redis://{host}:{port}/0'\
                                        .format(host=config.CELERY_HOST,
                                                port=config.CELERY_PORT)
# Initialize Celery
celery_app = Celery(config.APP_NAME, broker=c_config['CELERY_BROKER_URL'])
celery_app.conf.update(c_config)
celery_app.conf.enable_utc = True
celery_app.conf.CELERY_ACCEPT_CONTENT = ['json']
celery_app.conf.CELERY_RESULT_SERIALIZER = 'json'
celery_app.conf.CELERY_TASK_SERIALIZER = 'json'
