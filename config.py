# -*- coding: utf-8 -*-
import os
import re

__version__ = '0.1'

APP_MODE = os.getenv('APP_MODE','SERVICE')
APP_NAME='geoprice-'+APP_MODE.lower()
APP_PORT = os.getenv('APP_PORT', 8000)
APP_SECRET = os.getenv('APP_SECRET', '#geoprice')
ENV = os.getenv('ENV','DEV')

# Testing
TESTING= True if os.getenv('TESTING',False) else False

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BASEDIR = BASE_DIR
PATH = os.path.dirname(os.path.realpath(__file__)) + "/"

# Celery
CELERY_BROKER = os.getenv('CELERY_BROKER', 'rabbitmq')
CELERY_HOST = os.getenv("CELERY_HOST", "localhost")
CELERY_PORT = int(os.getenv("CELERY_PORT", 6379))
CELERY_USER =  os.getenv('CELERY_USER', '')
CELERY_PASSWORD = os.getenv('CELERY_PASSWORD','')

TASK_BACKEND = os.getenv('TASK_BACKEND',None)
REDIS_HOST = os.getenv('REDIS_HOST', None)
REDIS_PORT = os.getenv('REDIS_PORT', None)
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_DB = 0 if ENV != 'DEV' else 1

# Database
CASSANDRA_CONTACT_POINTS =  os.getenv('CASSANDRA_CONTACT_POINTS','0.0.0.0')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE','prices')
CASSANDRA_KEYSPACE2 = os.getenv('CASSANDRA_KEYSPACE','stats')
CASSANDRA_PORT = os.getenv('CASSANDRA_PORT','9042')
CASSANDRA_USER = os.getenv('CASSANDRA_USER','')
CASSANDRA_PASSWORD = os.getenv('CASSANDRA_PASSWORD','')

# Env-dependent variables
if not TESTING:
    CASSANDRA_KEYSPACE=CASSANDRA_KEYSPACE+"_dev" if ENV.upper() in ['DEV','LOCAL'] else CASSANDRA_KEYSPACE
else:
    CASSANDRA_KEYSPACE=CASSANDRA_KEYSPACE+"_test"

# Logging and remote logging
LOG_LEVEL = os.getenv('LOG_LEVEL', ('DEBUG' if ENV in ['DEV','LOCAL'] else 'DEBUG'))
LOG_HOST = os.getenv('LOG_HOST', 'logs5.papertrailapp.com')
LOG_PORT = os.getenv('LOG_PORT', 27971)

# Consumer vars
STREAMER = os.getenv('STREAMER', 'rabbitmq')
STREAMER_HOST = os.getenv('STREAMER_HOST', 'localhost')
STREAMER_PORT = os.getenv('STREAMER_PORT', '')
STREAMER_EXCHANGE = os.getenv('STREAMER_EXCHANGE', 'data')
STREAMER_EXCHANGE_TYPE = os.getenv('STREAMER_EXCHANGE_TYPE', 'direct')

# Rabbit queues
QUEUE_ROUTING = "bp_routing_dev" if ENV.upper() in ['DEV', 'LOCAL'] else "bp_routing"
QUEUE_GEOPRICE = 'bp_geoprice_dev' if ENV.upper() in ['DEV', 'LOCAL'] else 'bp_geoprice'
QUEUE_CACHE = "bp_cache_dev" if ENV.upper() in ['DEV', 'LOCAL'] else "bp_cache"

# Split contact points
CASSANDRA_CONTACT_POINTS = CASSANDRA_CONTACT_POINTS.split(",")

# Services
SRV_PROTOCOL = os.getenv('SRV_PROTOCOL', 'http')
SRV_CATALOGUE = SRV_PROTOCOL + "://" + ('dev.' if ENV in ['DEV', 'LOCAL'] else '')  + os.getenv('SRV_CATALOGUE', 'catalogue')
SRV_GEOLOCATION = SRV_PROTOCOL + "://" + ('dev.' if ENV in ['DEV', 'LOCAL'] else '') + os.getenv('SRV_GEOLOCATION', 'geolocation')

# Tasks arguments 
TASK_ARG_CREATE_DUMPS = os.getenv('TASK_ARG_CREATE_DUMPS', 'byprice,ims,walmart') 

# AWS keys to access S3
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '') 
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY','')








