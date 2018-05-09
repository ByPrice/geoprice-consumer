# -*- coding: utf-8 -*-
import os
import re

__version__ = '0.1'

APP_MODE = os.getenv('APP_MODE','SERVICE')
APP_NAME='geolocation-'+APP_MODE.lower()
APP_PORT = os.getenv('APP_PORT', 8000)
APP_SECRET = os.getenv('APP_SECRET', '#geolocation')

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BASEDIR = BASE_DIR
PATH = os.path.dirname(os.path.realpath(__file__)) + "/"

# Celery
CELERY_BROKER = os.getenv('CELERY_BROKER', 'redis')
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
CASSANDRA_PORT = os.getenv('CASSANDRA_PORT','9042')

# Env-dependent variables
ENV = os.getenv('ENV','DEV')
CASSANDRA_KEYSPACE=CASSANDRA_KEYSPACE+"_dev" if ENV.upper() == 'DEV' else CASSANDRA_KEYSPACE

# App Name
TESTING=False

# Logging and remote logging
LOG_LEVEL = os.getenv('LOG_LEVEL', ('DEBUG' if ENV != 'PRODUCTION' else 'DEBUG'))
LOG_HOST = os.getenv('LOG_HOST', 'logs5.papertrailapp.com')
LOG_PORT = os.getenv('LOG_PORT', 27971)

# Consumer vars
STREAMER = os.getenv('STREAMER', 'rabbitmq')
STREAMER_HOST = os.getenv('STREAMER_HOST', 'localhost')
STREAMER_PORT = os.getenv('STREAMER_PORT', '')
STREAMER_EXCHANGE = os.getenv('STREAMER_EXCHANGE', 'data')
STREAMER_EXCHANGE_TYPE = os.getenv('STREAMER_EXCHANGE_TYPE', 'direct')

# Rabbit queues
QUEUE_ROUTING = "routing_dev" if ENV.upper() == 'DEV' else "routing"
QUEUE_GEOPRICE = 'geoprice_dev' if ENV.upper() == 'DEV' else 'geoprice'
QUEUE_CACHE = "cache_dev" if ENV.upper() == 'DEV' else "cache"

# Cassandra seeds
contact_points = []
for contact_points in CASSANDRA_CONTACT_POINTS.split(","):
    if not re.match(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b',seed) and ENV.upper() != 'DEV' and ENV.upper() != 'LOCAL':
        contact_points.append("dev."+seed)
    else:
        contact_points.append(seed)
CASSANDRA_CONTACT_POINTS=contact_points

# Services
SRV_PROTOCOL = os.getenv('SRV_PROTOCOL', 'http')
SRV_CATALOGUE = SRV_PROTOCOL + "://" + ('dev.' if ENV == 'DEV' else '')  + os.getenv('SRV_CATALOGUE', 'catalogue')
SRV_GEOLOCATION = SRV_PROTOCOL + "://" + ('dev.' if ENV == 'DEV' else '') + os.getenv('SRV_GEOLOCATION', 'geolocation')










