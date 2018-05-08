import datetime
import json
from config import *
import requests
import re
from uuid import uuid1 as UUID
import app as flask_app
from flask import g
from functools import wraps
from app.utils import db, applogger, errors
from app.utils.rabbit_engine import RabbitEngine
import sys

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
        # Connect db
        flask_app.get_db()
        original_function(*args,**kwargs)
        # Teardown context
        ctx.pop()
        return True
    return new_function

def connect():
    global consumer, producer
    # Connect to consumer
    consumer = RabbitEngine(config={
        'queue':QUEUE_GEOPRICE, 
        'routing_key':QUEUE_GEOPRICE
    },blocking=False)
    try:
        # Producer to price-cache
        producer = RabbitEngine({
            'queue': QUEUE_CACHE, 
            'routing_key': QUEUE_CACHE
        },blocking=True)
    except Exception as e:
        logger.error("Coud not connect to rabbit producer!")
        logger.error(e)


# Rabbit MQ callback function
def callback(ch, method, properties, body):
    global producer
    try:
        new_price = json.loads(body.decode('utf-8'))
        logger.debug("Price "+new_price['retailer']+" - "+new_price['item_uuid']+" - stores: "+ str(len(new_price['location']['store'])))
        # Valuamos las variables recibidas para verificar que tenga todos los datos
        if not Price.validate(new_price):
            logger.debug('Could not validate price')
            pass
        else:
            price = Price(new_price)
            price.save_all()
            logger.info('Saved price for ' + price.retailer + ' ' + str(price.item_uuid))
            # Publish message to price-cache
            if producer:
                producer.publish_message(QUEUE_CACHE, new_price)
    except NoHostAvailable as e:
        logger.error("No Cassandra host available, shutting down...")
        logger.error(e)
        sys.exit()
    except Exception as e:
        logger.error(e)
    ch.basic_ack(delivery_tag = method.delivery_tag)


@with_context
def start():
    global producer, consumer
    connect()
    consumer.set_callback(callback)
    logger.info("Callback set for rabbitmq consumer")
    consumer.run()
