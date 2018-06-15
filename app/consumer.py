import datetime
import json
from config import *
import requests
import re
from uuid import uuid1 as UUID
import app as geoprice
from flask import g
from functools import wraps
from app.utils import db, applogger, errors
from app.utils.rabbit_engine import RabbitEngine
import sys

logger = applogger.get_logger()

q_geoprice = QUEUE_GEOPRICE
q_cache = QUEUE_CACHE


def with_context(original_function):
    """ Flask Context decorator for inside execution
    """
    @wraps(original_function)
    def new_function(*args,**kwargs):
        # Declare Flask App context
        ctx = geoprice.app.app_context()
        # Init Ctx Stack 
        ctx.push()
        # Connect db
        geoprice.build_context(
            queue_consumer=q_geoprice, 
            queue_producer=q_cache
        )
        original_function(*args,**kwargs)
        # Teardown context
        ctx.pop()
        return True
    return new_function


# Rabbit MQ callback function
def callback(ch, method, properties, body):
    try:
        new_price = json.loads(body.decode('utf-8'))
        logger.debug("Price "+new_price['retailer']+" - "+new_price['product_uuid']+" - stores: "+ str(len(new_price['location']['store'])))
        # Valuamos las variables recibidas para verificar que tenga todos los datos
        if not Price.validate(new_price):
            logger.debug('Could not validate price')
            pass
        else:
            price = Price(new_price)
            price.save_all()
            logger.info('Saved price for ' + price.retailer + ' ' + str(price.product_uuid))
            # Publish message to price-cache
            if g._producer[q_cache]:
                g._producer[q_cache].publish_message(q_cache, new_price)
    except NoHostAvailable as e:
        logger.error("No Cassandra host available, shutting down...")
        logger.error(e)
        sys.exit()
    except Exception as e:
        logger.error(e)
    ch.basic_ack(delivery_tag=method.delivery_tag)


@with_context
def start():
    g._consumer[q_geoprice].set_callback(callback)
    logger.info("Callback set for rabbitmq consumer")
    g._consumer[q_geoprice].run()
