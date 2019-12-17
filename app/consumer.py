import datetime
import json
from config import *
import requests
from cassandra.cluster import NoHostAvailable
import re
from uuid import uuid1 as UUID
import app as geoprice
from flask import g
from functools import wraps
from app.utils import db, errors
from ByHelpers import applogger
from app.models.price import Price
import sys
from time import time

logger = applogger.get_logger()

q_geoprice = QUEUE_GEOPRICE
q_cache = QUEUE_CACHE
# Global counter
gcounter = 1


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
    t0 = time()
    global gcounter
    try:

        new_price = json.loads(body.decode('utf-8'))
        logger.debug("Price "+new_price['retailer']+" - "+new_price['product_uuid']+" - stores: "+ str(len(new_price['location']['store'])))
        # Valuamos las variables recibidas para verificar que tenga todos los datos
        if not Price.validate(new_price):
            logger.warning('Could not validate price')
            pass
        else:
            t1 = time()
            price = Price(new_price)
            # Set Partition value
            price.part = gcounter
            # Save elements
            price.save_all()
            t2 = time()
            logger.info('Price() and save_all = {}'.format(str(t2-t1)))
            logger.info('Saved price for ' + price.retailer + ' ' + str(price.product_uuid))
            # Publish message to price-cache
            if q_cache in g._producer and  g._producer[q_cache]:
                g._producer[q_cache].send(new_price)
                # Modify partition to distribute
                if gcounter >= 20:
                    gcounter = 1
                else:
                    gcounter += 1
            else:
                logger.warning("Producer not initialized!")
                logger.error(g._producer)
    except NoHostAvailable as e:
        logger.error("No Cassandra host available, shutting down...")
        logger.error(e)
        sys.exit()
    except Exception as e:
        logger.error(e)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    t4 = time()
    logger.info('Total callback time = {}'.format(str(t4 - t0)))



@with_context
def start():
    g._consumer[q_geoprice].set_callback(callback)
    logger.info("Callback set for rabbitmq consumer")
    g._consumer[q_geoprice].run()
