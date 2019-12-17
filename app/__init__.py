# -*- coding: utf-8 -*-
import os
from flask import Flask, request, jsonify, g
from flask_cors import CORS
import click
import json
import config
from config import *
from config import BASEDIR
import datetime
from ByHelpers import applogger
import app.utils.errors as errors
import app.utils.db as db
if APP_MODE == 'CONSUMER':
    from ByHelpers.rabbit_engine import RabbitEngine
from redis import Redis

# Flask app declaration
app = Flask(__name__)
CORS(app)
applogger.create_logger()
logger = applogger.get_logger()


def build_context(
    services=None,
    queue_consumer=None,
    queue_producer=None 
    ):
    """ App method to setup global
        variables for app context
    """
    get_db()
    get_redis()
    get_sdks(services)
    if APP_MODE == 'CONSUMER':
        get_consumer(queue=queue_consumer)
        get_producer(queue=queue_producer)


def get_db():
    """ Method to connect to C*
    """
    try:
        if not hasattr(g, '_db'):
            g._db = db.getdb()
    except Exception as e:
        logger.error("Could not connect to Database!!")
        logger.error(e)


def get_redis():
    """ Method to connect to redis
    """
    try:
        if not hasattr(g, '_redis') and config.TASK_BACKEND=='redis':
            g._redis = Redis(
                db=config.REDIS_DB,
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                password=config.REDIS_PASSWORD or None
            )
    except Exception as e:
        logger.error("Could not connect to redis server!!")
        logger.error(e)


def get_sdks(modules):
    """ Method build service SDKs
    """
    if modules is None:
        return False
    # Import geolocation
    if 'geolocation' in modules:
        from app.utils.geolocation import Geolocation
        if not hasattr(g, '_geolocation'):
            g._geolocation = Geolocation(
                uri=config.SRV_GEOLOCATION,
                protocol=config.SRV_PROTOCOL
            )
    # Import catalogue
    if 'catalogue' in modules:
        from app.utils.catalogue import Catalogue
        if not hasattr(g, '_catalogue'):
            g._catalogue = Catalogue(
                uri=config.SRV_CATALOGUE,
                protocol=config.SRV_PROTOCOL
            )


def get_consumer(queue=None):
    """ App method to connect to rabbit consumer
    """
    try:
        if not hasattr(g, "_consumer"):
            g._consumer = {}
        if queue != None and queue not in g._consumer:
            g._consumer[queue] = RabbitEngine(config={
                'queue': queue, 
                'routing_key': queue
            }, blocking=False)
            logger.debug("Init Consumer..")
    except Exception as e:
        logger.error("Could not connect to rabbitmq consumer!!")
        logger.error(e)


def get_producer(queue=None):
    """ App method to connect to rabbit consumer
    """
    try:
        if not hasattr(g, "_producer"):
            g._producer = {}
        if queue != None and queue not in g._producer:
            g._producer[queue] = RabbitEngine(config={
                'queue': queue, 
                'routing_key': queue
            }, blocking=True)
            logger.debug("Init Producer..")
    except Exception as e:
        logger.error("Could not connect to rabbitmq producer!!")
        logger.error(e)


@app.before_request
def before_request():
    """ Before request method
    """
    # Connect to database
    build_context(services=['geolocation', 'catalogue'])

@app.teardown_appcontext
def close_db(error):
    ''' 
        Close connection at the end of every request
    '''
    logger.debug("Teardown Method")
    gdb = getattr(g, '_db', None)
    if gdb is not None:
        gdb.close()  

@app.cli.command('initdb')
def initdb_cmd():
    """ Creates db from cli 
    """
    db.initdb()
    logger.info("Initialized database")

# Consumer command
@app.cli.command('consumer')
def consumer_cmd():
    """ Execute app consumer
    """
    from app.consumer import start
    start()
    logger.info("Initialized database")

@app.cli.command('script')
@click.option('--name', default=None, help="Provide the task name with the option --name=<script>")
def dump_cmd(name):
    """ Execute script by it's name
    """
    if not name:
        logger.error("You must define the name of the script to be executed")
        return False
    from app.scripts import start_script
    start_script(name)

@app.cli.command('stats_retailer')
@click.option('--date', default='', help="IsoFormat Date to run stats --date=<date>")
@click.option('--rets', default='', help="Comma separated retailer keys --rets=<rets>")
def stats_by_ret_cmd(date, rets):
    """ Execute Stats of given retailers and date
    """
    if not rets:
        logger.error("Must define Retailers")
        return False
    if not date:
        logger.error("Must define date")
        return False
    from app.scripts.create_stats import retailers_start
    retailers_start(date, rets)

@app.cli.command('backups_retailer')
@click.option('--date', default='', help="IsoFormat Date to run stats --date=<date>")
@click.option('--rets', default='', help="Comma separated retailer keys --rets=<rets>")
def backups_by_ret_cmd(date, rets):
    """ Execute Backups of given retailers and date
    """
    if not rets:
        logger.error("Must define Retailers")
        return False
    if not date:
        logger.error("Must define date")
        return False
    from app.scripts.create_backups import retailers_start
    retailers_start(date, rets)

# Functional Endpoints
@app.route('/')
def main():
    """ Service information endpoint
    """
    return jsonify({
        'service' : 'ByPrice Price Geoprice v{}'.format(config.__version__),
        'author' : 'ByPrice Dev Team',
        'date' : datetime.datetime.utcnow()
    })


# Error Handlers
@app.errorhandler(404)
def not_found(error):
    """ Not Found Error Handler
    """
    logger.debug(error)
    return jsonify({
        "msg": "Incorrect Path",
        "error": 40004
    }), 400

@app.errorhandler(400)
def bad_request(error):
    """ HTTP Error handling
    """
    return jsonify({
        "error": 40000,
        "msg": "Bad Request"
    }), 400

# API errors
@app.errorhandler(errors.AppError)
def handle_api_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


# Flask controllers imports

#from app.controllers import product, stats, alarm, dump, promos, geo_alert
from app.controllers import geo_mapa, task, history_product, geo_alert, geo_check, geo_dump,\
                        geo_historia, history_alarm, stats

# Flask blueprint registration+
# Register Mapa module
app.register_blueprint(geo_mapa.mod, url_prefix='/geo/mapa')
# Register Task module
app.register_blueprint(task.mod, url_prefix='/task')
# Register Stats module
app.register_blueprint(stats.mod, url_prefix='/stats')
# Register History Alarm module
app.register_blueprint(history_alarm.mod, url_prefix='/history/alarm')
# Register History Product module
app.register_blueprint(history_product.mod, url_prefix='/history/product')
# Register Geo Alert module
app.register_blueprint(geo_alert.mod, url_prefix='/geo/alert')
# Register Geo Check module
app.register_blueprint(geo_check.mod, url_prefix='/geo/check')
# Register Geo Dump module
app.register_blueprint(geo_dump.mod, url_prefix='/geo/dump')
# Register Geo Historia
app.register_blueprint(geo_historia.mod, url_prefix='/geo/historia')
#app.register_blueprint(promos.mod, url_prefix='/promos')



if __name__ == '__main__':
    app.run(
        host=config.APP_HOST,
        port=config.APP_PORT,
        debug=config.DEBUG
    )
