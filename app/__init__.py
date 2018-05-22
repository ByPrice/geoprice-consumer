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
import app.utils.applogger as applogger
import app.utils.errors as errors
import app.utils.db as db
from app.utils.rabbit_engine import RabbitEngine
from redis import Redis

# Flask app declaration
app = Flask(__name__)
CORS(app)
applogger.create_logger()
logger = applogger.get_logger()

# Flask controllers imports
#from app.controllers import product, stats, alarm

# Flask blueprint registration
#app.register_blueprint(product.mod, url_prefix='/product')
#app.register_blueprint(stats.mod, url_prefix='/stats')
#app.register_blueprint(alarm.mod, url_prefix='/alarm')
#app.register_blueprint(mapa.mod, url_prefix='/mapa')
#app.register_blueprint(historia.mod, url_prefix='/historia')
#app.register_blueprint(dump.mod, url_prefix='/dump')
#app.register_blueprint(check.mod, url_prefix='/check')


def get_db():
    """ Method to connect to C*
    """
    if not hasattr(g, '_db'):
        g._db = db.getdb()

def get_redis():
    """ Method to connect to redis
    """
    if not hasattr(g, '_redis'):
        g._redis = Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            password=config.REDIS_PASSWORD or None
        )

def get_consumer(queue=None):
    """ App method to connect to rabbit consumer
    """
    if not hasattr(g, "consumer") and queue != None:
        g._consumer = RabbitEngine(config={
            'queue': queue, 
            'routing_key': queue
        }, blocking=False)
    return g._consumer
    

@app.before_request
def before_request():
    """ Before request method
    """
    # Connect to database
    get_db()
    # Connect to redis
    if config.TASK_BACKEND=='redis':
        get_redis()
    

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

if __name__ == '__main__':
    app.run(
        host=config.APP_HOST,
        port=config.APP_PORT,
        debug=config.DEBUG
    )