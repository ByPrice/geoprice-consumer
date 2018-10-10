from flask import g
from app.utils import applogger 
import importlib

logger = applogger.get_logger()

def start_script(script):
    """ Evaluate the task to be executed and start 
        execution.
    """
    #try:
    if True:
        logger.info(script)
        module = importlib.import_module("app.scripts.{}".format(script))
        module.start()
    '''except ImportError as e:
        logger.error("Could not find the specified module...")
    except Exception as e:
        logger.error(e)'''
        