from flask import g
from app.utils import applogger

logger = applogger.get_logger()


def start_script(script):
    """ Evaluate the task to be executed and start 
        execution.
    """
    try:
        #module = __import__(script)
        scr = None
        exec("from app.scripts import {} as scr".format('create_stats'))
        logger.info("Running script:")
        logger.info(scr)
        logger.info("Elements:")
        logger.info(dir(scr))
        scr.start()
    except ImportError as e:
        logger.error("Could not find the specified module...")
    except Exception as e:
        logger.error(e)
