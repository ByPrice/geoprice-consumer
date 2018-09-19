from flask import g
from app.utils import applogger

logger = applogger.get_logger()


def start_script(script):
    """ Evaluate the task to be executed and start 
        execution.
    """
    try:
        #module = __import__(script)
        exec("from app.scripts import {} as m_script".format(script))
        logger.info("Running script:")
        logger.info(m_script)
        logger.info("Elements:")
        logger.info(dir(m_script))
        module.start()
    except ImportError as e:
        logger.error("Could not find the specified module...")
    except Exception as e:
        logger.error(e)
