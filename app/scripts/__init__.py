from flask import g
from app.utils import applogger

logger = applogger.get_logger()


def start_script(script):
    """ Evaluate the task to be executed and start 
        execution.
    """

    if script == 'create_stats':
        module = import create_stats
        module.start()
    elif script == 'create_dumps':
        module = import create_dumps
        module.start()
    else:
        # raise
        logger.error("Could not find the specified module...")
