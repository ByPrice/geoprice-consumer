from flask import g
from app.utils import applogger

logger = applogger.get_logger()


def start_script(script):
    """ Evaluate the task to be executed and start 
        execution.
    """

    if script == 'create_stats':
        logger.info("trying to import create_stats")
        from app.scripts import create_stats as module
        logger.info("create_stats imported")
        module.start()
    elif script == 'create_dumps':
        from app.scripts import create_dumps as module
        module.start()
    else:
        # raise
        logger.error("Could not find the specified module...")
