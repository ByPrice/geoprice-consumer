from flask import g


class Task(object):
    """ Class for managing asynchronous tasks.
        
        Attributes:
            task_id (uuid) : task_uuid to track task
            status (str): status (PENDING, RUNNING, COMPLETED)
            result (json): json object with the task result
            backend (str)
            ttl (int) : task's result time to live
    """

    def __init__(self):
        
        pass

    def save_status(self):
        """ Save status to redis or to file
        """
        self.set_status('PENDING',)
        if backend == 'redis':
            # Get status from redis
            g._redis.set('status:'+self.task_uuid,status)

        elif backend == None:
            # Get status from file
            pass
        