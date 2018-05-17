from flask import g
import uuid
from ..utils import applogger
import json

logger = applogger.get_logger()

class Task(object):
    """ Class for managing asynchronous tasks.
        
        Attributes:
            _task_uuid (uuid) : task_uuid to track task
            _result (json): json object with the task result
            _backend (str)
            _ttl (int) : task's result time to live
            _status (dic): dictionaty with status vars
                - progress (float)
                - msg (text)
                - text (text [PENDING, RUNNING, COMPLETED]) 
    """

    def __init__(self, task_uuid=None):
        self._backend = 'redis'
        self._status = {}
        self._ttl =  86400  # One day 
        self._progress = 0
        self._text = ''
        self._message = ''
        # New task
        if not task_uuid:
            # If new task, generate random uuid
            self._task_uuid = str(uuid.uuid4())     
        else:
            self._task_uuid = task_uuid


    @property
    def task_uuid(self):
        """ Getter for task_uuid
        """ 
        return self._task_uuid

    @property
    def status(self):
        """ get status from redis or file and
            set status
        """ 
        if self._backend == 'redis':
            res = g._redis.get("task:status:"+self._task_uuid)
            if res:
                self._status = json.loads(res.decode('utf-8'))
        elif self._backend == None:
            pass
        return self._status

    @status.setter
    def status(self, status):
        """ Set status variables and status dict
        """
        props = ['text','msg','progress']
        if type(status) != dict or (set(props) <= set(status.keys())) != True :
            logger.error("Invalid status for task")
            return False
        if 'text' in status and status['text']: self._text = status['text']
        if 'progress' in status and status['progress']: self._progress = status['progress']
        if 'msg' in status and status['msg']: self._msg = status['msg']
        self._status = {
            "task_uuid" : self._task_uuid,
            "text" : self._text,
            "progress" : self._progress,
            "msg" : self._msg
        }
        self._save_status()
        return self._status
        

    def _save_status(self):
        """ Save status to redis or to file
        """
        try:
            if self._backend == 'redis':
                # Get status from redis
                g._redis.set('task:status:'+self._task_uuid, json.dumps(self._status), ex=self._ttl )
                logger.debug("Task status stored in "+ 'task:status:'+self._task_uuid )
                return True
            elif self._backend == None:
                pass
        except Exception as e:
            logger.error("Could not persist task status, check configuration")
            logger.error(e)
            return False

        
    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, result):
        """ Set task result
        """
        if result:
            self._result = result

    def save_result(self):
        """ Save the task result to the backend
        """
        if self.backend == 'redis':
            g._redis.set('task:result:'+self._task_uuid, json.dumps(self._result), ex=self._ttl)