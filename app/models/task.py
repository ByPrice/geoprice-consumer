from flask import g
import uuid
from ..utils import applogger
import json
import datetime

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
            _data (dict) : result data
            _result (dict) : dictionary with
                - text (str)
                - data (dict) 
            _status_msg (text)
            _result_msg (text)
    """

    def __init__(self, task_uuid=None):
        """ Pass a task_uuid, if its empty it assumes it's a new task
        """
        self._backend = 'redis'
        self._status = {}
        self._result = {}
        self._data = {}
        self._ttl =  86400  # One day 
        self._progress = 0
        self._text = ''
        self._status_msg = ''
        self._result_msg = ''
        
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

    @task_uuid.setter
    def task_uuid(self, value):
        """ Task uuid setter, empty the value of
            the status and the result
        """
        self._status = {}
        self._result = {}
        self._task_uuid = value
        return self.task_uuid

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
            logger.error("Backend not defined to get result")
        return self._status

    @status.setter
    def status(self, status):
        """ 
            Setter for task status, save the value
            in the given backend (redis)

                @Params:
                    - value (dict)
                
                @Returns:
                    - result (bool)
        """
        props = ['text','msg','progress']
        if type(status) != dict or (set(props) <= set(status.keys())) != True :
            logger.error("Invalid status for task")
            return False
        if 'text' in status and status['text']: self._text = status['text']
        if 'progress' in status and status['progress']: self._progress = status['progress']
        if 'msg' in status and status['msg']: self._status_msg = status['msg']
        self._status = {
            "task_uuid" : self._task_uuid,
            "text" : self._text,
            "progress" : self._progress,
            "date" : datetime.datetime.utcnow().strftime("%Y-%m-%d %I:%M:%S"),
            "msg" : self._status_msg
        }
        self._save_status()
        return self._status
        
    @property
    def result(self):
        """ 
            Getter for task result, get the value
            from the given backend and set it to the
            variable as well
        """
        if self._backend == 'redis':
            print("Getting result from redis" + "task:result:"+self._task_uuid)
            res = g._redis.get("task:result:"+self._task_uuid)
            if res:
                self._result = json.loads(res.decode('utf-8'))
            else:
                self._result = {}
        elif self._backend == None:
            logger.error("Backend not defined to get result")
        return self._result

    @result.setter
    def result(self, value):
        """ 
            Setter for task result, save the value
            in the given backend (redis)

                @Params:
                    - value (dict)
                
                @Returns:
                    - result (bool)
        """
        props = ['data', 'msg']
        if type(value) != dict or (set(props) <= set(value.keys())) != True:
            logger.error("Malformed result")
            return False
        if 'data' in value and value['data']: self._data = value['data']
        if 'msg' in value and value['msg']: self._result_msg = value['msg']
        self._result = {
            "task_uuid" : self._task_uuid,
            "msg" : self._result_msg,
            "data" : self._data,
            "date" : datetime.datetime.utcnow().strftime("%Y-%m-%d %I:%M:%S")
        }
        self._save_result()
        return self._result



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
                logger.error("No backend defined")
                pass
        except Exception as e:
            logger.error("Could not persist task status, check configuration")
            logger.error(e)
            return False

    def _save_result(self):
        """ Save the result of the task in redis
        """
        if not hasattr(self, '_task_uuid'):
            logger.error("Can not save result without defining the task_uuid!")
            return False 
        try:
            if self._backend == 'redis':
                g._redis.set("task:result:"+self._task_uuid, json.dumps(self._result), ex=self._ttl )
                return True
            elif self._backend == None:
                logger.error("No backend defined")
                pass
        except Exception as e:
            logger.error("Something went wrong saving task result")
            logger.error(e)
            return False
