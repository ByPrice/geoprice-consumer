from flask import g, request
import uuid
from ByHelpers import applogger
import json
import datetime
from functools import wraps
from enum import Enum
import importlib

# logger
logger = applogger.get_logger()


def asynchronize(async_function):
    """ Asynchronize Decorator
    """
    def wrap(f):
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            if request.method == 'POST':
                params = request.get_json()
            if request.method == 'GET':
                params = request.args.to_dict()
            # Execute function asynchonously
            action = getattr(
                importlib.import_module(
                    "app.celery_app"
                ), 
                "main_task"
            )
            celery_task = action.apply_async(args=(async_function,params))
            async_id = celery_task.id
            # Setting initial status
            task = Task(async_id)
            task.progress = 0
            # Set request id in the request global object
            request.async_id = celery_task.id
            return f(*args, **kwargs)
        return wrapped_f
    return wrap


class TaskState(Enum):
    NONE = 0
    STARTING = 1
    RUNNING = 2
    COMPLETED = 3
    CANCELLED = 4
    ERROR = -1


class Task(object):
    """ Class for managing asynchronous tasks.
        
        Attributes:
            _task_id (uuid) : task_id to track task
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

    # Dictionary of task stages
    STAGE = {
        1 : 'STARTING',
        2 : 'RUNNING',
        3 : 'COMPLETED',
        0 : 'ERROR',
        4 : 'CANCELLED'
    }


    def __init__(self, task_id=None):
        """ Pass a task_id, if its empty it assumes it's a new task
        """
        self._backend = 'redis'
        self._status = {}
        self._result = {}
        self._data = {}
        self._ttl =  86400  # One day 
        self._progress = 0
        self._stage = ''
        self._status_msg = ''
        self._result_msg = ''
        # If no task_id create new one        
        if not task_id:
            # If new task, generate random uuid
            self._task_id = str(uuid.uuid4())     
        else:
            self._task_id = task_id

    @property
    def task_id(self):
        """ Getter for task_id
        """ 
        return self._task_id

    @task_id.setter
    def task_id(self, value):
        """ Task uuid setter, empty the value of
            the status and the result
        """
        self._status = {}
        self._result = {}
        self._task_id = value
        return self.task_id

    @property
    def progress(self):
        """ Getter for task_id
        """ 
        return self._progress

    @progress.setter
    def progress(self, value):
        """ Task progress value setter,
            while setting the value saves the status
        """
        try:
            value = int(value)
            if value < -1 or value > 100:
                raise Exception
        except Exception as e:
            logger.error("Incorrect value format")
            return False

        # Case 0: STARTING
        if value == 0:
            status = {
                "stage" : self.STAGE[1],
                "msg" : "Task is starting",
                "progress" : value
            }

        # Case >0 <100: In Progress
        elif value > 0 and value < 100:
            status = {
                "stage" : self.STAGE[2],
                "msg" : "Task is executing...",
                "progress" : value
            }

        # Case 100: COMPLETED
        elif value >= 100:
            status = {
                "stage" : self.STAGE[3],
                "msg" : "Task completed",
                "progress" : value
            }
        # Case -1: CANCELLED
        elif value == -1:
            status = {
                "stage" : self.STAGE[4],
                "msg" : "Task cancelled",
                "progress" : value
            }

        self.status = status
        return self.progress

    @property
    def status(self):
        """ get status from redis or file and
            set status
        """ 
        if self._backend == 'redis':
            res = g._redis.get("task:status:"+self._task_id)
            if res:
                self._status = json.loads(res.decode('utf-8'))
            else:
                self._status = {
                    "stage" : self.STAGE[1],
                    "msg" : "Task is pending to start",
                    "progress" : 0
                }
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
        props = ['stage','msg','progress']
        if type(status) != dict or (set(props) <= set(status.keys())) != True :
            logger.error("Invalid status for task")
            return False
        if 'stage' in status and status['stage']:
            if type(status['stage']) == str:
                self._stage = status['stage']  
            else:
                self._stage = self.STAGE[status['stage']]
        if 'progress' in status and status['progress']: self._progress = status['progress']
        if 'msg' in status and status['msg']: self._status_msg = status['msg']
        self._status = {
            "task_id" : self._task_id,
            "stage" : self._stage,
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
            print("Getting result from redis: " + "task:result:"+self._task_id)
            res = g._redis.get("task:result:"+self._task_id)
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
            "task_id" : self._task_id,
            "msg" : self._result_msg,
            "data" : self._data,
            "date" : datetime.datetime.utcnow().strftime("%Y-%m-%d %I:%M:%S")
        }
        print(self._result)
        self._save_result()
        return self._result

    @property
    def name(self):
        """ 
            Getter for task name, get the value
            from the given backend and set it to the
            variable as well
        """
        if self._backend == 'redis':
            print("Getting name from redis" + "task:name:"+self._task_id)
            res = g._redis.get("task:name:"+self._task_id)
            if res:
                self._name = res.decode('utf-8')
            else:
                self._name = {}
        elif self._backend == None:
            logger.error("Backend not defined to get name")
        return self._name

    @name.setter
    def name(self, name):
        """ 
            Setter for task result, save the value
            in the given backend (redis)

                @Params:
                    :value (dict)
                
                @Returns:
                    :name (name)
        """
        if type(name) != str:
            logger.error("Malformed name")
            return False
        self._name = name
        self._save_name()
        return self._name

    def _save_name(self):
        """ Save status to redis or to file
        """
        try:
            if self._backend == 'redis':
                # Get status from redis
                g._redis.set('task:name:'+self._task_id, self._name, ex=self._ttl )
                logger.debug("Task name stored in "+ 'task:name:'+self._task_id )
                return True
            elif self._backend == None:
                logger.error("No backend defined")
                pass
        except Exception as e:
            logger.error("Could not persist task name, check configuration")
            logger.error(e)
            return False

    def _save_status(self):
        """ Save status to redis or to file
        """
        try:
            if self._backend == 'redis':
                # Get status from redis
                g._redis.set('task:status:'+self._task_id, json.dumps(self._status), ex=self._ttl )
                logger.debug("Task status stored in "+ 'task:status:'+self._task_id )
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
        if not hasattr(self, '_task_id'):
            logger.error("Can not save result without defining the task_id!")
            return False 
        try:
            print("Storing Result", self._task_id)
            if self._backend == 'redis':
                g._redis.set("task:result:"+self._task_id, 
                    json.dumps(self._result), 
                    ex=self._ttl )
                return True
            elif self._backend == None:
                logger.error("No backend defined")
                pass
        except Exception as e:
            logger.error("Something went wrong saving task result")
            logger.error(e)
            return False

    def is_running(self):
        """ Check if task is still running
            @Returns
                : bool
        """
        return self.status['progress'] < 100 and self.status['progress'] >= 0

    def error(self, msg="Error"):
        """ Save error status in the running task
        """
        if not hasattr(self, '_task_id'):
            logger.error("Can not set error to an undefined task")
            return False 
        # Set error status
        self.status = {
            "stage" : self.STAGE[4],
            "msg" : msg,
            "progress" : -1
        }
        return True
        