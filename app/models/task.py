from flask import g
import uuid


class Task(object):
    """ Class for managing asynchronous tasks.
        
        Attributes:
            _task_id (uuid) : task_uuid to track task
            _result (json): json object with the task result
            _backend (str)
            _ttl (int) : task's result time to live
            _status (dic): dictionaty with status vars
                - progress (float)
                - msg (text)
                - text (text [PENDING, RUNNING, COMPLETED]) 
    """

    def __init__(self, task_uuid):
        if not task_uuid:
            # If new task, generate random uuid
            self._task_uuid = str(uuid.uuid4())     
            self._status = None  
            self._backend = 'redis'
            self._ttl =  86400  # One day 
        else:
            self._task_uuid = task_uuid
            self.status()

    @property
    def status(self):
        """ get status from redis or file and
            set status
        """ 
        if self._backend == 'redis':
             status = g._redis.get("task_status:"+self._task_uuid)
             if status:
                 self._status = json.loads(status)
        elif self._backend == None:
            pass
        return self._status

    @status.setter
    def status(self, text, progress, msg):
        """ Set status variables and status dict
        """
        if text: self._text = text
        if progress: self._progress = progress
        if msg: self._msg = msg
        self._status = {
            "task_id" : self._task_uuid
            "text" : self._status_text,
            "progress" : self._progress
            "msg" : self._status_msg
        }
        return self._status
        

    def save_status(self):
        """ Save status to redis or to file
        """
        if self.backend == 'redis':
            # Get status from redis
            g._redis.set('task:status:'+self.task_uuid, self.status, ex=self.ttl )

        elif self.backend == None:
            # Get status from file
            pass

        
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
            g._redis.set('task:result:'+self.task_uuid, json.dumps(self._result), ex=self.ttl)