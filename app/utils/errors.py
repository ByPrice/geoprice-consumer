from flask import jsonify


class AppError(Exception):

    def __init__(self, error, description, status_code=400):
        self.error = error
        self.status_code = status_code
        self.description = description

    def to_dict(self):
        rv = dict()
        rv['error'] = self.error
        rv['message'] = self.description
        return rv


class TaskError(Exception):
    pass