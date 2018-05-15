import datetime
from uuid import UUID
from io import StringIO
import math
import json
import itertools
from collections import OrderedDict
from flask import g
import pandas as pd
import numpy as np
import requests
from app import errors, logger
from config import *
from app.models.item import Item
from app.utils.helpers import *


class Alarm(object):
    """ Class perform query methods 
        on Cassandra products over elements
        to verify change.
    """

    def __init__(self):
        pass

    @staticmethod
    def prices_vs_prior(params):
        return []