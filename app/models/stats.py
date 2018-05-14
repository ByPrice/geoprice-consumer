import datetime
import uuid
from uuid import UUID
from io import StringIO
import math
import json
from collections import OrderedDict
from flask import g
import pandas as pd
import numpy as np
import requests
from app import errors, logger
from config import *
from app.utils.helpers import *

class Stats(object):
    """ Class perform query methods 
        on Cassandra products over aggregated tables
    """

    def __init__(self):
        pass