import datetime
from uuid import UUID
from io import StringIO
from flask import g
import pandas as pd
from app import errors, logger
from config import *
from app.models.item import Item


class Promos(object):
    """ Class perform query methods 
        on Cassandra to get the
        applying promos for that day.
    """
    
    @staticmethod
    def get_cassandra_promos_by_day(day, ip=1, ipp=500):
        """ Query applying promos in that day
            
            Params:
            -----
            day :  str
                day to look for promos
            ip :  int
                page number
            ipp : int
                number of results to look for

            Returns:
            -----
            promos : dict
                JSON with applying promos
        """
        
        CHUNK_SIZE = 500        
        qs = []
        
        prods = Item.get_all_items(ip, ipp*2)
        
        for i in range(0, len(prods), CHUNK_SIZE):
        
            limited_prods = prods[i:i+CHUNK_SIZE]
            
            # Fetch prod uuids
            puuids = [p['product_uuid'] for p in limited_prods]
            cass_query = """SELECT *
                    FROM promo
                    WHERE product_uuid IN {}
                    AND date = {}"""\
                        .format(str(tuple(_p for _p in puuids)).replace("'",""),
                                day.replace("-",""))
                                
            try:
                q = g._db.query(cass_query,
                    timeout=20)
                if not q:
                    continue
                qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                continue
        logger.info("Finished, found {} promos"\
            .format(len(qs)))
        
        return {
            'promos': pd.DataFrame(list(qs)).to_dict(orient='records'),
        }
