import datetime
from uuid import UUID
from io import StringIO
from flask import g
import pandas as pd
from app import errors, logger
from config import *
from app.models.item import Item
import json


class Promos(object):
    """ Class perform query methods 
        on Cassandra to get the
        applying promos for that day.
    """

    @staticmethod
    def get_cassandra_promos_by_day(day, max_num_promos=20):
        """ Query applying promos in that day

            Params:
            -----
            day :  str
                day to look for promos
            num_promos :  int
                minimum number of promos to get

            Returns:
            -----
            promos : dict
                JSON with applying promos
        """
        CHUNK_SIZE = 500
        num_found_promos = 0
        ip = 1
        has_printed = False

        yield '{"items": ['
        while True:
            #  get items to look for promos
            prods = Item.get_all_items(ip, CHUNK_SIZE)

            logger.info("Chunk {}"
                        .format(ip))

            #  if no more items or we reached the limit, we're done
            if len(prods) == 0 or (max_num_promos > 0 and num_found_promos >= max_num_promos):
                logger.info(
                    "FINISHED FOUND {} Promos, exiting while..".format(num_found_promos))
                break

            # Fetch prod uuids
            puuids = [p['product_uuid'] for p in prods]
            cass_query = """SELECT *
                    FROM promo
                    WHERE product_uuid IN {}
                    AND date = {}"""\
                        .format(str(tuple(_p for _p in puuids)).replace("'", ""),
                                day.replace("-", ""))

            try:
                q = g._db.query(cass_query,
                                timeout=20)
                if not q:
                    ip += 1
                    continue
                qs = list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                ip += 1
                continue

            num_found_promos += len(qs)
            logger.info("Finished chunk {}, found {} promos, found {} total promos"
                        .format(ip, len(qs), num_found_promos))

            #  create the DF and cast types
            temp_df = pd.DataFrame(list(qs))
            temp_df['product_uuid'] = temp_df['product_uuid'].astype('str')
            temp_df['store_uuid'] = temp_df['store_uuid'].astype('str')
            temp_df['time'] = temp_df['time'].astype('str')
            for d in temp_df.to_dict(orient='records'):
                if (has_printed):
                    yield ', '+json.dumps(d)
                else:
                    yield json.dumps(d)
                    has_printed = True

            ip += 1
        yield '], "message": "finished"}'
