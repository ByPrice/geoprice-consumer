#-*- coding: utf-8 -*-
from flask import g
from config import CASSANDRA_TTL
import datetime
import json
import uuid
import math
from ByHelpers import applogger
import warnings
from uuid import UUID
import pandas as pd

# Database connection:  db.session
logger = applogger.get_logger()

class Price(object):
    ''' All attributes received from kafka that a price can have
    '''
    # Db Session
    session = None
    fields = [
        'product_uuid', 'gtin', 'store_uuid', 'product_id',
        'price','price_original','discount', 'currency',
        'promo','date','location','coords','zips', 'source',
        'stores', 'lats', 'lngs', 'cities', 'url', 'retailer'
    ]

    product_uuid = None
    source = None
    retailer = None
    gtin = None
    url = None
    price = None
    price_original = None
    currency = 'MXN'
    discount = None
    promo = None
    date = None
    coords = []
    zips = []
    stores = []
    cities = []
    states = []
    lats = []
    lngs = []
    location = {}
    _part = None
    insert_ttl = CASSANDRA_TTL

    def __init__(self, *initial_data, **kwargs):
        # Db session init
        self.session = g._db
        # In case of dictionary initialization
        for dictionary in initial_data:
            for key in dictionary:
                if key in dir(self):
                    setattr(self, key, dictionary[key])
        # In case of keyworded initialization
        for key in kwargs:
            if key in self.__dict__.keys():
                setattr(self, key, kwargs[key])
        # Date conversion
        try:
            self.time = datetime.datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S.%f')
        except:
            self.time = datetime.datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S')
        #self.date = self.time.strftime('%Y-%m-%d')     # <- Date as string
        self.date =  int(str(self.time.year)+str(self.time.month).zfill(2)+str(str(self.time.day).zfill(2)))
        # Gtin
        try:
            self.gtin = int(self.gtin)
        except:
            self.gtin = None
            #logger.error("Gtin invalid format, only int accepted: {}".format(self.gtin))
        # Locations
        coords = []
        zips = []
        stores = []
        cities = []
        states = []
        lats = []
        lngs = []
        for i in range(0,len(self.location['store'])):
            # Explode location array into multiple
            stores.append(self.location['store'][i])
            coords.append(self.location['coords'][i])
            zips.append(self.location['zip'][i])
            cities.append(self.location['city'][i])
            states.append(self.location['state'][i])
            lats.append(self.location['coords'][i]['lat'])
            lngs.append(self.location['coords'][i]['lng'])
        self.stores = stores
        self.coords = coords
        self.zips = zips
        self.cities = cities
        self.states = states
        self.lats = lats
        self.lngs = lngs
        # Retailer as source
        self.source = self.retailer if not self.source else self.source

    @property
    def values(self):
        ''' Display the column names and values of the current object
        '''
        values = {}
        for f in self.fields:
            if f in self.__dict__:
                values[f] = self.__dict__[f]
        return values

    @property
    def as_dict(self):
        ''' Dictionary representation for saving to cassandra '''
        return {
            'product_uuid' : uuid.UUID(self.product_uuid),
            'gtin' :  self.gtin if self.gtin is None else int(self.gtin),
            'source' : self.source,
            'url' : self.url if self.url != None else '',
            'price' : float(self.price),
            'price_original' : float(self.price_original) if self.price_original else self.price,
            'currency' : self.currency,
            'discount' : float(self.discount) if self.discount else 0,
            'promo' : self.promo if (self.promo != None and self.promo != 'null') else 0,
            'date' : self.date,
            'time' : self.time,
            'store' : [sts if sts != None else '' for sts in self.stores],
            'state' : [sts if sts != None else '' for sts in self.states],
            'city' : [sts if sts != None else '' for sts in self.cities],
            'zip' : [sts if sts != None else '' for sts in self.zips],
            'coords' : self.coords,
            'lat' : [ coords['lat'] for coords in self.location['coords'] ],
            'lng' : [ coords['lng'] for coords in self.location['coords'] ],
        }

    @property
    def part(self):
        """ Partition property
        """
        return self._part

    @part.setter
    def part(self, val):
        """ Partition property
        """
        assert isinstance(val, int)
        self._part = val


    @staticmethod
    def validate(elem):
        ''' Quick fields validation
        '''
        logger.debug("Validating price")
        req_vars = ["product_uuid","price","price_original","date","location"]
        keys = list(elem.keys())
        # Si no tiene todas las keys requeridas regresamos False
        if not set(req_vars).issubset(keys):
            logger.error("Invalid price: not complete set of required params")
            logger.debug(elem)
            return False
        # If there is no price, return False
        try:
            assert (type(elem['price']) is float or type(elem['price']) is int ) == True
        except:
            logger.error("Invalid price: error in price field")
            return False
        # Currency
        try:
            if 'currency' in elem:
                assert type(elem['currency']) == str
        except:
            logger.warning("Invalid price: error in currency field")
            if 'currency' in elem:
                del elem['currency']
        # Retailer or Source validation
        if 'retailer' not in elem and 'source' not in elem:
            logger.error("Missing Retailer or Source field")
            return False
        # If there is no location of the price, return False
        if not elem['location'] or not elem['location']['coords'] or type(elem['location']['coords']) != list:
            logger.error("Invalid price: error in location")
            return False
        return True


    def loc_generator(self):
        ''' Generator for locations in a price
        '''
        for i in range(0,len(self.location['store'])):
            yield {
                'product_uuid' : uuid.UUID(self.product_uuid),
                'gtin' :  self.gtin if self.gtin is None else int(self.gtin),
                'source' : self.source,
                'url' : self.url,
                'price' : float(self.price),
                'price_original' : float(self.price_original) if self.price_original else self.price,
                'currency' : self.currency,
                'discount' : float(self.discount) if self.discount else 0,
                'promo' : self.promo,
                'date' : self.date,
                'time' : self.time,
                'store_uuid' : uuid.UUID(self.stores[i]),
                'state' : self.states[i],
                'city' : self.cities[i],
                'zip' : self.zips[i],
                'coords' : self.coords[i],
                'lat' : self.lats[i],
                'lng' : self.lngs[i],
                'part': self.part,
                'insert_ttl': self.insert_ttl
            }


    # Save price in every price table
    def save_all(self):
        ''' Save price in all tables
            - execute()
            - execue_async() en caso de que se haga cuello de botella
        '''
        #logger.info("[3] Saving price in all tables...")
        # self.save_price()    # DEPRECATED
        self.save_price_by_product_date()
        # self.save_price_by_date()    # DEPRECATED
        self.save_price_by_product_store()
        # self.save_price_by_geohash()  # DEPRECATED
        # self.save_price_by_source()   # DEPRECATED
        self.save_price_by_store()
        self.save_promo()
        self.save_promo_by_store()
        return True

    # Save as raw price in json format
    def save_price_raw(self):
        """ [DEPRECATED] `price_raw` table saver method. 
        """
        # try:
        #     return self.session.execute(
        #         """ 
        #         INSERT INTO price_raw (
        #             date, product_uuid, raw
        #         )
        #         VALUES (
        #             %(date)s, %(product_uuid)s, %(raw)s
        #         ) 
        #         """, {
        #             "date" : self.date,
        #             "product_uuid" : self.product_uuid,
        #             "raw" : json.dumps(self.as_dict)
        #         })
        # except Exception as e:
        #     logger.error("Could not save price raw data")
        #     logger.error(self.as_dict)
        #     logger.error(e)
        #     return []
        warnings.warn("This table (`price_raw`) has been deprecated from the C* data model!")
        return []


    # Salvamos en tabla price
    def save_price(self):
        """ [DEPRECATED] `price` table saver method. 
        """
        # try:
        #     for elem in self.loc_generator():
        #         self.session.execute(
        #             """
        #             INSERT INTO price(
        #                 product_uuid, time, gtin, store_uuid, lat, lng, price, price_original, promo, url, currency 
        #             )
        #             VALUES(
        #                 %(product_uuid)s, %(time)s, %(gtin)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
        #             )
        #             """,
        #             elem
        #         )
        #     logger.debug("OK save_price")
        #     return True
        # except Exception as e:
        #     # logger.error("Could not save price")
        #     # logger.error(self.as_dict)
        #     # logger.error(e)
        #     return []
        warnings.warn("This table (`price`) has been deprecated from the C* data model!")
        return []

    def save_price_by_product_date(self):
        """ `price_by_product_date` table save method
        """
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_product_date(
                        product_uuid, date, time, store_uuid, source, price, price_original, promo, currency, url
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(source)s, %(price)s, %(price_original)s, %(promo)s, %(currency)s, %(url)s
                    )
                    USING TTL %(insert_ttl)s
                    """,
                    elem
                )
            logger.debug("OK save_price_by_product_date")
            return True
        except Exception as e:
            # logger.debug("Could not save price_by_product_date")
            # logger.error(self.as_dict)
            logger.error(e)
            return []

    def save_price_by_date(self):
        """ [DEPRECATED] `price_by_date_parted` table save method
        """
        # try:
        #     for elem in self.loc_generator():
        #         self.session.execute(
        #             """
        #             INSERT INTO price_by_date_parted(
        #                 date, part, time, product_uuid, store_uuid, price, price_original, promo, url, currency 
        #             )
        #             VALUES(
        #                 %(date)s, %(part)s, %(time)s, %(product_uuid)s, %(store_uuid)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
        #             )
        #             """,
        #             elem
        #         )
        #     logger.debug("OK save_price_by_date_parted")
        #     return True
        # except Exception as e:
        #     logger.error("Could not save price_by_date_parted")
        #     # logger.error(self.as_dict)
        #     logger.error(e)
        #     return []
        warnings.warn("This table (`price_by_date_parted`) has been deprecated from the C* data model!")
        return []

    def save_price_by_product_store(self):
        """ `price_by_product_store` table save method
        """
        #try:
        if True:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_product_store(
                        product_uuid, date, store_uuid, time, source, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(store_uuid)s, %(time)s, %(source)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    USING TTL %(insert_ttl)s
                    """,
                    elem
                )
            logger.debug("OK save_price_by_product_store")
            return True
        if False:
        #except Exception as e:
            # logger.error("Could not save price_by_product_store")
            # logger.error(self.as_dict)
            # logger.error(e)
            return []

    def save_price_by_geohash(self):
        """ [DEPRECATED] Saves price by each geohash with a resolution from 4 to 12
            at the `price_by_geohash` table.
        """
        # for elem in self.loc_generator():
        #     # Get the geohash of the coordinates
        #     ghash = geohash.encode(float(elem['lat']), float(elem['lng']))
        #     geo = [
        #         ghash,
        #         ghash[:-1],
        #         ghash[:-2],
        #         ghash[:-3],
        #         ghash[:-4],
        #         ghash[:-5],
        #         ghash[:-6],
        #         ghash[:-7],
        #         ghash[:-8]
        #     ]
        #     # Loop geohashes to save
        #     for gh in geo:
        #         # Get the geohash
        #         elem['geohash'] = gh
        #         self.session.execute(
        #             """
        #             INSERT INTO price_by_geohash(
        #                 product_uuid, geohash, time, source, store_uuid, lat, lng, price, price_original, promo, url, currency
        #             )
        #             VALUES(
        #                 %(product_uuid)s, %(geohash)s, %(time)s, %(source)s, %(store_uuid)s, %(lat)s, 
        #                 %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
        #             )
        #             """,
        #             elem
        #         )
        # logger.debug("OK save_price_by_geohash")
        # return True
        warnings.warn("This table (`price_by_geohash`) has been deprecated from the C* data model!")
        return False


    def save_price_by_source(self):
        """ [DEPRECATED] `price_by_source_parted` table saver method
        """
        # try:
        #     for elem in self.loc_generator():
        #         self.session.execute(
        #             """
        #             INSERT INTO price_by_source_parted (
        #                  source, date, part, time, product_uuid, store_uuid, price, price_original, promo, url, currency 
        #             )
        #             VALUES(
        #                 %(source)s, %(date)s, %(part)s, %(time)s, %(product_uuid)s, %(store_uuid)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
        #             )
        #             """,
        #             elem
        #         )
        #     logger.debug("OK save_price_by_source_parted")
        #     return True
        # except Exception as e:
        #     logger.error("Could not save price_by_source_parted")
        #     # logger.error(self.as_dict)
        #     logger.error(e)
        #     return []
        warnings.warn("This table (`price_by_source_parted`) has been deprecated from the C* data model!")
        return []

    def save_price_by_store(self):
        """ `price_by_store` table saver method
        """
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_store (
                        store_uuid, date, time, product_uuid, source, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(store_uuid)s, %(date)s, %(time)s, %(product_uuid)s, %(source)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    USING TTL %(insert_ttl)s
                    """,
                    elem
                )
            logger.debug("OK save_price_by_store")
            return True
        except Exception as e:
            # logger.error("Could not save price_by_source")
            # logger.error(self.as_dict)
            logger.error(e)
            return []

    def save_promo(self):
        """ Save only if the item has a promo at `promo` table
        """
        # Valida Promo
        if not self.promo or self.promo == None or self.promo == '':
            return True
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO promo (
                        product_uuid, date, time, store_uuid, source, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(source)s,%(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    USING TTL %(insert_ttl)s
                    """,
                    elem
                )
            logger.debug("OK save_promo")
            return True
        except Exception as e:
            logger.debug("Could not save promo")
            # logger.error(self.as_dict)
            logger.error(e)
            return []

    def save_promo_by_store(self):
        """ Save only if the item has a promo at `promo_by_store` table
        """
        if not self.promo or self.promo == None or self.promo == '':
            return True

        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO promo_by_store (
                        product_uuid, date, time, store_uuid, source, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(source)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    USING TTL %(insert_ttl)s
                    """,
                    elem
                )
            logger.debug("OK save_promo_by_store")
            return True
        except Exception as e:
            # logger.error("Could not save promo_by_store")
            # logger.error(self.as_dict)
            logger.error(e)
            return []
    
    @staticmethod
    def save_stats_by_product(elem):
        """ Save aggregated values by day

            Params:
            -----
            elem : dict
                Product aggregated values
        """    
        try:
            if not hasattr(elem, 'insert_ttl'):
                elem['insert_ttl'] = CASSANDRA_TTL
            g._db.execute(
                """
                INSERT INTO stats_by_product (
                    product_uuid, date, avg_price, source, datapoints,
                    max_price, min_price, mode_price, std_price
                )
                VALUES(
                    %(product_uuid)s, %(date)s, %(avg_price)s, %(source)s,
                    %(datapoints)s, %(max_price)s, %(min_price)s,
                    %(mode_price)s, %(std_price)s
                )
                USING TTL %(insert_ttl)s
                """,
                elem
            )
            logger.debug("OK save_stats_by_product")
            return True
        except Exception as e:
            # logger.error("Could not save save_stats_by_product")
            # logger.error(elem)
            logger.error(e)
            return False

    # Save price in every price table
    def save_all_batch(self):
        ''' Save price in all tables
            - execute()
            - execue_async() en caso de que se haga cuello de botella
        '''
        self.save_batch()
        return True

    def save_batch(self):
        """ Store in batch for following tables:
            `price_by_product_date`
            `price_by_product_store`
            `price_by_store`
            `promo`
            `promo_by_store`
        """ 
        try:
            elem = list(self.loc_generator())[0]
            self.session.execute(
                """
                BEGIN BATCH

                INSERT INTO price_by_product_date(
                        product_uuid, date, time, store_uuid, source, price, price_original, promo, currency, url
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(source)s, %(price)s, %(price_original)s, %(promo)s, %(currency)s, %(url)s
                    ) USING TTL %(insert_ttl)s ;

                INSERT INTO price_by_product_store(
                        product_uuid, date, store_uuid, source,  time, lat, lng, price, price_original, promo, url, currency
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(store_uuid)s, %(source)s, %(time)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    ) USING TTL %(insert_ttl)s;

                INSERT INTO price_by_store (
                    store_uuid, date, time, product_uuid, source, lat, lng, price, price_original, promo, url, currency
                )
                VALUES(
                    %(store_uuid)s, %(date)s, %(time)s, %(product_uuid)s, %(source)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                ) USING TTL %(insert_ttl)s;

                INSERT INTO promo (
                    product_uuid, date, time, store_uuid, source, lat, lng, price, price_original, promo, url, currency
                )
                VALUES(
                    %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(source)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                ) USING TTL %(insert_ttl)s;

                INSERT INTO promo_by_store (
                    product_uuid, date, time, store_uuid, source, lat, lng, price, price_original, promo, url, currency
                )
                VALUES(
                    %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(source)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                ) USING TTL %(insert_ttl)s;

                APPLY BATCH;
                """,
                elem
            )
            return True
        except Exception as e:
            logger.error(e)
            return False


    @staticmethod
    def query_by_product_store(products=[], stores=[], dates=[]):
        """ Query cassandra by prod_uuid, store_uuid and dates
            by three nested loops getting all values possibles
            @Returns:
                [{
                    "product" : "",
                    "store" : "",
                    "date" : ""
                }]
        """
        logger.info("Querying prices in C*")
        # Order dates
        #dates.sort()
        result = []
        # Nested loops
        for d in dates:
            for p in products:
                try:
                    rows = g._db.query("""
                        SELECT source as retailer,
                        product_uuid, price_original,
                        store_uuid, price, time, date, promo
                        FROM price_by_product_date
                        WHERE product_uuid=%s 
                        AND date=%s
                    """, (UUID(p), d) )
                    if rows:
                        for _tr in rows: 
                            if str(_tr.store_uuid) in stores:
                                result.append(_tr)
                except Exception as e:
                    logger.error(e)
                    continue
        logger.info("Returning C* prices")
        return result     

    @staticmethod
    def get_by_store(st_uuid, hours):
        """ Get prices from a given store
            from the past X hours.
            
            Params:
            -----
            st_uuid : str
                Store UUID
            hours : int
                Past hours to consult
            
            Returns:
            -----
            list:
                Unique prices
        """ 
        now = datetime.datetime.utcnow()
        then = _date = now - datetime.timedelta(hours=abs(hours))
        logger.debug("Getting prices from {}".format(then))
        _prices = []
        # Query all prices
        while _date.date() <= now.date():
            rows = g._db.query("""SELECT product_uuid,
                        store_uuid, price, time,
                        price_original, promo
                    FROM price_by_store
                    WHERE store_uuid = %s
                    AND date = %s """,
                    (UUID(st_uuid), int(_date.strftime("%Y%m%d")) )
            )
            if rows:
                _prices += rows
            # add to date 
            _date += datetime.timedelta(days=1)
        if not _prices:
            logger.warning("Not prices found!")
            return []
        # Remove duplicated prices
        _df = pd.DataFrame(_prices)\
                .sort_values('time', ascending=False)\
                .drop_duplicates(['product_uuid', 'store_uuid'])
        # Cast UUIDs
        _df['product_uuid'] = _df.product_uuid.astype(str)
        _df['store_uuid'] = _df.store_uuid.astype(str)
        return _df.to_dict(orient='records')