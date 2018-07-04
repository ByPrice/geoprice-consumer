#-*- coding: utf-8 -*-
from flask import g
import datetime
import uuid
import math
from ..utils import geohash
from .. import applogger

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
        'stores', 'lats', 'lngs', 'cities', 'url'
    ]

    product_uuid = None
    source = None
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
            'price_uuid' : uuid.uuid4(),
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


    @staticmethod
    def validate(elem):
        ''' Quick fields validation
        '''
        logger.debug("Validating price")
        req_vars = ["product_uuid","source","price","price_original","date","location"]
        keys = list(elem.keys())
        # Si no tiene todas las keys requeridas regresamos False
        if not set(req_vars).issubset(keys):
            logger.error("Invalid price: not complete set of required params")
            return False
        # If there is no price, return False
        try:
            assert (type(elem['price']) is float or type(elem['price']) is int ) == True
        except:
            logger.error("Invalid price: error in price field")
            return False
        # Currency
        try:
            assert type(elem['currency']) == str
        except:
            logger.error("Invalid price: error in currency field")
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
            }


    # Save price in every price table
    def save_all(self):
        ''' Save price in all tables
            - execute()
            - execue_async() en caso de que se haga cuello de botella
        '''
        #logger.info("[3] Saving price in all tables...")
        # self.save_price()
        # self.save_price_by_product_date()
        # self.save_price_by_date()
        # self.save_price_by_product_store()
        self.save_price_by_geohash()
        # self.save_price_by_source()
        # self.save_price_by_store()
        # self.save_promo()
        # self.save_promo_by_store()
        #
        #self.save_batch()

        #logger.info("[4] Finish saving...")
        return True

    # Save as raw price in json format
    def save_price_raw(self):
        try:
            return self.session.execute(
                """ 
                INSERT INTO price_raw (
                    date, product_uuid, raw
                )
                VALUES (
                    %(date)s, %(product_uuid)s, %(raw)s
                ) 
                """, {
                    "date" : self.date,
                    "product_uuid" : self.product_uuid,
                    "raw" : json.dump(self.as_dict)
                })
        except Exception as e:
            logger.error("Could not save price raw data")
            logger.error(self.as_dict)
            logger.error(e)
            return []


    # Salvamos en tabla price
    def save_price(self):
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price(
                        product_uuid, time, gtin, store_uuid, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(time)s, %(gtin)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            logger.debug("OK save_price")
            return True
        except Exception as e:
            # logger.error("Could not save price")
            # logger.error(self.as_dict)
            # logger.error(e)
            return []

    def save_price_by_product_date(self):
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_product_date(
                        product_uuid, date, time, store_uuid, price, price_original, promo, currency, url
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(price)s, %(price_original)s, %(promo)s, %(currency)s, %(url)s
                    )
                    """,
                    elem
                )
            logger.debug("OK save_price_by_product_date")
            return True
        except Exception as e:
            # logger.error("Could not save price_by_product_date")
            # logger.error(self.as_dict)
            # logger.error(e)
            return []

    def save_price_by_date(self):
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_date(
                        date, time, product_uuid, store_uuid, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(date)s, %(time)s, %(product_uuid)s, %(store_uuid)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            logger.debug("OK save_price_by_date")
            return True
        except Exception as e:
            # logger.error("Could not save price_by_date")
            # logger.error(self.as_dict)
            # logger.error(e)
            return []

    def save_price_by_product_store(self):
        #try:
        if True:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_product_store(
                        product_uuid, date, store_uuid, time, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(store_uuid)s, %(time)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
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
        """ Save price by each goehash
            Resolution from 4 to 12
        """
        for elem in self.loc_generator():
            # Get the geohash of the coordinates
            ghash = geohash.encode(float(elem['lat']), float(elem['lng']))
            geo = [
                ghash,
                ghash[:-1],
                ghash[:-2],
                ghash[:-3],
                ghash[:-4],
                ghash[:-5],
                ghash[:-6],
                ghash[:-7],
                ghash[:-8]
            ]
            # Loop geohashes to save
            for gh in geo:
                # Get the geohash
                elem['geohash'] = gh
                self.session.execute(
                    """
                    INSERT INTO price_by_geohash(
                        product_uuid, geohash, time, source, store_uuid, lat, lng, price, price_original, promo, url, currency
                    )
                    VALUES(
                        %(product_uuid)s, %(geohash)s, %(time)s, %(source)s, %(store_uuid)s, %(lat)s, 
                        %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
        logger.debug("OK save_price_by_geohash")
        return True

    def save_price_by_source(self):
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_source (
                         source, date, time, product_uuid, store_uuid, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(source)s, %(date)s, %(time)s, %(product_uuid)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            logger.debug("OK save_price_by_source")
            return True
        except Exception as e:
            # logger.error("Could not save price_by_source")
            # logger.error(self.as_dict)
            # logger.error(e)
            return []

    def save_price_by_store(self):
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_store (
                        store_uuid, date, time, product_uuid, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(store_uuid)s, %(date)s, %(time)s, %(product_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            logger.debug("OK save_price_by_store")
            return True
        except Exception as e:
            # logger.error("Could not save price_by_source")
            # logger.error(self.as_dict)
            # logger.error(e)
            return []

    def save_promo(self):
        """ Save only if the item has a promo
        """
        if not self.promo or self.promo == None or self.promo == '':
            return True

        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO promo (
                        product_uuid, date, time, store_uuid, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            logger.debug("OK save_promo")
            return True
        except Exception as e:
            # logger.error("Could not save promo")
            # logger.error(self.as_dict)
            # logger.error(e)
            return []

    def save_promo_by_store(self):
        """ Save only if the item has a promo
        """
        if not self.promo or self.promo == None or self.promo == '':
            return True

        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO promo_by_store (
                        product_uuid, date, time, store_uuid, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            logger.debug("OK save_promo_by_store")
            return True
        except Exception as e:
            # logger.error("Could not save promo_by_store")
            # logger.error(self.as_dict)
            # logger.error(e)
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
            g._db.execute(
                """
                INSERT INTO stats_by_product (
                    product_uuid, date, avg_price, datapoints,
                    max_price, min_price, mode_price, std_price
                )
                VALUES(
                    %(product_uuid)s, %(date)s, %(avg_price)s,
                    %(datapoints)s, %(max_price)s, %(min_price)s,
                    %(mode_price)s, %(std_price)s
                )
                """,
                elem
            )
            logger.debug("OK save_stats_by_product")
            return True
        except Exception as e:
            # logger.error("Could not save save_stats_by_product")
            # logger.error(elem)
            # logger.error(e)
            return False

    # Save price in every price table
    def save_all_batch(self):
        ''' Save price in all tables
            - execute()
            - execue_async() en caso de que se haga cuello de botella
        '''
        self.save_batch_geohash()
        self.save_batch()
        return True


    def save_batch_geohash(self):
        """ Save price by each goehash
            Resolution from 4 to 12
        """
        try:
            elem = list(self.loc_generator())[0]
            elem["geohash"] = geohash.encode(float(elem['lat']), float(elem['lng']))
            # Get the geohash of the coordinates
            for i in range(0, 9):
                self.session.execute(
                    """
                    INSERT INTO price_by_geohash(
                        product_uuid, geohash, time, source, store_uuid, lat, lng, price, price_original, promo, url, currency
                    )
                    VALUES(
                        %(product_uuid)s, %(geohash)s, %(time)s, %(source)s, %(store_uuid)s, %(lat)s, 
                        %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
                elem['geohash'] = elem['geohash'][:-1]
            return True
        except Exception as e:
            logger.error("Cannot save geohash: {}".format(e))
            return False


    def save_batch(self):
        try:
            elem = list(self.loc_generator())[0]
            self.session.execute(
                """
                BEGIN BATCH

                INSERT INTO price(
                        product_uuid, time, gtin, store_uuid, lat, lng, price, price_original, promo, url, currency
                    )
                    VALUES(
                        %(product_uuid)s, %(time)s, %(gtin)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    );

                INSERT INTO price_by_product_date(
                        product_uuid, date, time, store_uuid, price, price_original, promo, currency, url
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(price)s, %(price_original)s, %(promo)s, %(currency)s, %(url)s
                    );

                INSERT INTO price_by_date(
                        date, time, product_uuid, store_uuid, price, price_original, promo, url, currency
                    )
                    VALUES(
                        %(date)s, %(time)s, %(product_uuid)s, %(store_uuid)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    );

                INSERT INTO price_by_product_store(
                        product_uuid, date, store_uuid, time, lat, lng, price, price_original, promo, url, currency
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(store_uuid)s, %(time)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    );

                INSERT INTO price_by_source (
                     source, date, time, product_uuid, store_uuid, lat, lng, price, price_original, promo, url, currency
                )
                VALUES(
                    %(source)s, %(date)s, %(time)s, %(product_uuid)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                );

                INSERT INTO price_by_store (
                    store_uuid, date, time, product_uuid, lat, lng, price, price_original, promo, url, currency
                )
                VALUES(
                    %(store_uuid)s, %(date)s, %(time)s, %(product_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                );

                INSERT INTO promo (
                    product_uuid, date, time, store_uuid, lat, lng, price, price_original, promo, url, currency
                )
                VALUES(
                    %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                );

                INSERT INTO promo_by_store (
                    product_uuid, date, time, store_uuid, lat, lng, price, price_original, promo, url, currency
                )
                VALUES(
                    %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                );

                APPLY BATCH;
                """,
                elem
            )
            return True
        except Exception as e:
            logger.error(e)
            return False

