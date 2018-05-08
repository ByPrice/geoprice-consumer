#-*- coding: utf-8 -*-
from .. import db
import datetime
import uuid
import math
from . import geohash
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
        'price','price_original','discount',
        'promo','date','location','coords','zips',
        'stores', 'lats', 'lngs', 'cities'
    ]

    product_uuid = None
    source = None
    gtin = None
    url = None
    price = None
    price_original = None
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

    def __init__(self, *initial_data, **kwargs):
        # Db session init
        self.session = db.session
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
            'id' : self.id,
            'product_uuid' : uuid.UUID(self.item_uuid),
            'gtin' : self.gtin,
            'source' : self.source,
            'url' : self.url if self.url != None else '',
            'price' : float(self.price),
            'price_original' : float(self.price_original) if self.price_original else self.price,
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
            return False
        # If there is no price, return False
        if not elem['price'] or (type(elem['price']) is not float and type(elem['price']) is not int ):
            return False
        # If there is no location of the price, return False
        if not elem['location'] or not elem['location']['coords'] or type(elem['location']['coords']) != list:
            return False
        return True


    def loc_generator(self):
        ''' Generator for locations in a price
        '''
        for i in range(0,len(self.location['store'])):
            yield {
                'product_uuid' : uuid.UUID(self.product_uuid),
                'gtin' : self.gtin,
                'source' : self.source,
                'url' : self.url,
                'price' : float(self.price),
                'price_original' : float(self.price_original) if self.price_original else self.price,
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
        logger.debug("Saving price in all tables...")
        self.save_price()
        self.save_price_by_product_date()
        self.save_price_by_date()
        self.save_price_by_product_store()
        self.save_price_by_geohash()
        self.save_price_by_source()
        self.save_price_by_store()
        self.save_promo()
        self.save_promo_by_store()
        return True

    # Save as raw price in json format
    def save_price_raw(self):
        try:
            return self.session.execute(
            """ INSERT INTO price_raw (
                    date, product_uuid, raw
                )
                VALUES(
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
            return True
        except Exception as e:
            logger.error("Could not save price")
            logger.error(self.as_dict)
            logger.error(e)
            return []

    def save_price_by_product_date(self):
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_product_date(
                        product_uuid, date, time, store_uuid, price, price_original, promo, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(date)s, %(time)s, %(store_uuid)s, %(price)s, %(price_original)s, %(promo)s, %(currency)s
                    )
                    """,
                    elem
                )
            return True
        except Exception as e:
            logger.error("Could not save price_by_product_date")
            logger.error(self.as_dict)
            logger.error(e)
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
            return True
        except Exception as e:
            logger.error("Could not save price_by_date")
            logger.error(self.as_dict)
            logger.error(e)
            return []

    def save_price_by_product_store(self):
        try:
            for elem in self.loc_generator():
                self.session.execute(
                    """
                    INSERT INTO price_by_product_store(
                        product_uuid, store_uuid, time, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(store_uuid)s, %(time)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            return True
        except Exception as e:
            logger.error("Could not save price_by_product_store")
            logger.error(self.as_dict)
            logger.error(e)
            return []


    # Save geohash, 
    def save_price_geohash(self):
        for elem in self.loc_generator():
            # Get the geohash of the coordinates
            ghash = geohash.encode(float(elem['lat']),float(elem['lng']))
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
                    INSERT INTO price_geohash(
                        product_uuid, geohash, time, source, store_uuid, lat, lng, price, price_original, promo, url, currency
                    )
                    VALUES(
                        %(product_uuid)s, %(geohash)s, %(time)s, %(source)s, %(store_uuid)s, %(lat)s, 
                        %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
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
            return True
        except Exception as e:
            logger.error("Could not save price_by_source")
            logger.error(self.as_dict)
            logger.error(e)
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
            return True
        except Exception as e:
            logger.error("Could not save price_by_source")
            logger.error(self.as_dict)
            logger.error(e)
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
                        product_uuid, time, store_uuid, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(time)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            return True
        except Exception as e:
            logger.error("Could not save promo")
            logger.error(self.as_dict)
            logger.error(e)
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
                        product_uuid, time, store_uuid, lat, lng, price, price_original, promo, url, currency 
                    )
                    VALUES(
                        %(product_uuid)s, %(time)s, %(store_uuid)s, %(lat)s, %(lng)s, %(price)s, %(price_original)s, %(promo)s, %(url)s, %(currency)s
                    )
                    """,
                    elem
                )
            return True
        except Exception as e:
            logger.error("Could not save promo")
            logger.error(self.as_dict)
            logger.error(e)
            return []
