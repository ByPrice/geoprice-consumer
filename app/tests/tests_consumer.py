# -*- coding: utf-8 -*-
import app
import config
import unittest
import json
import threading
import time
import sys
from pprint import pprint
from app.models.price import Price
from app.norm import map_product_keys as mpk

# Incoming Product Test (PRICE)
prods_test_price = [
    {"retailer": "superama", "location": {"store": ["ca7a6808-7afa-11e7-8dda-0242ac110005"]}, "discount": 0, "promo": "", "price": 795.01, "route_key": "price", "date": "2018-04-16 18:22:39.175227", "id": "842755810511", "url": "https://www.superama.com.mx/catalogo/d-exclusivos-online/f-vinos-exclusivos/l-tinto-exclusivo/vino-tinto-luis-canas-gran-reserva-750-ml/0842755810511", "price_original": 0.0},
    {"promo": "", "id": "000000000004900040", "retailer": "san_pablo", "location": {"store": ["e8c8008c-7ae5-11e7-a394-0242ac110003", "e914761a-7ae5-11e7-a394-0242ac110003", "e94ac4c2-7ae5-11e7-a394-0242ac110003", "5b8e249a-7ae9-11e7-a394-0242ac110003", "5ba8393e-7ae9-11e7-a394-0242ac110003", "5bfcb5f4-7ae9-11e7-a394-0242ac110003", "5c1c6e44-7ae9-11e7-a394-0242ac110003", "5c69abaa-7ae9-11e7-a394-0242ac110003", "5bc316f0-7ae9-11e7-a394-0242ac110003", "5bdf11ac-7ae9-11e7-a394-0242ac110003", "5cebfb64-7ae9-11e7-a394-0242ac110003", "5c35bb74-7ae9-11e7-a394-0242ac110003", "5cd14242-7ae9-11e7-a394-0242ac110003", "e9331fde-7ae5-11e7-a394-0242ac110003", "5d3e4cb6-7ae9-11e7-a394-0242ac110003", "5c9d1efe-7ae9-11e7-a394-0242ac110003", "5d22b280-7ae9-11e7-a394-0242ac110003", "5b760ae0-7ae9-11e7-a394-0242ac110003", "e8fcb7c8-7ae5-11e7-a394-0242ac110003", "5e10a918-7ae9-11e7-a394-0242ac110003", "5dd77a30-7ae9-11e7-a394-0242ac110003", "5f4acaf2-7ae9-11e7-a394-0242ac110003", "ecfe71f8-7b09-11e7-9262-80e6500152aa", "5f02c7b6-7ae9-11e7-a394-0242ac110003", "5f2fe908-7ae9-11e7-a394-0242ac110003", "5e922006-7ae9-11e7-a394-0242ac110003", "e298ab8c-7b09-11e7-a9cc-80e6500152aa", "ec845ca6-7b09-11e7-acc2-80e6500152aa", "e312b758-7b09-11e7-bb5f-80e6500152aa", "eb8e976c-7b09-11e7-9bbd-80e6500152aa", "e4828908-7b09-11e7-b5dd-80e6500152aa", "e407cdd8-7b09-11e7-a5a8-80e6500152aa", "e91d8838-7b09-11e7-8d06-80e6500152aa", "ea8e2f94-7b09-11e7-af83-80e6500152aa", "f9b3f9ec-7b09-11e7-bd95-80e6500152aa", "f939edd0-7b09-11e7-b7c1-80e6500152aa", "fd0ea286-7b09-11e7-9569-80e6500152aa", "fc180c76-7b09-11e7-910f-80e6500152aa", "edfd1c00-7b09-11e7-a312-80e6500152aa", "ee785c06-7b09-11e7-b485-80e6500152aa", "eef2e534-7b09-11e7-b87b-80e6500152aa", "ef7c6c14-7b09-11e7-90ac-80e6500152aa", "eff8f81c-7b09-11e7-be42-80e6500152aa", "f168c1b4-7b09-11e7-8606-80e6500152aa", "f1e49826-7b09-11e7-9159-80e6500152aa", "f2607b34-7b09-11e7-8f5e-80e6500152aa", "faa9ce5e-7b09-11e7-965e-80e6500152aa", "f2db02a6-7b09-11e7-9075-80e6500152aa", "f3d0c724-7b09-11e7-8bcb-80e6500152aa", "fb9dea36-7b09-11e7-a55f-80e6500152aa", "f540a62e-7b09-11e7-91d7-80e6500152aa", "f8bf99f4-7b09-11e7-bcdd-80e6500152aa", "fefef76c-7b09-11e7-8e1e-80e6500152aa", "fe80eda2-7b09-11e7-b588-80e6500152aa", "e8e0f40c-7ae5-11e7-a394-0242ac110003", "5df4a592-7ae9-11e7-a394-0242ac110003", "e871dfc2-7ae5-11e7-a394-0242ac110003", "f074a1de-7b09-11e7-a6d6-80e6500152aa", "ed809e0a-7b09-11e7-994f-80e6500152aa", "f0eef7ec-7b09-11e7-922e-80e6500152aa", "f72b8846-7b09-11e7-9dcf-80e6500152aa", "e998775c-7b09-11e7-8760-80e6500152aa"]}, "url": "https://www.farmaciasanpablo.com.mx/p//dermocosmeticos/dermocosmeticos/cremas/genove-antiedad-fluidbase-k/p/000000000004900040", "price_original": 521.5, "date": "2018-04-16 16:40:17.137261", "route_key": "price", "match": ["000000000004900040", "Genové Antiedad Fluidbase k", "20 ML Tubo"], "price": 521.5, "discount": 0}
]

# Incoming Product Test (ITEM)
prods_test_item = [
    {"images": ["https://www.superama.com.mx/Content/images/products/img_large/0750101530083L.jpg"], "url": "https://www.superama.com.mx/catalogo/d-lavanderia-hogar-y-mascotas/f-hogar/l-jardin/liquido-para-encender-carbon-quick-fire-carnu-480-ml/0750101530083", "categories": ["Lavanderia Hogar Y Mascotas", "Hogar", "Jardin"], "date": "2018-04-16 18:25:24.928761", "match": {"gtin": "750101530083", "id": "750101530083", "name": "QUICK FIRE ALCOHOL LIQUIDO 480 ML.", "text": "L&#237;quido para Encender Carb&#243;n Quick Fire Carnu 480 ml"}, "raw_attributes": {"characteristics": {"Contenido del Empaque": "1 Líquido para Encender Carbón"}, "info_nutrimental": {}}, "provider": "", "id": "750101530083", "retailer": "superama", "ingredients": ["Metanol"], "description": "L&#237;quido para Encender Carb&#243;n Quick Fire Carnu 480 ml", "brand": "Quick Fire", "route_key": "item", "name": "QUICK FIRE ALCOHOL LIQUIDO 480 ML.", "location": {"store": ["ca7a6808-7afa-11e7-8dda-0242ac110005"]}, "raw_ingredients": "Metanol"},
    {"description": "Frasco Mason  vidrio 500 ml", "brand": "Mason", "match": {"text": "Frasco Mason  vidrio 500 ml", "gtin": "1138100235", "id": "1138100235", "name": ""}, "id": "1138100235", "url": "https://www.superama.com.mx/catalogo/d-lavanderia-hogar-y-mascotas/f-hogar/l-tarros-mason-jars/frasco-mason-vidrio-500-ml/0001138100235", "date": "2018-04-16 18:24:54.889395", "provider": "", "ingredients": [""], "route_key": "item", "raw_attributes": {"info_nutrimental": {}, "characteristics": {"Promoción Envio": "Si"}}, "location": {"store": ["ca7a6808-7afa-11e7-8dda-0242ac110005"]}, "retailer": "superama", "images": ["https://www.superama.com.mx/Content/images/products/img_large/0001138100235L.jpg"], "raw_ingredients": "", "categories": ["Lavanderia Hogar Y Mascotas", "Hogar", "Tarros Mason Jars"], "name": ""},
    {"url": "https://www.farmaciasanpablo.com.mx/p//bebes/dermopediatria/talcos-y-aceites/mon-amour-aceite-corporal-relajante-para-bebe/p/000000000041750012", "images": ["https://sanpablodigital.s3.amazonaws.com/uploads-prod/productimages/Fsp275Wx275H_41750012_1bnn5i89a"], "name": "Mon Amour Aceite corporal Relajante para bebé", "ingredients": [], "raw_ingredients": "", "brand": "", "categories": ["Bebés", "Dermopediatría", "Talcos y aceites"], "id": "000000000041750012", "raw_attributes": "", "retailer": "san_pablo", "location": {"store": ["e8c8008c-7ae5-11e7-a394-0242ac110003", "e914761a-7ae5-11e7-a394-0242ac110003", "e94ac4c2-7ae5-11e7-a394-0242ac110003", "5b8e249a-7ae9-11e7-a394-0242ac110003", "5ba8393e-7ae9-11e7-a394-0242ac110003", "5bfcb5f4-7ae9-11e7-a394-0242ac110003", "5c1c6e44-7ae9-11e7-a394-0242ac110003", "5c69abaa-7ae9-11e7-a394-0242ac110003", "5bc316f0-7ae9-11e7-a394-0242ac110003", "5bdf11ac-7ae9-11e7-a394-0242ac110003", "5cebfb64-7ae9-11e7-a394-0242ac110003", "5c35bb74-7ae9-11e7-a394-0242ac110003", "5cd14242-7ae9-11e7-a394-0242ac110003", "e9331fde-7ae5-11e7-a394-0242ac110003", "5d3e4cb6-7ae9-11e7-a394-0242ac110003", "5c9d1efe-7ae9-11e7-a394-0242ac110003", "5d22b280-7ae9-11e7-a394-0242ac110003", "5b760ae0-7ae9-11e7-a394-0242ac110003", "e8fcb7c8-7ae5-11e7-a394-0242ac110003", "5e10a918-7ae9-11e7-a394-0242ac110003", "5dd77a30-7ae9-11e7-a394-0242ac110003", "5f4acaf2-7ae9-11e7-a394-0242ac110003", "ecfe71f8-7b09-11e7-9262-80e6500152aa", "5f02c7b6-7ae9-11e7-a394-0242ac110003", "5f2fe908-7ae9-11e7-a394-0242ac110003", "5e922006-7ae9-11e7-a394-0242ac110003", "e298ab8c-7b09-11e7-a9cc-80e6500152aa", "ec845ca6-7b09-11e7-acc2-80e6500152aa", "e312b758-7b09-11e7-bb5f-80e6500152aa", "eb8e976c-7b09-11e7-9bbd-80e6500152aa", "e4828908-7b09-11e7-b5dd-80e6500152aa", "e407cdd8-7b09-11e7-a5a8-80e6500152aa", "e91d8838-7b09-11e7-8d06-80e6500152aa", "ea8e2f94-7b09-11e7-af83-80e6500152aa", "f9b3f9ec-7b09-11e7-bd95-80e6500152aa", "f939edd0-7b09-11e7-b7c1-80e6500152aa", "fd0ea286-7b09-11e7-9569-80e6500152aa", "fc180c76-7b09-11e7-910f-80e6500152aa", "edfd1c00-7b09-11e7-a312-80e6500152aa", "ee785c06-7b09-11e7-b485-80e6500152aa", "eef2e534-7b09-11e7-b87b-80e6500152aa", "ef7c6c14-7b09-11e7-90ac-80e6500152aa", "eff8f81c-7b09-11e7-be42-80e6500152aa", "f168c1b4-7b09-11e7-8606-80e6500152aa", "f1e49826-7b09-11e7-9159-80e6500152aa", "f2607b34-7b09-11e7-8f5e-80e6500152aa", "faa9ce5e-7b09-11e7-965e-80e6500152aa", "f2db02a6-7b09-11e7-9075-80e6500152aa", "f3d0c724-7b09-11e7-8bcb-80e6500152aa", "fb9dea36-7b09-11e7-a55f-80e6500152aa", "f540a62e-7b09-11e7-91d7-80e6500152aa", "f8bf99f4-7b09-11e7-bcdd-80e6500152aa", "fefef76c-7b09-11e7-8e1e-80e6500152aa", "fe80eda2-7b09-11e7-b588-80e6500152aa", "e8e0f40c-7ae5-11e7-a394-0242ac110003", "5df4a592-7ae9-11e7-a394-0242ac110003", "e871dfc2-7ae5-11e7-a394-0242ac110003", "f074a1de-7b09-11e7-a6d6-80e6500152aa", "ed809e0a-7b09-11e7-994f-80e6500152aa", "f0eef7ec-7b09-11e7-922e-80e6500152aa", "f72b8846-7b09-11e7-9dcf-80e6500152aa", "e998775c-7b09-11e7-8760-80e6500152aa"]}, "match": {"id": "000000000041750012", "text": "125 ML Botella", "name": "Mon Amour Aceite corporal Relajante para bebé"}, "provider": "", "description": "125 ML Botella", "date": "2018-04-16 16:48:26.278304", "route_key": "item"},
    {"date": "2018-04-16 16:48:26.346992", "name": "Mon Amour Gel de cabello Para bebé", "raw_attributes": "", "brand": "", "location": {"store": ["e8c8008c-7ae5-11e7-a394-0242ac110003", "e914761a-7ae5-11e7-a394-0242ac110003", "e94ac4c2-7ae5-11e7-a394-0242ac110003", "5b8e249a-7ae9-11e7-a394-0242ac110003", "5ba8393e-7ae9-11e7-a394-0242ac110003", "5bfcb5f4-7ae9-11e7-a394-0242ac110003", "5c1c6e44-7ae9-11e7-a394-0242ac110003", "5c69abaa-7ae9-11e7-a394-0242ac110003", "5bc316f0-7ae9-11e7-a394-0242ac110003", "5bdf11ac-7ae9-11e7-a394-0242ac110003", "5cebfb64-7ae9-11e7-a394-0242ac110003", "5c35bb74-7ae9-11e7-a394-0242ac110003", "5cd14242-7ae9-11e7-a394-0242ac110003", "e9331fde-7ae5-11e7-a394-0242ac110003", "5d3e4cb6-7ae9-11e7-a394-0242ac110003", "5c9d1efe-7ae9-11e7-a394-0242ac110003", "5d22b280-7ae9-11e7-a394-0242ac110003", "5b760ae0-7ae9-11e7-a394-0242ac110003", "e8fcb7c8-7ae5-11e7-a394-0242ac110003", "5e10a918-7ae9-11e7-a394-0242ac110003", "5dd77a30-7ae9-11e7-a394-0242ac110003", "5f4acaf2-7ae9-11e7-a394-0242ac110003", "ecfe71f8-7b09-11e7-9262-80e6500152aa", "5f02c7b6-7ae9-11e7-a394-0242ac110003", "5f2fe908-7ae9-11e7-a394-0242ac110003", "5e922006-7ae9-11e7-a394-0242ac110003", "e298ab8c-7b09-11e7-a9cc-80e6500152aa", "ec845ca6-7b09-11e7-acc2-80e6500152aa", "e312b758-7b09-11e7-bb5f-80e6500152aa", "eb8e976c-7b09-11e7-9bbd-80e6500152aa", "e4828908-7b09-11e7-b5dd-80e6500152aa", "e407cdd8-7b09-11e7-a5a8-80e6500152aa", "e91d8838-7b09-11e7-8d06-80e6500152aa", "ea8e2f94-7b09-11e7-af83-80e6500152aa", "f9b3f9ec-7b09-11e7-bd95-80e6500152aa", "f939edd0-7b09-11e7-b7c1-80e6500152aa", "fd0ea286-7b09-11e7-9569-80e6500152aa", "fc180c76-7b09-11e7-910f-80e6500152aa", "edfd1c00-7b09-11e7-a312-80e6500152aa", "ee785c06-7b09-11e7-b485-80e6500152aa", "eef2e534-7b09-11e7-b87b-80e6500152aa", "ef7c6c14-7b09-11e7-90ac-80e6500152aa", "eff8f81c-7b09-11e7-be42-80e6500152aa", "f168c1b4-7b09-11e7-8606-80e6500152aa", "f1e49826-7b09-11e7-9159-80e6500152aa", "f2607b34-7b09-11e7-8f5e-80e6500152aa", "faa9ce5e-7b09-11e7-965e-80e6500152aa", "f2db02a6-7b09-11e7-9075-80e6500152aa", "f3d0c724-7b09-11e7-8bcb-80e6500152aa", "fb9dea36-7b09-11e7-a55f-80e6500152aa", "f540a62e-7b09-11e7-91d7-80e6500152aa", "f8bf99f4-7b09-11e7-bcdd-80e6500152aa", "fefef76c-7b09-11e7-8e1e-80e6500152aa", "fe80eda2-7b09-11e7-b588-80e6500152aa", "e8e0f40c-7ae5-11e7-a394-0242ac110003", "5df4a592-7ae9-11e7-a394-0242ac110003", "e871dfc2-7ae5-11e7-a394-0242ac110003", "f074a1de-7b09-11e7-a6d6-80e6500152aa", "ed809e0a-7b09-11e7-994f-80e6500152aa", "f0eef7ec-7b09-11e7-922e-80e6500152aa", "f72b8846-7b09-11e7-9dcf-80e6500152aa", "e998775c-7b09-11e7-8760-80e6500152aa"]}, "description": "250 ML Botella", "provider": "", "retailer": "san_pablo", "raw_ingredients": "", "ingredients": [], "route_key": "item", "images": ["https://sanpablodigital.s3.amazonaws.com/uploads-prod/productimages/Fsp275Wx275H_41750002_1bt68j1o5"], "url": "https://www.farmaciasanpablo.com.mx/p//bebes/dermopediatria/geles-y-jabones/mon-amour-gel-de-cabello-para-bebe/p/000000000041750002", "id": "000000000041750002", "categories": ["Bebés", "Dermopediatría", "Geles y  jabones"], "match": {"id": "000000000041750002", "name": "Mon Amour Gel de cabello Para bebé", "text": "250 ML Botella"}}
]

# Dummy prod
new_prod_test = {
    "product_id": "00000000000000124901",
    "gtin": "00000001249002",
    "source": "chedraui",
    "name": "refresco pepsi cola sin cafeina 354 ml",
    "description": "refresco pepsi cola sin cafeina 354 ml",
    "images" :  ['http://chedraui.com.mx/media/catalog/product/1/2/124900_00.jpg'],
    "categories": "Despensa",
    "url": "http://www.chedraui.com.mx/index.php/universidad/refresco-pepsi-cola-sin-cafeina-354ml.html",
    "brand": "Pepsi Cola",
    "provider": "Pepsico",
    "attributes": [{
        "attr_name": "Despensa",
        "attr_key": "despensa",
        "clss_name": "Categoría",
        "clss_key": "category",
    }],
    "raw_html": "<body>product_html</body>"
    # "item_uuid": "" # Missing
}

class TestStreamer(threading.Thread):
    """ Testing Streamer Thread
    """

    def __init__(self):
        threading.Thread.__init__(self)      

    def run(self):
        print('Async Streaming Test Price!')
        ## Run consumer
        i = 0
        for i in range(1000000):
            print(i)

class GeopriceConsumerTestCase(unittest.TestCase):
    """ Test Case for Geoprice Consumer
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the database
        """
        # Define test database
        if config.TESTING:
            with app.app.app_context():
                app.initdb()

    @classmethod
    def tearDownClass(cls):
        """ Drops database
        """
        if config.TESTING:
            with app.app.app_context():
                app.dropdb()

    def setUp(self):
        """ Set up
        """
        # Init Flask ctx
        self.ctx = app.app.app_context()
        self.ctx.push()
        app.get_db()

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()

    @unittest.skip('Already tested')
    def test_00_product_validation(self):
        """ Testing Catalogue DB connection
        """ 
        global new_prod_test
        print("Testing Prduct validation")
        prod = Product(new_prod_test)
        pprint(prod.__dict__)
        print('Trying to save...')
        prod.save()
        # Set UUID
        new_prod_test['product_uuid'] = prod.product_uuid
        try:
            self.assertTrue(prod.product_uuid)
        except:
            self.assertFalse(True)
        # Delete product
        print('Deleting test')
        _del = Product.delete(prod.product_uuid)
        self.assertTrue(_del)
        if _del:
            print(_del)