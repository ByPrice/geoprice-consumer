# -*- coding: utf-8 -*-
import app
import config
import unittest
import json
from pprint import pprint
import io
import pandas as pd


class GeoPriceServiceTestCase(unittest.TestCase):
    """ Test Case for GeoPrice Service
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
        """ Generating Flask App context for testing
        """
        print("\n***************************\n")
        self.app = app.app.test_client()

    def tearDown(self):
        pass

    #@unittest.skip('Already tested')
    def test_00_geoprice_connection(self):
        """ Testing GeoPrice DB connection
        """ 
        print("Testing GeoPrice DB connection")
        _r =  self.app.get('/product/one')
        print(_r.status_code)
        try:
            print(json.loads(_r.data.decode('utf-8')))
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Already tested')
    def test_01_add_item(self):
        """ Add New Item
        """ 
        global new_item_test
        print("Add New Item")
        _r =  self.app.post('/item/add',
                data=json.dumps(new_item_test),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
        new_item_test['item_uuid'] = _jr['item_uuid']
    
    @unittest.skip('Already tested')
    def test_02_modify_item(self):
        """ Modify existing Item
        """ 
        print("Modify existing Item")
        global new_item_test
        _tmp_item = new_item_test
        _tmp_item['name'] = new_item_test['name'].upper()
        _r =  self.app.post('/item/modify',
                data=json.dumps(_tmp_item),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Already tested')
    def test_03_delete_item(self):
        """ Delete existing Item
        """ 
        print("Delete Item")
        global new_item_test
        _r =  self.app.get('/item/delete?uuid='\
                        + new_item_test['item_uuid'])
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    @unittest.skip('Already tested')
    def test_04_add_product(self):
        """ Add New Product
        """ 
        global new_prod_test
        print("Add New Product")
        _r =  self.app.post('/product/add',
                data=json.dumps(new_prod_test),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
        new_prod_test['product_uuid'] = _jr['product_uuid']
    
    @unittest.skip('Already tested')
    def test_05_modify_product(self):
        """ Modify existing Product
        """ 
        global new_prod_test
        print("Modify existing Product")
        new_prod_test['name'] = new_prod_test['name'].upper()
        _r =  self.app.post('/product/modify',
                data=json.dumps(new_prod_test),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Already tested')
    def test_06_upload_product_normalized(self):
        """ Batch Upload normalized
        """ 
        global new_prod_test
        print("Batch Upload normalized")
        _buffer = io.StringIO()
        pd.DataFrame([
            {'product_uuid': new_prod_test['product_uuid'],
            'normalized': new_prod_test['name'].lower()}])\
            .to_csv(_buffer)
        _r =  self.app.post('/product/normalized',
                content_type='multipart/form-data',
                data={'normalized.csv': (io.BytesIO(_buffer.getvalue().encode('utf-8')), 'text.csv')})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Already tested')
    def test_07_update_prod_img(self):
        """ Update Product Image
        """ 
        global new_prod_test
        print("Update Product Image")
        img_prod_test['product_uuid'] = new_prod_test['product_uuid']
        _r =  self.app.post('/product/image',
                data=json.dumps(img_prod_test),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Already tested')
    def test_08_get_prods_by_item(self):
        """ Get Products by Item UUID (p=1, ipp=50)
        """ 
        print("Get Products by Item UUID (p=1, ipp=50)")
        _p, _ipp = 1, 50
        global new_item_test
        _r =  self.app.get('/product/by/iuuid?keys={}&cols={}&p={}&ipp={}'\
                .format('', #new_item_test['item_uuid'],
                    ','.join(cols_test),
                    _p, _ipp
                    )
                )
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            pprint(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
        #self.assertGreater(len(_jr), 0)
        #self.assertTrue(set(cols_test).issubset(_jr[0].keys()))
    
    @unittest.skip('Already tested')
    def test_09_get_prods_by_product(self):
        """ Get Products by Product UUID (p=1, ipp=50)
        """ 
        print("Get Products by Product UUID (p=1, ipp=50)")
        _p, _ipp = 1, 50
        global new_prod_test, response_prod
        _r =  self.app.get('/product/by/puuid?keys={}&cols={}&p={}&ipp={}'\
                .format(new_prod_test['product_uuid'],
                    ','.join(cols_test),
                    _p, _ipp
                    )
                )
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            pprint(_jr)
            # Global assingment
            response_prod = _jr['products'][0]
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Already tested')
    def test_10_get_prods_by_source(self):
        """ Get Products by Source (p=1, ipp=50)
        """ 
        print("Get Products by Source (p=1, ipp=50)")
        _p, _ipp = 1, 50
        global new_prod_test
        _r =  self.app.get('/product/by/source?keys={}&cols={}&p={}&ipp={}'\
                .format(new_prod_test['source'],
                    ','.join(cols_test),
                    _p, _ipp
                    )
                )
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            pprint(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
        print('Found {} prods'.format(len(_jr['products'])))
    
    @unittest.skip('Already tested')
    def test_11_get_prods_by_attr(self):
        """ Get Products by Attr (p=1, ipp=50)
        """ 
        print("Get Products by Attr (p=1, ipp=50)")
        _p, _ipp, _vals, _rets = 1, 50, '', ''
        global new_prod_test
        _r =  self.app\
            .get('/product/by/attr?keys={}&vals={}&rets={}&cols={}&p={}&ipp={}'\
                .format(new_prod_test['brand'],
                    _vals, _rets,
                    ','.join(cols_test),
                    _p, _ipp
                    )
                )
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            pprint(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
        print('Found {} prods'.format(len(_jr['products'])))

    @unittest.skip('Already tested')
    def test_12_delete_product_image(self):
        """ Delete existing Product Image
        """ 
        print("Delete existing Product Image")
        global response_prod
        _r =  self.app.get('/product/delete/image?uuid={}&id={}'\
                        .format(response_prod['product_uuid'],
                                response_prod['prod_images'][0]['id_p_image']))
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    @unittest.skip('Already tested')
    def test_13_delete_product_attr(self):
        """ Delete existing Product Attr
        """ 
        print("Delete existing Product Attr")
        global response_prod
        _r =  self.app.get('/product/delete/attr?uuid={}&id={}'\
                        .format(response_prod['product_uuid'],
                                response_prod['prod_attrs'][0]['id_p_attr']))
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    @unittest.skip('Already tested')
    def test_90_delete_product(self):
        """ Delete existing Product and its references
        """ 
        print("Delete existing Product and its references")
        global new_prod_test
        _r =  self.app.get('/product/delete?uuid='\
                        + new_prod_test['product_uuid'])
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)


if __name__ == '__main__':
    unittest.main()