import datetime
import uuid
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

class Stats(object):
    """ Class perform query methods 
        on Cassandra products over aggregated tables
    """

    def __init__(self):
        pass

    @staticmethod
    def fetch_from_catalogue(filters, rets):
        """ Fetch products info from Catalogue Srv

            Params:
            -----
            filters : list
                List of dicts containing elements to filter
                from.
            rets : list
                List of Sources keys

            Returns:
            -----
            resp : list
                List of dicts with name, item_uuid, prod_uuid, etc.
        """
        prods = []
        # Fetch uuids from filters in Catalogue
        _cols = ['product_uuid', 'item_uuid',
            'gtin', 'name', 'source']     
        _iuuids = [f['item_uuid'] for f in filters\
            if 'item_uuid' in f]
        logger.debug(_iuuids)
        _puuids = [f['product_uuid'] for f in filters\
            if 'product_uuid' in f]
        logger.debug(_puuids)
        # Get by item uuid
        for _iu in _iuuids:
            prods += Item.get_by_item(_iu, _cols)
        # Get by product_uuid
        if len(_puuids) > 0:
            prods += Item.get_by_product(_puuids, _cols)
        # Filter products from requested retailers
        prods = pd.DataFrame(prods)
        if prods.empty:
            return []
        return prods[prods['source'].isin(rets)]\
                .to_dict(orient='records')
        
    @staticmethod
    def fetch_rets(params):
        """ Retrieve retailers info
            Params:
            -----
            filters : list
                List of dicts containing elements to filter
                from.

            Returns:
            -----
            resp : list
                List of dicts with name, item_uuid, prod_uuid, etc.
        """
        # Verifying sent retailers
        rets = []
        for x in params:
            if 'retailer' in x:
                rets.append(x['retailer'])
        # Fetching retailers from GEO
        if not rets:
            try:
                rets = [r['key'] \
                    for r in requests\
                            .get(SRV_GEOLOCATION+'/retailer/all')\
                            .json()]
            except Exception as e:
                logger.error(e)
                raise errors.AppError(80005,
                    'No Retailers found  Geolocation Service')
                return []
        return rets

    @staticmethod
    def get_cassandra_by_ret(prods, rets, dates):
        """ Query prices of aggregated table 
            
            Params:
            -----
            prods : list
                List of products
            rets : list
                List of source/retailer keys
            dates : list
                List of dates
            
            Returns
            -----
            df : pandas.DataFrame
                Product aggregated prices
        """
        # Fetch prod uuids
        puuids = [p['product_uuid'] for p in prods]
        # Generate dates
        dates = sorted(dates)
        if len(dates) == 1:
            period = 1
        else:
            period = (dates[-1] - dates[0]).days
        _days = tupleize_date(dates[0].date(), period)
        cass_query = """SELECT product_uuid, avg_price,
                min_price, max_price,
                mode_price, date
                FROM stats_by_product
                WHERE product_uuid = %s
                AND date = %s"""
        qs = []
        # Iterate for each product-date combination
        for _p, _d in itertools.product(puuids, _days):
            try:
                q = g._db.query(cass_query, 
                    (UUID(_p), _d),
                    timeout=100)
                if not q:
                    continue
                qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))
                continue
        logger.info("Fetched {} prices".format(len(qs)))
        logger.debug(qs[:1] if len(qs) > 1 else [])
        # Empty validation
        if len(qs) == 0:
            return pd.DataFrame({'date':[], 'product_uuid':[]})
        # Load Response into a DF
        df = pd.DataFrame(qs)
        df['product_uuid'] = df.product_uuid.astype(str)
        return df

    @staticmethod
    def get_actual_by_ret(params):
        """ Retrieve current prices given a set of filters
            and sources to query from.

            Params:
            -----
            params: dict
                Params with filters including
                (retailer, item, or category)
            
            Returns:
            -----
            formatted : list
                List of formatted values
        """
        logger.info('Entered Current by Retailer..')
        # Retailers from service
        rets = Stats.fetch_rets(params)
        if not rets:
            raise errors.AppError(80011,
                "No retailers found.")
        # Items from service
        filt_items = Stats.fetch_from_catalogue(params, rets)
        if not filt_items:
            logger.warning("No Products found!")
            return []
        logger.info("Got filtered items..")
        _now = datetime.datetime.utcnow()
        # Products query
        df_curr = Stats\
            .get_cassandra_by_ret(filt_items,
                rets,
                [_now,
                _now - datetime.timedelta(days=1)])\
            .sort_values(by=['date'], ascending=False)\
            .drop_duplicates(subset=['product_uuid'], keep='first') # today
        df_curr.rename(columns={'avg_price':'avg',
            'max_price':'max', 'min_price': 'min',
            'mode_price': 'mode'}, inplace=True)
        df_prev = Stats\
            .get_cassandra_by_ret(filt_items,
                rets,
                [_now - datetime.timedelta(days=1),
                _now - datetime.timedelta(days=2)])\
                .sort_values(by=['date'], ascending=False)\
            .drop_duplicates(subset=['product_uuid'], keep='first') # yesterday
        # If queried lists empty
        if df_curr.empty:
            # Return empty set
            logger.warning('Empty set from query...')
            return []
        if df_prev.empty:
            # Create a  copy of previous and set everything to zero
            df_prev = df_curr.copy()\
                .drop(['avg', 'min',
                        'max', 'date',
                        'mode'],
                    axis=1)
            _ = ['prev_avg', 'prev_min',
                'prev_max', 'prev_mode']
            for j in _:
                df_prev[j] = '-'
        else:
            df_prev.rename(columns={
                "avg_price": "prev_avg",
                "min_price": "prev_min",
                "max_price": "prev_max",
                "mode_price": "prev_mode"
                }, inplace=True)
        # Add product attributes to Current prices DF
        info_df = pd.DataFrame(filt_items,
            columns=['item_uuid', 'product_uuid',
                'name', 'gtin', 'source'])
        df_curr = pd.merge(df_curr, info_df,
            on='product_uuid', how='left')
        # Merge Current with prev to retrieve previous prices
        df = pd.merge(df_curr, df_prev,
            on='product_uuid', how='left')
        df.fillna('-', axis=0, inplace=True)
        ### TODO:
        # Add rows with unmatched products!
        non_matched = df[df['item_uuid'].isnull() | 
            (df['item_uuid'] == '')].copy()
        # Format only products with matched results
        df = df[~(df['item_uuid'].isnull()) & 
            (df['item_uuid'] != '')]
        formatted = []
        for i, prdf in df.groupby(by=['item_uuid']):
            _first = prdf[:1].reset_index()
            tmp = {
                'item_uuid': _first.loc[0,'item_uuid'],
                'name': _first.loc[0,'name'],
                'gtin': _first.loc[0,'gtin'],
                'prices': {}
            }
            for j, row in prdf.iterrows():
                _r = row.to_dict()
                del _r['source']
                del _r['date']
                tmp['prices'].update({
                    row['source']: _r
                })
            for r in (set(rets) - tmp['prices'].keys()):
                tmp['prices'].update({
                    r: { 'avg': '-', 'min': '-',
                        'max': '-', 'mode': '-',
                        'prev_avg': '-', 'prev_min': '-',
                        'prev_max': '-', 'prev_mode': '-'
                    }
                })
            formatted.append(tmp)
        logger.info('Got actual!!')
        return formatted

    @staticmethod
    def add_empty_interval(intd, date_groups, rets, params):
        """ Add empty intervals 
            to complete correct ones

            Params:
            -----
            intd : datetime.datetime
                Initial interval
            date_groups: list
                List of lists of dates
            rets : list
                List of sources keys
            params: dict
                Params of request

            Returns:
            -----
            tmp2 : dict
                Extra interval dict
        """
        try:
            if params['interval'] == 'week':
                intd = [int(dtt) \
                    for dtt in pd\
                                .to_datetime('{}-{}'\
                                            .format(intd[0],
                                                    str(intd[1]).zfill(2))\
                                            + '-0', format='%Y-%W-%w')\
                                .date().__str__().split('-')]
            elif params['interval'] == 'month':
                lday = calendar.monthrange(*intd)[1]
                intd = [int(dtt) \
                    for dtt in pd\
                                .to_datetime('{}-{}'\
                                            .format(intd[0],
                                                    str(intd[1]).zfill(2)) \
                                            + '-{}'.format(lday), format='%Y-%m-%d')\
                                .date().__str__().split('-')]
            else:
                # day
                pass
            d_belong = find_date_interval(pd.tslib\
                                            .Timestamp(*(intd)),
                                          date_groups)
            tmp2 = {
                'date_start': d_belong[0],
                'date_end': d_belong[1],
                'client': '-',
                'avg': '-',
                "difference": '-',
                "retailers": []
            }
        except Exception as e:
            logger.error(e)
            logger.warning(intd)
        # All retailers completion
        for r in rets:
            # Avoid Client in response
            if r == params['client']:
                continue
            if (' '.join([ik[0].upper() + ik[1:]
                for ik in r.split('_')]))\
                    in [x['source'] for x in tmp2['retailers']]:
                continue
            tmp2['retailers'].append({
                'source': ' '.join([ik[0].upper() + ik[1:]
                                     for ik in r.split('_')]),
                'price': '-',
                'difference': '-'
            })
        return tmp2

    @staticmethod
    def get_comparison(params):
        """ Retrieve current prices given a set 
            of filters and sources to compare them 
            against a fixed source

            Params:
            -----
            params: dict
                Params with filters including
                (retailer, item, or category)
            
            Returns:
            -----
            formatted : list
                List of formatted values
        """
        logger.debug("Entered to Compare by period...")
        # Retailers from service
        rets = Stats.fetch_rets(params['filters'])
        if not rets:
            raise errors.AppError(80011,
                "No retailers found.")
        # Products from service
        filt_items = Stats\
            .fetch_from_catalogue(params['filters'], rets)
        if not filt_items:
            logger.warning("No Products found!")
            return []
        # Date Grouping
        date_groups = grouping_periods(params)
        logger.info('Found grouped dates')
        # Retrieve prices from
        df = Stats.get_cassandra_by_ret(filt_items,
            rets, [date_groups[0][0], date_groups[-1][-1]])
        if df.empty:
            logger.warning('Empty set from query...')
            return []
        # Parse datapoint date
        df['date'] = df['date'].apply(get_datetime())
        df['day'] = df['date'].apply(lambda x: x.day)
        df['month'] = df['date'].apply(lambda x: x.month)
        df['year'] = df['date'].apply(lambda x: x.year)
        df['week'] = df['date'].apply(lambda x: x.isocalendar()[1])
        grouping_cols = {'day': ['year', 'month', 'day'],
                         'month': ['year', 'month'],
                         'week': ['year', 'week']}
        # Obtain all total amount of intervals
        interval_to_have = []
        for ii, row_df in df.groupby(grouping_cols[params['interval']]):
            interval_to_have.append(ii)
        # Set Products DF
        info_df = pd.DataFrame(filt_items,
            columns=['item_uuid', 'product_uuid',
                'name', 'gtin', 'source'])
        # Add product info
        df = pd.merge(df, info_df,
                on='product_uuid', how='left')
        ### TODO:
        # Add rows with unmatched products!
        non_matched = df[df['item_uuid'].isnull() | 
            (df['item_uuid'] == '')].copy()
        # Format only products with matched results
        df = df[~(df['item_uuid'].isnull()) & 
            (df['item_uuid'] != '')]
        # Group by item and them depending on Date Range
        interv_list = []
        for i, tdf in df.groupby('item_uuid'):
            tdf.reset_index(inplace=True)
            tmp = {'item_uuid': i,
                    'name': tdf['name'][0],
                    'gtin': tdf['gtin'][0],
                    'interval_name': params['interval'],
                    'intervals': []}
            # Grouping by Time interval columns
            en = 0
            prev_rets = {z: 0 for z in rets}
            for j, df_t in tdf.groupby(grouping_cols[params['interval']]):
                client = df_t[df_t['source'] ==
                              params['client']]['avg_price'].mean()
                avg = df_t[df_t['source'] !=
                           params['client']]['avg_price'].mean()
                # Compare Date, if the same process it, otherwise continue
                if j != interval_to_have[en]:
                    while j != interval_to_have[en]:
                        logger.info('Setting new empty interval')
                        tmp2 = Stats\
                            .add_empty_interval(interval_to_have[en],
                                                date_groups, rets, params)
                        tmp['intervals'].append(tmp2)
                        en += 1
                        # Security Break
                        if en >= len(interval_to_have):
                            break
                #print(i, j)
                # Client Price and competition average computation
                try:
                    client = float(client)
                    if str(client) == 'nan':
                        raise Exception('NaN')
                except Exception:
                    client = 0
                try:
                    avg = float(avg)
                    if str(avg) == 'nan':
                        raise Exception('NaN')
                except Exception:
                    avg = 0
                # Temporal dict update
                tmp2 = {}
                d_belong = find_date_interval(df_t['date'].min(), date_groups)
                #print(d_belong)
                tmp2.update({
                    'date_start': d_belong[0],
                    'date_end': d_belong[1],
                    'client': client,
                    'avg': avg,
                    "retailers": []
                })
                tmp2.update({'difference': tmp2['client']-tmp2['avg']})
                # Retailers computation
                for k, df_r in df_t.groupby(['source']):
                    tmp2['retailers'].append({
                        'source': ' '.join([ik[0].upper() + ik[1:]
                                              for ik in k.split('_')]),
                        'price': df_r['avg_price'].mean(),
                        'difference': (tmp2['client']-df_r['avg_price'].mean()
                                       if ((isinstance(tmp2['client'], float))
                                           or (isinstance(tmp2['client'], int))
                                           and (tmp2['client'] > 0)) else '-')
                        })
                    # Assign prev prices for next analysis
                    prev_rets[k] = df_r['avg_price'].mean()
                # All retailers completion
                for r in rets:
                    # Avoid Client in response
                    if r == params['client']:
                        continue
                    if (' '.join([ik[0].upper() + ik[1:]
                        for ik in r.split('_')])) \
                            in [x['source'] for x in tmp2['retailers']]:
                        continue
                    tmp2['retailers'].append({
                        'source': ' '.join([ik[0].upper() + ik[1:]
                                             for ik in r.split('_')]),
                        'price': '-',
                        'difference': '-'
                    })
                tmp['intervals'].append(tmp2)
                # Add to enumerator to substract interval dates
                en += 1
            # Add to the response list
            if en < (len(interval_to_have)):
                while en < (len(interval_to_have)):
                    logger.info('Setting new empty interval')
                    tmp2 = Stats.add_empty_interval(interval_to_have[en],
                                                       date_groups,
                                                       rets, params)
                    tmp['intervals'].append(tmp2)
                    en += 1
            interv_list.append(tmp)
        return interv_list

    @staticmethod
    def convert_csv_actual(prod):
        logger.debug("Converting list to csv file..")
        df = pd.DataFrame()
        for crow in prod:
            col_names = {'gtin':crow['gtin'],
                'Nombre': crow['name']}
            col_names.update(dictify(crow['prices']))
            df = pd.concat([df,pd.DataFrame([col_names])])
        df.set_index(['gtin','Nombre'],inplace=True)
        #df.drop('item_uuid', axis=1, inplace=True)
        _buffer = StringIO()
        iocsv = df.to_csv(_buffer)
        _buffer.seek(0)
        return _buffer

    @staticmethod
    def convert_csv_market(prod):
        logger.debug("Converting list to csv file..")
        df = pd.DataFrame()
        for crow in prod:
            recs = []
            for ivs in crow['intervals']:
                tmp = {'gtin':crow['gtin'],
                    'Nombre':crow['name']}
                tmp.update({'Fecha Inicio': ivs['date_start'],
                            'Fecha Final' : ivs['date_end'],
                            'Mi Retailer': ivs['client']})
                for rs in ivs['retailers']:
                    rrs = readfy(rs['retailer'])
                    tmp.update({rrs:rs['price'], 
                            rrs+' Diferencia' : rs['difference']})
                recs.append(tmp)
            df = pd.concat([df,pd.DataFrame(recs)])
        df.set_index(['gtin','Nombre', 'Fecha Inicio', 'Fecha Final', 'Mi Retailer'],inplace=True)
        df.replace([0,0.0],['-','-'], inplace=True)
        _buffer = StringIO()
        iocsv = df.to_csv(_buffer)
        _buffer.seek(0)
        return _buffer


    @staticmethod
    def get_historics(params):
        logger.debug("Entered to extract Historic by period...")
        # Items from service
        filt_items = Stats.fetch_from_catalogue(params['filters'])
        if not filt_items:
            return False
        # Retailers from service
        rets = Stats.fetch_rets(params['filters'])
        if not rets:
            logger.warning("No retailers")
            return rets
        # Date Grouping
        params.update({'ends':False})
        date_groups = grouping_periods(params)
        logger.debug('Grouping dates')
        #logger.debug(pf(date_groups))
        # Query over all range
        ld = [d.date() for d in [date_groups[0][0],date_groups[-1][-1]]]
        iqres = Retailer.get_cassandra_by_ret(filt_items,rets,ld)
        # Prices DF           
        df = pd.DataFrame(iqres)
        if len(df) < 1:
            return []
        # Products DF 
        fdf = pd.DataFrame(filt_items, columns=["item_uuid","gtin","name"])
        # Obtaining Dates and formating
        df['item_uuid'] = df['item_uuid'].apply(lambda x : str(x))
        df['date'] = df['time'].apply(get_time_from_uuid())
        df['timetuple'] = df['date'].apply(lambda d: tuple(int(x) if i!= 1 else int(x)\
                                            for i,x in enumerate(d.date().isoformat().split('-')))+(0,0)\
                                        )   ## cahnge int(x) -> int(x) + 1
        df['date'] = df['timetuple'].apply(lambda dt :datetime.datetime(*dt))
        df['time_js'] = df['date'].apply(lambda djs: (djs - datetime.datetime(1970, 1, 1,0,0))/datetime.timedelta(seconds=1)*1000)
        df['day'] = df['date'].apply(lambda x : x.day)
        df['month'] = df['date'].apply(lambda x : x.month)
        df['year'] = df['date'].apply(lambda x : x.year)
        df['week'] = df['date'].apply(lambda x : x.isocalendar()[1])
        grouping_cols = {'day':['year','month','day'],
                        'month':['year','month'],
                        'week': ['year','week']}
        df_n = pd.merge(df,fdf,on='item_uuid')
        #logger.debug(len(df_n))
        # --- Compute for Metrics
        # Group by timestamp
        avg_l,min_l, max_l = [],[],[]
        for j,df_t in df_n.groupby(grouping_cols[params['interval']]):
            # Compute max, min, avg
            #print(df_t['time_js'][0])
            avg_l.append([list(df_t['time_js'])[0],df_t['avg_price'].mean()])
            min_l.append([list(df_t['time_js'])[0],df_t['min_price'].mean(),df_t['avg_price'].mean()])
            max_l.append([list(df_t['time_js'])[0],df_t['max_price'].mean(),df_t['avg_price'].mean()])
        logger.debug('Got Metrics...')
        # --- Compute for Retailers
        retailers = []
        # Group by retailer
        for i,df_t in df_n.groupby('retailer'):
            tmp = {'name': ' '.join([rsp[0].upper() + rsp[1:] for rsp in i.split('_')]), 'data':[]}
            #logger.debug('Grouped by retailer: '+str(i))
            for j,df_p in df_t.groupby(grouping_cols[params['interval']]):#('time_js'):
                # Group by timestamp
                #print(list(df_t['time_js'])[0])
                #print(dir(df_p['time_js']))
                tmp['data'].append([df_p['time_js'].tolist()[0],df_p['avg_price'].mean()])
            retailers.append(tmp)
            #logger.debug('Appended retailer: '+str(i))
        logger.debug('Got Retailers...')
        sub_str = "<b> Retailers:</b> " + ', '.join([' '.join([rsp[0].upper() + rsp[1:] for rsp in rt.split('_')]) for rt in rets]) + '.'
        return {
                'title': 'Tendencia de Precios',
                'subtitle': '<b>Periodo</b>: {} - {} <br> {}'.format(ld[0].isoformat(),ld[1].isoformat(), sub_str),
                'metrics': {'avg':avg_l,'min':min_l,'max':max_l},
                'retailers': retailers
                }

    @staticmethod 
    def get_count_by_cat(categ_its):
        """
            Method to get all the count of the elements in a categ (given the list of items of it)
            Params:
                categ_its: list of dicts
            Return:
            [
                { x: 1, y: 180.7, z: 200, name: 'Superama',retailer: 'Superama' },
                { x: 1, y: 180.7, z: 200, name: 'Superama',retailer: 'Superama' }
            ]
        """
        items = [{'item_uuid': iu['item']} for iu in categ_its if 'item' in iu ]
        rets = Stats.fetch_rets(categ_its)
        qres = Stats.get_cassandra_by_ret(items,rets,[datetime.date.today()+datetime.timedelta(days=-1)], False) # today
        if len(qres) == 0:
            logger.warning('No prices found!')
            return []
        df = pd.DataFrame(qres)
        #logger.debug(df.head())
        ccat, counter,digs = [], 1, 1
        for i,row in df.groupby('retailer'):
            prod_count = row.drop_duplicates(['item_uuid'])['avg_price'].count()
            prod_avg = row.drop_duplicates(['item_uuid'])['avg_price'].mean()
            ccat.append({
                    'x':counter, 
                    'name': i, 
                    'retailer': " ".join([x[0].upper()+x[1:] for x in i.split('_')]),
                    'z': round(float(prod_count),2),
                    'y': round(float(prod_avg),2)
                    })
            counter+=1
            digs = digs if digs > len(str(prod_count)) else len(str(prod_count))
        # Scaling x for plot
        for i,xc in enumerate(ccat):
            ccat[i]['x'] = xc['x']*(10**(digs))
        #logger.debug(ccat)
        logger.info('Got Category counts')
        return ccat

def find_date_interval(date_i, d_groups):
    """
        Method to find date belonging over time periods
    
        Params:
        -----
        date_i : datetime
            Initial date
        d_groups : list
            Date groups

        Returns:
        -----
        (date_initial, date_final) : tuple
            Initial and End date of analized period
    """
    for dg in d_groups:
        #print('Data day', date_i.to_datetime())
        if len(dg) == 1:
            # Day case
            #print('Group day',dg[0])
            if date_i.to_pydatetime().date() == dg[0].date():
                #print('Same day')
                return (dg[0],dg[0])
        else:
            #print('Group days',dg[0], dg[1])
            if (date_i.to_pydatetime().date() >= dg[0].date()) \
                and (date_i.to_pydatetime().date() <= dg[1].date()):
                #print('Range day')
                return (dg[0],dg[1])
    #print('NO DATE FOUND ----------------------')