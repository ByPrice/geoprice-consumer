from uuid import UUID
from io import StringIO
import json
import datetime
import itertools
from collections import defaultdict
from flask import g
from app import errors
from app.models.item import Item
from app.utils.helpers import *
from app.models.task import Task
from ByHelpers import applogger

def dd(): return defaultdict(dd)


def datetime_converter(o):
    if isinstance(o, datetime.datetime):
        return o.date().__str__()


def jsonify(dict_):
    return json.loads(json.dumps(dict_, default=datetime_converter))


def dd_to_dict(dd):
    if isinstance(dd, defaultdict):
        aux = dict(dd)
        aux = {k: dd_to_dict(val) for k, val in aux.items()}
        return aux
    else:
        return dd

logger = applogger.get_logger()

class Stats(object):
    """ Class perform query methods 
        on Cassandra products over aggregated tables

        Completion of methods needed can be found here:
        https://github.com/ByPrice/byprice-stats/blob/dev/app/models/retailer.py 
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
        _iuuids = [f['item_uuid'] for f in filters
                   if 'item_uuid' in f]
        _iuuids += [f['item'] for f in filters if 'item' in f]
        # remove duplicates
        _iuuids = list(set(_iuuids))
        logger.debug(_iuuids)
        _puuids = [f['product_uuid'] for f in filters
                   if 'product_uuid' in f]
        _puuids += [f['product'] for f in filters
                    if 'product' in f]
        logger.debug(_puuids)
        # Get by item uuid
        prods = Item.get_by_items_and_retailers(_iuuids, rets)
        '''for _iu in _iuuids:
            prods += Item.get_by_item(_iu, _cols)'''
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
                rets = [r['key']
                        for r in requests
                        .get(SRV_PROTOCOL + "://" + SRV_GEOLOCATION+'/retailer/all')
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

        chunk_size = int(2500 / (len(dates) + len(prods)))
        logger.info('chunk size')
        logger.info(chunk_size)
        logger.info('len(prods)')
        logger.info(len(prods))
        logger.info('chunk size before')
        logger.info(int(2500 / (len(dates))))

        # Fetch prod uuids
        puuids = [p['product_uuid'] for p in prods]
        chunk_puuids = Stats.divide_chunks(puuids, chunk_size)
        # Generate dates
        dates = sorted(dates)
        if len(dates) == 1:
            period = 2
        else:
            period = (dates[-1] - dates[0]).days + 1
        _days = tupleize_date(dates[0].date(), period)
        logger.info("Querying Stats by product..")
        logger.debug(puuids)
        logger.debug(_days)

        cass_query = """SELECT product_uuid, avg_price,
                min_price, max_price,
                mode_price, date
                FROM stats_by_product
                WHERE product_uuid in ({})
                AND date in {}"""

        qs = []

        for puuids in chunk_puuids:
            cass_query_text = cass_query.format(', '.join(puuids), str(_days))
            logger.debug(cass_query_text)

            try:
                q = g._db.query(cass_query_text,
                                timeout=2000)
                if q:
                    qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))

        logger.info("Fetched {} prices".format(len(qs)))
        logger.debug(qs[:1] if len(qs) > 1 else [])
        # Empty validation
        if len(qs) == 0:
            return pd.DataFrame({'date': [], 'product_uuid': []})
        # Load Response into a DF
        df = pd.DataFrame(qs)
        df['product_uuid'] = df.product_uuid.astype(str)
        return df

    @staticmethod
    def get_cassandra_by_retailers_and_period(prods, rets, dates):
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

        chunk_size = int(2500 / (len(dates) + len(prods)))
        logger.info('chunk size')
        logger.info(chunk_size)
        logger.info('len(prods)')
        logger.info(len(prods))
        logger.info('chunk size before')
        logger.info(int(2500 / (len(dates))))

        # Fetch prod uuids
        puuids = [p['product_uuid'] for p in prods]
        chunk_puuids = Stats.divide_chunks(puuids, chunk_size)
        # Generate dates
        dates = sorted(dates)
        if len(dates) == 1:
            period = 1
        else:
            period = (dates[-1] - dates[0]).days
        _days = tupleize_date(dates[-1].date(), period)
        date_start = int(dates[0].date().__str__().replace('-', ''))
        if date_start not in _days:
            _days = _days + (date_start,)

        cass_query = """SELECT product_uuid, avg_price,
                min_price, max_price,
                mode_price, date
                FROM stats_by_product
                WHERE product_uuid in ({})
                AND date in {}"""
    
        qs = []

        for puuids in chunk_puuids:
            cass_query_text = cass_query.format(', '.join(puuids), str(_days))
            logger.debug(cass_query_text)

            try:
                q = g._db.query(cass_query_text,
                                timeout=2000)
                if q:
                    qs += list(q)
            except Exception as e:
                logger.error("Cassandra Connection error: " + str(e))

        logger.info("Fetched {} prices".format(len(qs)))
        logger.debug(qs[:1] if len(qs) > 1 else [])
        # Empty validation
        if len(qs) == 0:
            return pd.DataFrame({'date': [], 'product_uuid': []})
        # Load Response into a DF
        df = pd.DataFrame(qs)
        df['product_uuid'] = df.product_uuid.astype(str)
        return df

    @staticmethod
    def get_actual_by_retailer_task(task_id, params):
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
        logger.info("Retailer current ")
        logger.debug(params)
        if not params['filters']:
            raise errors.TaskError("Not filters requested!")
        params = params['filters']
        logger.info('Entered Current by Retailer..')

        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        # Retailers from service
        rets = Stats.fetch_rets(params)
        if not rets:
            raise errors.TaskError("No retailers found.")
        logger.debug("Fetching data for: {}".format(rets))
        # Items from service
        filt_items = Stats.fetch_from_catalogue(params, rets)
        logger.debug("Prices of:  {}".format(filt_items))
        task.progress = 10
        if not filt_items:
            logger.warning("No Products found!")
            raise errors.TaskError("No products found")
        logger.info("Got filtered items..")
        _now = datetime.datetime.utcnow()
        # Products query
        df_curr = Stats\
            .get_cassandra_by_ret(filt_items,
                                  rets,
                                  [_now,
                                   _now - datetime.timedelta(days=1)])\
            .sort_values(by=['date'], ascending=False)\
            .drop_duplicates(subset=['product_uuid'], keep='first')  # today
        df_curr.rename(columns={'avg_price': 'avg',
                                'max_price': 'max', 'min_price': 'min',
                                'mode_price': 'mode'}, inplace=True)
        task.progress = 35
        df_prev = Stats\
            .get_cassandra_by_ret(filt_items,
                                  rets,
                                  [_now - datetime.timedelta(days=1),
                                   _now - datetime.timedelta(days=2)])\
            .sort_values(by=['date'], ascending=False)\
            .drop_duplicates(subset=['product_uuid'], keep='first')  # yesterday
        task.progress = 50
        # If queried lists empty
        if df_curr.empty:
            # Return empty set
            logger.warning('Empty set from query...')
            raise errors.TaskError("Empty set from query...")
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
        task.progress = 60
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
        # TODO:
        # Add rows with unmatched products!
        non_matched = df[df['item_uuid'].isnull() |
                         (df['item_uuid'] == '')].copy()
        # Format only products with matched results
        df = df[~(df['item_uuid'].isnull()) &
                (df['item_uuid'] != '')]
        formatted = []
        task.progress = 65
        for i, prdf in df.groupby(by=['item_uuid']):
            _first = prdf[:1].reset_index()
            tmp = {
                'item_uuid': _first.loc[0, 'item_uuid'],
                'name': _first.loc[0, 'name'],
                'gtin': _first.loc[0, 'gtin'],
                'prices': {}
            }
            for j, row in prdf.iterrows():
                _r = row.to_dict()
                del _r['source']
                del _r['date_x']
                del _r['date_y']
                tmp['prices'].update({
                    row['source']: _r
                })
            for r in (set(rets) - tmp['prices'].keys()):
                tmp['prices'].update({
                    r: {'avg': '-', 'min': '-',
                        'max': '-', 'mode': '-',
                        'prev_avg': '-', 'prev_min': '-',
                        'prev_max': '-', 'prev_mode': '-'
                        }
                })
            formatted.append(tmp)
        logger.info('Got actual!!')
        task.progress = 100
        return {"data": formatted, "msg": "Task completed"}


    @staticmethod
    def get_today_by_retailer_task(task_id, params):
        """ Retrieve today prices given a set of filters
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
        logger.info("Retailer today ")
        logger.debug(params)
        if not params['filters']:
            raise errors.TaskError("Not filters requested!")
        params = params['filters']
        logger.info('Entered Current by Retailer..')

        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        # Retailers from service
        rets = Stats.fetch_rets(params)
        if not rets:
            raise errors.TaskError("No retailers found.")
        logger.debug("Fetching data for: {}".format(rets))
        # Items from service
        filt_items = Stats.fetch_from_catalogue(params, rets)
        logger.debug("Prices of:  {}".format(filt_items))
        task.progress = 10
        if not filt_items:
            logger.warning("No Products found!")
            raise errors.TaskError("No products found")
        logger.info("Got filtered items..")
        _now = datetime.datetime.utcnow()
        print(_now)
        print(_now + datetime.timedelta(days=1))
        # Products query
        df_curr = Stats\
            .get_cassandra_by_ret(filt_items,
                                  rets,
                                  [_now + datetime.timedelta(days=1),
                                   _now + datetime.timedelta(days=2)])\
            .sort_values(by=['date'], ascending=False)\
            .drop_duplicates(subset=['product_uuid'], keep='first')  # today
        df_curr.rename(columns={'avg_price': 'avg',
                                'max_price': 'max', 'min_price': 'min',
                                'mode_price': 'mode'}, inplace=True)
        task.progress = 50
        # If queried lists empty
        if df_curr.empty:
            # Return empty set
            logger.warning('Empty set from query...')
            raise errors.TaskError("Empty set from query...")
        
        task.progress = 60
        # Add product attributes to Current prices DF
        info_df = pd.DataFrame(filt_items,
                               columns=['item_uuid', 'product_uuid',
                                        'name', 'gtin', 'source'])
        df = pd.merge(df_curr, info_df,
                           on='product_uuid', how='left')
        df.fillna('-', axis=0, inplace=True)
        # TODO:
        # Add rows with unmatched products!
        non_matched = df[df['item_uuid'].isnull() |
                         (df['item_uuid'] == '')].copy()
        # Format only products with matched results
        df = df[~(df['item_uuid'].isnull()) &
                (df['item_uuid'] != '')]
        formatted = []
        task.progress = 65
        for i, prdf in df.groupby(by=['item_uuid']):
            _first = prdf[:1].reset_index()
            tmp = {
                'item_uuid': _first.loc[0, 'item_uuid'],
                'name': _first.loc[0, 'name'],
                'gtin': _first.loc[0, 'gtin'],
                'prices': {}
            }
            for j, row in prdf.iterrows():
                _r = row.to_dict()
                del _r['source']
                tmp['prices'].update({
                    row['source']: _r
                })
            for r in (set(rets) - tmp['prices'].keys()):
                tmp['prices'].update({
                    r: {'avg': '-', 'min': '-',
                        'max': '-', 'mode': '-',
                        'prev_avg': '-', 'prev_min': '-',
                        'prev_max': '-', 'prev_mode': '-'
                        }
                })
            formatted.append(tmp)
        logger.info('Got today!!')
        task.progress = 100
        return {"data": formatted, "msg": "Task completed"}


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
                intd = [int(dtt)
                        for dtt in pd
                        .to_datetime('{}-{}'
                                     .format(intd[0],
                                             str(intd[1]).zfill(2))
                                     + '-0', format='%Y-%W-%w')
                        .date().__str__().split('-')]
            elif params['interval'] == 'month':
                lday = calendar.monthrange(*intd)[1]
                intd = [int(dtt)
                        for dtt in pd
                        .to_datetime('{}-{}'
                                     .format(intd[0],
                                             str(intd[1]).zfill(2))
                                     + '-{}'.format(lday), format='%Y-%m-%d')
                        .date().__str__().split('-')]
            else:
                # day
                pass
            d_belong = find_date_interval(pd.tslib
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
    def get_comparison_task(task_id, params):
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
        if not params:
            raise errors.TaskError("Not params requested!")
        if not params['filters']:
            raise errors.TaskError("Not filters requested!")
        if not params['interval']:
            raise errors.TaskError("Not interval requested!")
        if not params['date_start']:
            raise errors.TaskError("Not Start Date requested!")
        if not params['date_end']:
            raise errors.TaskError("Not Start Date requested!")
        logger.debug("Entered to Compare by period...")

        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        # Retailers from service
        rets = Stats.fetch_rets(params['filters'])
        if not rets:
            raise errors.TaskError("No retailers found.")
        # Products from service
        filt_items = Stats\
            .fetch_from_catalogue(params['filters'], rets)
        if not filt_items:
            logger.warning("No Products found!")
            raise errors.TaskError("No Products found!")
        logger.debug(filt_items)
        task.progress = 20
        # Date Grouping
        date_groups = grouping_periods(params)
        logger.info('Found grouped dates')
        # Retrieve prices from
        df = Stats.get_cassandra_by_retailers_and_period(filt_items,
                                                         rets, [date_groups[0][0], date_groups[-1][-1]])
        task.progress = 60
        if df.empty:
            logger.warning('Empty set from query...')
            raise errors.TaskError("Empty set from query...")
        # Parse datapoint date
        df['date'] = df['date'].apply(get_datetime())
        df['day'] = df['date'].apply(lambda x: x.day)
        df['month'] = df['date'].apply(lambda x: x.month)
        df['year'] = df['date'].apply(lambda x: x.year)
        df['week'] = df['date'].apply(lambda x: x.isocalendar()[1])
        grouping_cols = {'day': ['year', 'month', 'day'],
                         'month': ['year', 'month'],
                         'week': ['year', 'week']}
        task.progress = 65
        # Obtain all total amount of intervals
        interval_to_have = []
        # for ii, row_df in df.groupby(grouping_cols[params['interval']]):
        #     interval_to_have.append(ii)
        t_date_df = pd.date_range(
                        datetime.datetime.strptime(params['date_start'],'%Y-%m-%d'),
                        datetime.datetime.strptime(params['date_end'],'%Y-%m-%d')
                ).to_series().to_frame()
        t_date_df['day'] = t_date_df[0].apply(lambda x: x.day)
        t_date_df['month'] = t_date_df[0].apply(lambda x: x.month)
        t_date_df['year'] = t_date_df[0].apply(lambda x: x.year)
        t_date_df['week'] = t_date_df[0].apply(lambda x: x.isocalendar()[1])
        # for ii, row_df in df.groupby(grouping_cols[params['interval']]):
        #     interval_to_have.append(ii)
        for ii, row_df in t_date_df.groupby(grouping_cols[params['interval']]):
            interval_to_have.append(ii)
        task.progress = 75
        # Set Products DF
        info_df = pd.DataFrame(filt_items,
                               columns=['item_uuid', 'product_uuid',
                                        'name', 'gtin', 'source'])
        # Add product info
        df = pd.merge(df, info_df,
                      on='product_uuid', how='left')
        # TODO:
        # Add rows with unmatched products!
        non_matched = df[df['item_uuid'].isnull() |
                         (df['item_uuid'] == '')].copy()
        # Format only products with matched results
        df = df[~(df['item_uuid'].isnull()) &
                (df['item_uuid'] != '')]
        # Group by item and them depending on Date Range
        task.progress = 85

        interv_list = []
        item_uuid_groupby = df.groupby('item_uuid')
        loops_total = len(item_uuid_groupby)
        block_weight_total = 15
        delta = block_weight_total / loops_total
        block_progress = 0
        for i, tdf in item_uuid_groupby:
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
                        'retailer': ' '.join([ik[0].upper() + ik[1:]
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
                            in [x['retailer'] for x in tmp2['retailers']]:
                        continue
                    tmp2['retailers'].append({
                        'retailer': ' '.join([ik[0].upper() + ik[1:]
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
            block_progress += delta
            #task.progress = 85 + int(block_progress)
        task.progress = 100
        interv_list = jsonify(interv_list)
        return {"data": interv_list, "msg": "Task completed"}

    @staticmethod
    def convert_csv_actual(prod):
        """ Convert Data into CSV buffer
            with the Current Format
        """
        logger.debug("Converting list to csv file..")
        df = pd.DataFrame()
        for crow in prod:
            col_names = {'gtin': crow['gtin'],
                         'Nombre': crow['name']}
            col_names.update(dictify(crow['prices']))
            df = pd.concat([df, pd.DataFrame([col_names])])
        df.set_index(['gtin', 'Nombre'], inplace=True)
        #df.drop('item_uuid', axis=1, inplace=True)
        _buffer = StringIO()
        iocsv = df.to_csv(_buffer)
        _buffer.seek(0)
        return _buffer

    @staticmethod
    def convert_csv_market(prod):
        """ Convert Data into CSV buffer
            with the Market Format
        """
        logger.debug("Converting list to csv file..")
        df = pd.DataFrame()
        for crow in prod:
            recs = []
            for ivs in crow['intervals']:
                tmp = {'gtin': crow['gtin'],
                       'Nombre': crow['name']}
                tmp.update({'Fecha Inicio': ivs['date_start'],
                            'Fecha Final': ivs['date_end'],
                            'Mi Retailer': ivs['client']})
                for rs in ivs['retailers']:
                    rrs = readfy(rs['retailer'])
                    tmp.update({rrs: rs['price'],
                                rrs+' Diferencia': rs['difference']})
                recs.append(tmp)
            df = pd.concat([df, pd.DataFrame(recs)])
        df.set_index(['gtin', 'Nombre', 'Fecha Inicio',
                      'Fecha Final', 'Mi Retailer'], inplace=True)
        df.replace([0, 0.0], ['-', '-'], inplace=True)
        _buffer = StringIO()
        iocsv = df.to_csv(_buffer)
        _buffer.seek(0)
        return _buffer

    @staticmethod
    def get_historics(task_id, params):
        """ Retrieve historic prices given a set 
            of filters and sources to compare them 
            against a fixed source

            Params:
            -----
            params: dict
                Params with filters including
                (retailer, dates, or interval)

            Returns:
            -----
            formatted : list
                List of formatted values
        """
        if not params:
            raise errors.TaskError("No parameters passed!")
        if 'filters' not in params:
            raise errors.TaskError("No filters param passed!")
        if not ['filters']:
            raise errors.TaskError("No filters param passed!")

        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        logger.debug("Entered to extract Historic by period...")
        # Retailers from service
        rets = Stats.fetch_rets(params['filters'])
        if not rets:
            raise errors.TaskError("No retailers found.")
        task.progress = 10
        # Products from service
        filt_items = Stats\
            .fetch_from_catalogue(params['filters'], rets)
        if not filt_items:
            logger.warning("No Products found!")
            raise errors.TaskError("No Products found!")
        task.progress = 20
        # Date Grouping with not only ends
        params.update({'ends': False})
        date_groups = grouping_periods(params)
        logger.debug('Got grouping dates')
        # Query over all range
        range_dates = [date_groups[0][0], date_groups[-1][-1]]
        task.progress = 30
        df = Stats.get_cassandra_by_retailers_and_period(
            filt_items, rets, range_dates)
        if df.empty:
            raise errors.TaskError("No Prices found!")
        task.progress = 40
        # Products DF
        info_df = pd.DataFrame(filt_items,
                               columns=['item_uuid', 'product_uuid',
                                        'name', 'gtin', 'source'])
        # Obtaining Dates and formating
        df['date'] = df['date'].apply(get_datetime())
        df['time_js'] = df['date'].apply(lambda djs:
                                         (djs - datetime.datetime(1970, 1, 1, 0, 0))/datetime.timedelta(seconds=1)*1000)
        df['day'] = df['date'].apply(lambda x: x.day)
        df['month'] = df['date'].apply(lambda x: x.month)
        df['year'] = df['date'].apply(lambda x: x.year)
        df['week'] = df['date'].apply(lambda x: x.isocalendar()[1])
        task.progress = 50
        grouping_cols = {'day': ['year', 'month', 'day'],
                         'month': ['year', 'month'],
                         'week': ['year', 'week']}
        df_n = pd.merge(df, info_df,
                        on='product_uuid', how='left')
        task.progress = 60
        # TODO:
        # Add rows with unmatched products!
        # Review the use of the variables: non_matched and df
        # non_matched = df[df['item_uuid'].isnull() |
        #     (df['item_uuid'] == '')].copy()
        # # Format only products with matched results
        # df = df[~(df['item_uuid'].isnull()) &
        #     (df['item_uuid'] != '')]
        # --- Compute for Metrics
        # Group by interval
        avg_l, min_l, max_l = [], [], []
        for j, df_t in df_n.groupby(grouping_cols[params['interval']]):
            # Compute max, min, avg
            avg_l.append([
                list(df_t['time_js'])[0],
                df_t['avg_price'].mean()
            ])
            min_l.append([
                list(df_t['time_js'])[0],
                df_t['min_price'].mean(),
                df_t['avg_price'].mean()
            ])
            max_l.append([
                list(df_t['time_js'])[0],
                df_t['max_price'].mean(),
                df_t['avg_price'].mean()
            ])
        task.progress = 80
        logger.info('Got Metrics...')
        # --- Compute for Retailers
        retailers = []
        # Group by retailer
        for i, df_t in df_n.groupby('source'):
            tmp = {
                'name': ' '
                .join([rsp[0].upper() + rsp[1:]
                       for rsp in i.split('_')]),
                'data': []
            }
            # Group by time interval
            for j, df_p in df_t.groupby(grouping_cols[params['interval']]):
                tmp['data'].append([
                    df_p['time_js'].tolist()[0],
                    df_p['avg_price'].mean()
                ])
            retailers.append(tmp)
        logger.info('Got Retailers...')
        sub_str = "<b> Retailers:</b> " \
            + ', '.join([' '.join([rsp[0].upper() + rsp[1:]
                                   for rsp in rt.split('_')])
                         for rt in rets]) + '.'
        result = {
            'title': 'Tendencia de Precios',
            'subtitle': '<b>Periodo</b>: {} - {} <br> {}'.format(
                range_dates[0].isoformat(),
                range_dates[1].isoformat(),
                sub_str),
            'metrics': {
                'avg': avg_l,
                'min': min_l,
                'max': max_l
            },
            'retailers': retailers
        }
        task.progress = 100
        return {"data": result, "msg": "Task completed"}

    @staticmethod
    def get_count_by_cat(task_id, params):
        """ Retrieve all the count of the 
            elements in a categ  (given the 
            list of items).

            Params:
            ----
            filters: list
                Dicts of categories, items and retailers

            Returns:
            -----
            ccat : list
                List of Retailers with Category stats
        """
        if not params:
            raise errors.TaskError("No parameters passed!")
        if 'filters' not in params:
            raise errors.TaskError("No filters param passed!")
        if not ['filters']:
            raise errors.TaskError("No filters param passed!")

        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        params = params['filters']
        logger.debug("Retrieving stats by Retailer by given categ..")
        # Retailers from service
        rets = Stats.fetch_rets(params)
        if not rets:
            raise errors.TaskError("No retailers found.")
        task.progress = 10
        # Map item and item_uuid as products keys
        items = [{'item_uuid': iu['item']} for iu in params if 'item' in iu]
        params += items
        # Products from service
        filt_items = Stats\
            .fetch_from_catalogue(params, rets)
        task.progress = 30
        if not filt_items:
            logger.warning("No Products found!")
            raise errors.TaskError("No Products found!")
        # Set dates and retrieve info
        _dates = [datetime.datetime.utcnow()]
        #_dates.append(_dates[0] - datetime.timedelta(days=1))
        df = Stats\
            .get_cassandra_by_ret(filt_items,
                                  rets, _dates)
        task.progress = 50
        if df.empty:
            # logger.warning('No prices found!')
            # raise errors.TaskError("No prices found!")
            return {"data": [], "msg": "Task completed"}
        # Products DF
        info_df = pd.DataFrame(filt_items,
                               columns=['item_uuid', 'product_uuid',
                                        'name', 'gtin', 'source'])
        df = pd.merge(df, info_df,
                      on='product_uuid', how='left')
        task.progress = 70
        # TODO:
        # Add rows with unmatched products!
        non_matched = df[df['item_uuid'].isnull() |
                         (df['item_uuid'] == '')].copy()
        # Format only products with matched results
        df = df[~(df['item_uuid'].isnull()) &
                (df['item_uuid'] != '')]
        task.progress = 75
        # Perform aggregates
        ccat, counter, digs = [], 1, 1
        for i, row in df.groupby('source'):
            prod_count = row\
                .drop_duplicates(['item_uuid'])\
                .avg_price.count()
            prod_avg = row\
                .drop_duplicates(['item_uuid'])\
                .avg_price.mean()
            ccat.append({
                'x': counter,
                'name': i,
                'retailer': " "
                        .join([x[0].upper()+x[1:]
                               for x in i.split('_')]),
                        'z': round(float(prod_count), 2),
                        'y': round(float(prod_avg), 2)
                        })
            counter += 1
            # Save biggest number of digits
            digs = digs if digs > len(
                str(prod_count)) else len(str(prod_count))
        # Scaling x for plot upon the number of digits
        task.progress = 95
        for i, xc in enumerate(ccat):
            ccat[i]['x'] = xc['x']*(10**(digs))
        logger.info('Got Category counts')
        task.progress = 100
        return {"data": ccat, "msg": "Task completed"}

    @staticmethod
    def stats_by_uuid(uuid, stats):
        """ Retrieve the stats requested from an specific uuid

            Params:
            ----
            uuid: <str> item_uuid or product_uuid of an item
            stats: <str> stats that are going to be in the cassandra query

            Returns:
            -----
            stats dict
        """
        logger.debug("Retrieving stats by uuid..")
        # Retailers from service
        try:
            items = requests.get(
                SRV_PROTOCOL + "://" + SRV_CATALOGUE + "/product/by/iuuid?keys={uuid}&ipp=50&cols=product_uuid".format(
                    uuid=uuid)).json()
            dates = [
                str(datetime.date.today()). replace("-", ""),
                str(datetime.date.today() +
                    datetime.timedelta(days=-1)).replace("-", ""),
                str(datetime.date.today() +
                    datetime.timedelta(days=-2)).replace("-", "")
            ]
            product_uuids = [item.get("product_uuid")
                             for item in items.get("products")]
            if not product_uuids:
                logger.debug("product_uuid found")
                product_uuids = [uuid]
            else:
                logger.debug("item_uuid found")
        except Exception as e:
            logger.error(
                "Stats by uuid is not working correctly! : {}".format(e))
            logger.error("Url: {}".format(SRV_PROTOCOL + "://" + SRV_CATALOGUE + "/product/by/iuuid?keys={uuid}&ipp=50&cols=product_uuid".format(
                uuid=uuid)))
            product_uuids = []

        stats_json = {}

        if product_uuids:
            logger.debug("Finding prices...")
            stats_script = [stat + "_price as " + stat for stat in stats]
            aux = """ 
                SELECT {stats} 
                    FROM stats_by_product 
                    WHERE product_uuid IN ({items}) 
                        AND date IN ({dates})  
            """.format(
                stats=",".join(stats_script),
                items=",".join(product_uuids),
                dates=",".join(dates)
            )
            logger.debug(aux)
            cass = g._db
            rows = cass.execute(aux)
            df = pd.DataFrame(list(rows))
            if not df.empty:
                if "max" in stats:
                    stats_json["max"] = round(max(df["max"]), 2)
                if "min" in stats:
                    stats_json["min"] = round(min(df["min"]), 2)
                if "avg" in stats:
                    stats_json["avg"] = round(
                        sum(df["avg"]) / len(df["avg"]), 2)
            else:
                if "max" in stats:
                    stats_json["max"] = None
                if "min" in stats:
                    stats_json["min"] = None
                if "avg" in stats:
                    stats_json["avg"] = None
            return stats_json
        else:
            if "max" in stats:
                stats_json["max"] = None
            if "min" in stats:
                stats_json["min"] = None
            if "avg" in stats:
                stats_json["avg"] = None
            logger.error(
                "Something is going wrong with stats_by_uuid in geoprice, FIX IT!!")

        return stats_json

    @staticmethod
    def exists_by_uuid(uuid):
        """ Retrieve the stats requested from an specific uuid

            Params:
            ----
            uuid: <str> item_uuid or product_uuid of an item
            stats: <str> stats that are going to be in the cassandra query

            Returns:
            -----
            stats dict
        """
        logger.debug("Retrieving stats by uuid..")
        # Retailers from service
        try:
            items = requests.get(
                SRV_PROTOCOL + "://" + SRV_CATALOGUE + "/product/by/iuuid?keys={uuid}&ipp=50&cols=product_uuid".format(
                    uuid=uuid)).json()
            dates = [
                str(datetime.date.today()). replace("-", ""),
                str(datetime.date.today() +
                    datetime.timedelta(days=-1)).replace("-", ""),
                str(datetime.date.today() +
                    datetime.timedelta(days=-2)).replace("-", "")
            ]
            product_uuids = [item.get("product_uuid")
                             for item in items.get("products")]
            if not product_uuids:
                logger.debug("product_uuid found")
                product_uuids = [uuid]
            else:
                logger.debug("item_uuid found")
        except Exception as e:
            logger.error(
                "Exists by uuid is not working correctly! : {}".format(e))
            logger.error("Url: {}".format(SRV_PROTOCOL + "://" + SRV_CATALOGUE + "/product/by/iuuid?keys={uuid}&ipp=50&cols=product_uuid".format(
                uuid=uuid)))
            product_uuids = []

        if product_uuids:
            logger.debug("Finding prices...")
            aux = """ 
                SELECT avg_price
                    FROM stats_by_product
                    WHERE product_uuid IN ({items}) 
                        AND date IN ({dates})
                    LIMIT 1  
            """.format(
                items=",".join(product_uuids),
                dates=",".join(dates)
            )
            cass = g._db
            rows = cass.execute(aux)
            df = pd.DataFrame(list(rows))
            if not df.empty:
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def get_matched_items_task(task_id, params):
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
        if not params:
            raise errors.TaskError("No parameters passed!")
        if 'filters' not in params:
            raise errors.TaskError("No filters param passed!")
        if not ['filters']:
            raise errors.TaskError("No filters param passed!")

        task = Task(task_id)
        task.task_id = task_id
        task.progress = 1

        items = [{'item_uuid': iu['item']}
                 for iu in params['filters'] if 'item' in iu]
        rets = Stats.fetch_rets(params['filters'])
        logger.debug("Fetching data for: {}".format(rets))
        filt_items = Stats \
            .fetch_from_catalogue(params['filters'], rets)
        logger.debug("Product data : {}".format(rets))
        if not filt_items:
            logger.warning("No Products found!")
            raise errors.TaskError("No Products found!")
        date_groups = grouping_periods(params)
        logger.debug('Got grouping dates')
        # Query over all range
        range_dates = [date_groups[0][0], date_groups[-1][-1]]
        qres = Stats.get_cassandra_by_retailers_and_period(
            filt_items,
            rets,
            range_dates)  # today
        task.progress = 10
        if len(qres) == 0:
            logger.warning('No prices found!')
            raise errors.TaskError("No prices found!")
        df = pd.DataFrame(qres)
        prods_by_uuids = {p['product_uuid']: p for p in filt_items}
        df['item_uuid'] = df.product_uuid.apply(
            lambda z: prods_by_uuids[z]['item_uuid'])
        df['retailer'] = df.product_uuid.apply(
            lambda z: prods_by_uuids[z]['source'])
        task.progress = 15
        # For every item, check availability in every retaliers
        # If not, delete from set
        filtered = []
        rejected = []
        item_uuids = [i['item_uuid'] for i in items if 'item_uuid' in i]
        for it in item_uuids:
            try:
                # If not in all retailers, pass
                logger.debug("item {} found in {} out of {} retailers".format(
                    it,
                    df[df['item_uuid'] == UUID(it)].groupby(
                        'retailer').size().shape[0],
                    len(rets)))
                if df[df['item_uuid'] == UUID(it)].groupby('retailer').size().shape[0] != len(rets):
                    rejected.append(UUID(it))
                    continue
            except Exception as e:
                logger.error(e)
                pass

        if rejected:
            df_new = df[~df['item_uuid'].isin(rejected)]
        task.progress = 50
        # logger.debug(df.head())
        ccat, counter, digs = [], 1, 1
        data = {}
        prices_per_retailer = dd()
        for i, row in df_new.groupby('retailer'):
            prod_count = row.drop_duplicates(['item_uuid'])[
                'avg_price'].count()
            prod_avg = row.drop_duplicates(['item_uuid'])['avg_price'].mean()
            # All item prices
            prices = {}
            for j, prod in row.drop_duplicates(['item_uuid']).iterrows():
                prices[str(prod['item_uuid'])] = prod['avg_price']
                prices_per_retailer[str(prod['item_uuid'])
                                    ][i] = prod['avg_price']

            # Retailer prices
            data[i] = prices
            ccat.append({
                'x': round(float(prod_avg), 2),
                'name': i,
                'ret': " ".join([x[0].upper() + x[1:] for x in i.split('_')]),
                'retailer': " ".join([x[0].upper() + x[1:] for x in i.split('_')]),
                'z': round(float(prod_count), 2),
                'prods': round(float(prod_count), 2),
                'y': 50,
                'prices': prices
            })
            counter += 1

        logger.info('Got Category counts')
        task.progress = 100
        result = {
            "graph": ccat,
            "items": list(set([str(iu) for iu in list(df_new['item_uuid'])])),
            "data": dd_to_dict(prices_per_retailer)
        }

        return {"data": result, "msg": "Task completed"}


    def divide_chunks(l, n): 
      
        # looping till length l 
        for i in range(0, len(l), n):  
            yield l[i:i + n] 
