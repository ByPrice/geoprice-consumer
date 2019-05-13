from flask import Response, send_file
from io import StringIO, BytesIO
import pandas as pd
from app import errors
from ByHelpers import applogger

# Logger
logger = applogger.get_logger()

def load_maps_df(data):
    """ Method to load Map/Table 
        specific format into a DF

        Params:
        -----
        - data: (dict) JSON formated prices

        Returns:
        ----- 
        (pandas.DataFrame) DF with respective columns of Table
    """
    # To create file only **tabla** is taken into account
    df = pd.DataFrame()
    _tabla = data['tabla']
    for ret in _tabla:
        tmp = pd.DataFrame(_tabla[ret])
        tmp['retailer'] = ret
        df = pd.concat([df, tmp])
    return df

def file_response(data, m_type, extension):
    """ Method to construct CSV/Excel Exports

        Params:
        -----
        - data : (dict) JSON data 
        - m_type : (str) Task Method type
        - extension : (str) Extension file to build

        Returns:
        -----
        (flask.Response) Formated Response as MIME type
    """
    logger.info('Generating {} response..'.format(extension))
    df = None
    # Map/Table Case
    if m_type == 'prices_map':
        try:
            df = load_maps_df(data)
        except Exception as e:
            logger.warning('Could not fetch {} result!'\
                            .format(extension.upper()))
            logger.error(e)
            raise errors.AppError(40005,
                        'Issues fetching {} results'\
                        .format(extension.upper()))
    # Any other case not available
    else: 
        logger.warning ('Task method not available!')
        raise errors.AppError(40006,
                'Task Method not available ')
    if extension.lower() == 'excel':
        # Excel Wrapper
        # Building IO Wrapper
        _buffer = BytesIO()
        df.to_excel(_buffer)
        _mime = "application/vnd.ms-excel"
        _ext = 'xlsx'
    else:
        # CSV Wrapper
        # Building IO Wrapper
        _buffer = StringIO()
        io_wrapper = df.to_csv(_buffer)
        _mime = 'text/csv' 
        _ext = 'csv'
    # Creating Filename
    fname = '{}.{}'.format(m_type, _ext)
    _buffer.seek(0)
    # Returning Response
    logger.info('Serving {} response...'.format(extension.upper()))
    return Response(_buffer,
                mimetype=_mime,
                headers={"Content-disposition": "attachment; \
                        filename={}".format(fname)})


def download_dataframe(dataframe, fmt="csv", name="default"):
    """ Generate response given a dataframe
    """
    if fmt.lower() == 'excel':
        # Excel Wrapper
        # Building IO Wrapper
        _buffer = BytesIO()
        dataframe.to_excel(_buffer)
        _mime = "application/vnd.ms-excel"
        _ext = 'xlsx'
    else:
        # CSV Wrapper
        # Building IO Wrapper
        _buffer = StringIO()
        io_wrapper = dataframe.to_csv(_buffer)
        _mime = 'text/csv' 
        _ext = 'csv'

    # Creating Filename
    fname = '{}.{}'.format(name, _ext)
    _buffer.seek(0)
    # Returning Response
    logger.info('Serving {} response...'.format(fmt.upper()))
    return Response(_buffer,
                mimetype=_mime,
                headers={"Content-disposition": "attachment; \
                        filename={}".format(fname)})