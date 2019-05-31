import boto3
from config import *
from app import logger, errors
from io import StringIO
import pandas as pd


# Aux Vars
BUCKET = "geoprice"

class Dump(object):
    """ Class to retrieve and put objects into S3
    """

    def get_recent_from_s3(fname):
        """ Get most recent existance of the file name
            given in S3 for hte given environment

            Params:
            -----
            fname: str
                Filename to check
        """
        s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        _key = ENV.lower() + "/" + fname
        # Fetch file
        try:
            remote_r = s3.get_object(
                Bucket=BUCKET,
                Key=_key
            )
            fbuff = remote_r['Body']
            df = pd.read_csv(fbuff).drop('Unnamed: 0', axis=1, errors='ignore')
        except Exception:
            logger.warning("Not found file!")
            raise errors.AppError("no_file_found", "Not found dump file in S3")
        return df

    @staticmethod
    def df_to_s3(df, source):
        """ Save dataframe directly to s3, for quick access
            to the most recent information
            File Path:   s3://<BUCKET>/<ENV>/<SOURCE>_stats_aggregate.csv

            Params:
            ----
            df: pd.DataFrame
                Data DataFrame
            source: str
                Source key
        """
        now = datetime.datetime.utcnow()
        # Key for bucket
        filename = ENV.lower() \
            + "/" + source + "_stats_aggregate.csv"
        bucket = BUCKET
        try:
            # Generate buffer
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            # Put object
            s3.put_object(
                Bucket=bucket,
                Key=filename,
                Body=csv_buffer.getvalue()
            )
            logger.info("Correctly stored file in S3!")
            return True
        except Exception as e:
            logger.warning("Could not save file to s3!")
            logger.error(e)
            return False
        

    def df_to_s3_historic(df, source):
        """ Save dataframe directly to s3 to save history
            File Path:   s3://<BUCKET>/<ENV>/<YEAR>/<MONTH>/<DAY>/<SOURCE>_stats_aggregate.csv

            Params:
            ----
            df: pd.DataFrame
                Data DataFrame
            source: str
                Source key
        """
        now = datetime.datetime.utcnow()
        # Key for bucket
        filename = ENV.lower() \
            + "/" + now.strftime("%Y")\
            + "/" + now.strftime("%m") \
            + "/" + now.strftime("%d") \
            + "/" + source + "_stats_aggregate.csv"
        bucket = BUCKET
        try:
            # Generate buffer
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            # Put object
            s3.put_object(
                Bucket=bucket,
                Key=filename,
                Body=csv_buffer.getvalue()
            )
            logger.info("Correctly stored historic file in S3!")
            return True
        except Exception as e:
            logger.warning("Could not save historic file to s3!")
            logger.error(e)
            return False