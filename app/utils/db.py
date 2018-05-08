#-*-coding: utf-8-*-
import sys
from app.models.simple_cassandra import SimpleCassandra
import config
import app.applogger as applogger
from app.errors import AppError

cluster = None
session = None
logger = applogger.get_logger()

# Create the database and schema
def initdb():
    ''' Initialize the db '''
    logger.info("Initializing keyspace. Host: "+str(CASSANDRA_CONTACT_POINTS)+" / Keyspace: "+CASSANDRA_KEYSPACE)
    cluster_init = Cluster(CASSANDRA_CONTACT_POINTS)
    session_init = cluster_init.connect()
    # Only drop keyspace if its in testing environmet
    if config.TESTING:
        session_init.execute("DROP KEYSPACE IF EXISTS {}".format(config.CASSANDRA_KEYSPACE))

    if config.ENV.upper() == 'DEV' or config.ENV.upper() == 'LOCAL':
        session_init.execute("""
            CREATE KEYSPACE %s 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} 
            AND durable_writes = true
        """ % config.CASSANDRA_KEYSPACE )
    else:
        session_init.execute("""
            CREATE KEYSPACE %s 
            WITH replication = {'class': 'NetworkTopologyStrategy', 'Core': '1', 'Analytics': '1'}  
            AND durable_writes = true
        """ % config.CASSANDRA_KEYSPACE ) 

    session_init.set_keyspace(config.CASSANDRA_KEYSPACE)
    with open( BASE_DIR + '/schema.cql','r') as f:
        #cont = f.read()
        commands = []
        cmd_str = ""
        cmd=0
        # Loop lines in the file
        for line in f:
            logger.info(line)
            if "command start" in line:
                cmd=1
                continue
            if "command end" in line:
                commands.append(cmd_str)
                cmd_str=""
                cmd=0
                continue
            if cmd==1:
                cmd_str+=line

        for c in commands:
            logger.info(c)
            session_init.execute(c)

    cluster_init.shutdown()
    logger.info("Keyspace successfully initialized")
    return True

def connectdb():
    """ Connect to Cassandra through SC
    """
    global cluster
    cass = SimpleCassandra(dict(
        CONTACT_POINTS=config.CASSANDRA_CONTACT_POINTS,
        PORT=config.CASSANDRA_PORT,
        CONSISTENCY_LEVEL="LOCAL_ONE"
    ))
    return cass.session


def getdb():
    """ Opens a new database connection if there is none yet for the
        current application context.
    """
    global session
    logger.info("Connecting to {}...".format(config.CASSANDRA_KEYSPACE))
    cass = SimpleCassandra(dict(
        CONTACT_POINTS=config.CASSANDRA_CONTACT_POINTS,
        KEYSPACE=config.CASSANDRA_KEYSPACE,
        CONSISTENCY_LEVEL="LOCAL_ONE"
    ))
    return cass

