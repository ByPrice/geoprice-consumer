#-*-coding: utf-8-*-
import sys
from cassandra import AlreadyExists
from app.utils.simple_cassandra import SimpleCassandra
import ByHelpers.applogger as applogger
from app.utils.errors import AppError
import config

cluster = None
session = None
logger = applogger.get_logger()

# Create the database and schema
def initdb():
    ''' Initialize the db '''
    logger.info("Initializing keyspace. Host: " + \
        str(config.CASSANDRA_CONTACT_POINTS)+" / Keyspace: "+\
        config.CASSANDRA_KEYSPACE)

    cass = SimpleCassandra(dict(
        CONTACT_POINTS=config.CASSANDRA_CONTACT_POINTS,
        PORT=config.CASSANDRA_PORT,
        CONSISTENCY_LEVEL="LOCAL_ONE",
        USER=config.CASSANDRA_USER,
        PASSWORD=config.CASSANDRA_PASSWORD
    ))
    cluster_init = cass.cluster
    session_init = cass.session
    logger.info("Correctly connected to Cassandra!")
    
    # Only drop keyspace if its in testing environmet
    if config.TESTING:
        logger.info("Dropping testing keyspace")
        session_init.execute("DROP KEYSPACE IF EXISTS {}".format(config.CASSANDRA_KEYSPACE))

    if config.ENV.upper() == 'LOCAL':
        try:
            logger.info("Creating keyspace...")
            session_init.execute("""
                CREATE KEYSPACE %s 
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} 
                AND durable_writes = true
            """ % config.CASSANDRA_KEYSPACE )
        except AlreadyExists:
            logger.info("Keyspace {} already exists".format(config.CASSANDRA_KEYSPACE))
            return True
    else:
        try:
            logger.info("Creating keyspace...")
            session_init.execute("""
                CREATE KEYSPACE %s 
                WITH replication = {'class': 'NetworkTopologyStrategy', 'us-west1': '2'}  
                AND durable_writes = true
            """ % config.CASSANDRA_KEYSPACE ) 
        except AlreadyExists:
            logger.info("Keyspace {} already exists".format(config.CASSANDRA_KEYSPACE))
            return True
    # Set Keyspace
    session_init.set_keyspace(config.CASSANDRA_KEYSPACE)
    logger.info("Created {} keyspace!".format(config.CASSANDRA_KEYSPACE))
    with open( config.BASE_DIR + '/schema.cql','r') as f:
        #cont = f.read()
        commands = []
        cmd_str = ""
        cmd, comnt = 0, False
        # Loop lines in the file
        for line in f:
            #logger.info(line)
            if "command start" in line:
                cmd=1
                continue
            if '/*' in line:
                comnt ^= True  # Switch to comment ON
            if '*/' in line:
                comnt ^= True  # Switch to comment OFF
            if "command end" in line:
                logger.info('\n'+cmd_str)
                #commands.append(cmd_str)
                session_init.execute(cmd_str, timeout=120)
                cmd_str=""
                cmd=0
                continue
            if cmd==1:
                if comnt:  
                    continue  # Skip comments
                if '/*' in line and '*/' in line:
                    continue  # Skip inline comments
                cmd_str+=line
    cluster_init.shutdown()
    logger.info("Keyspace successfully initialized")
    return True

def connectdb():
    """ Connect to Cassandra through SimpleCassandra
    """
    global cluster
    cass = SimpleCassandra(dict(
        CONTACT_POINTS=config.CASSANDRA_CONTACT_POINTS,
        PORT=config.CASSANDRA_PORT,
        CONSISTENCY_LEVEL="LOCAL_ONE",
        USER=config.CASSANDRA_USER,
        PASSWORD=config.CASSANDRA_PASSWORD
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
        PORT=config.CASSANDRA_PORT,
        KEYSPACE=config.CASSANDRA_KEYSPACE,
        CONSISTENCY_LEVEL="LOCAL_ONE",
        USER=config.CASSANDRA_USER,
        PASSWORD=config.CASSANDRA_PASSWORD
    ))
    return cass

