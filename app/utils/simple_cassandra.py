import sys
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, PreparedStatement, bind_params
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args
import logging

logger = logging.getLogger(__name__)


class SimpleCassandra(object):

    def __init__(self, config, **kwargs):
        self.config = config
        auth_provider = None

        # Kwargs
        self.autocommit = kwargs.get('autocommit', False)
        if not self.config:
            raise Exception("Configuration variables missing",'Missing vars in config')

        # Essential config variables
        self.config = {
            "CONTACT_POINTS" : config["CONTACT_POINTS"],
            "KEYSPACE" : None if "KEYSPACE" not in config else config['KEYSPACE'],
            "PORT" : "9042" if "PORT" not in config else config['PORT'],
            "TIMEOUT" : 30 if "TIMEOUT" not in config else config['TIMEOUT'],
            "CONSISTENCY_LEVEL" : "QUORUM" if "CONSISTENCY_LEVEL" not in config else config['CONSISTENCY_LEVEL'],
        }     
        # Auth
        if 'USER' in config and 'PASSWORD' in config:
            if config['USER'] and config['PASSWORD']:
                auth_provider = PlainTextAuthProvider(
                    username=config['USER'], 
                    password=config['PASSWORD']
                )   

        # Cluster
        if auth_provider:
            self.cluster = Cluster(
                self.config['CONTACT_POINTS'],
                port=self.config['PORT'],
                connect_timeout=60,
                auth_provider=auth_provider
            )
        else:
            self.cluster = Cluster(
                self.config['CONTACT_POINTS'],
                port=self.config['PORT'],
                connect_timeout=60
            )
        # Set session
        try:
            # If keyspac is set, connect...
            self.session = self.cluster.connect()
            if self.config['KEYSPACE']:
                self.set_keyspace()
        except Exception as e:
            logger.error("Something happened in SimpleCassandra connection")
            logger.error(self.config)
            logger.error(e)
            sys.exit()


    def set_keyspace(self):
        """ Once connecting, get the session
        """
        logger.info("Getting cassandra session...")
        if not hasattr(self,'session'):
            return None
        self.session.set_keyspace(self.config['KEYSPACE'])
        cl = ConsistencyLevel.ONE
        # Consistency level
        if self.config['CONSISTENCY_LEVEL'] == 'LOCAL_ONE':
            cl = ConsistencyLevel.LOCAL_ONE
        if self.config['CONSISTENCY_LEVEL'] == 'ONE':
            cl = ConsistencyLevel.ONE
        elif self.config['CONSISTENCY_LEVEL'] == 'QUORUM':
            cl = ConsistencyLevel.QUORUM
        elif self.config['CONSISTENCY_LEVEL'] == 'ALL':
            cl = ConsistencyLevel.ALL
        self.session.default_consistency_level = cl  
        return self.session


    def close(self):
        self.cluster.shutdown()


    def execute(self, qry, args=(), timeout=200):
        """ Cassandra simple execute statement
        """
        result = self.session.execute(qry, args, timeout=timeout)
        return result


    def query(self, qry, params=(), size=5000, timeout=30, consistency=None):
        """ Cassandra query with pagination
            @Params:
                - qry {str}: cassandra query
                - size {int}: size of the query batch
        """
        result = []
        consistency = consistency if consistency else self.session.default_consistency_level
        statement = SimpleStatement(
            qry, 
            fetch_size=size,
            consistency_level=consistency
        )
        for row in self.session.execute(statement, params, timeout=timeout):
            result.append(row)
        return result



    def query_concurrent(self, query, params):
        """ Execute query concurrently
            - query <str>: (WHERE = ?)
        """
        statement = self.session.prepare(query)
        result = list(execute_concurrent_with_args(self.session, statement, params))
        return result



    def query_async(self, qry, args=(""), lst=[]):
        """ Execute async queries, best for large 
            volume of data queries
        """
        prepared = self.session.prepare(qry)
        futures = []
        if len(args) > 0:
            for a in args:
                # Bind and execute
                bound = prepared.bind((a,))
                futures.append(self.session.execute_async(bound))
        else:
            bound = prepared
            futures.append(self.session.execute_async(bound))
            
        results = []
        for f in futures:
            try:
                r = f.result()
                results += r
            except Exception as e:
                logger.error(e)
                continue

        return results



class PagedHandler(object):

    def __init__(self, future):
        self.error = None
        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_err
        )

    def finished_event(self):
        #print("Finished event")
        pass

    def handle_page(self, rows):
        for row in rows:
            #print(row)
            pass

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()