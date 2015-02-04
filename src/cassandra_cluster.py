from cassandra.cluster import Cluster


class CassandraConnectionError(Exception):
    pass

class CassandraCluster(object):
    _instance = None
    def __new__(cls, cluster_nodes, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CassandraCluster, cls).__new__(cls, cluster_nodes, *args, **kwargs)
        return cls._instance

    def __init__(self ,cluster_node, *args, **kwargs):
        self.node_list = cluster_node
        self.cassandra_cluster = None
        self.test = 'SELECT now() FROM system.local;'
        self.session = None
    
    def create_session(self):
        try:
            self.cassandra_cluster = Cluster(self.node_list)
            self.session = self.cassandra_cluster.connect()
        except Exception, exception_message:
            self.cassandra_session = None
            print "vivek !!!could not create session " + str(exception_message)
            raise CassandraConnectionError, "CASSANDRA_CLUSTER_CONNECTION_FAILEDvivekkkk:{exception_message}".format(exception_message=exception_message)
            
    def _is_valid_connection(self):
        try:
            if self.session:
                try:
                    ping_result = self.session.execute(self.test)
                    ping_result[0]
                    return True
                except Exception, exception_message:
                    self.close_cassandra_session()
                    print 'Exception in ping %s' %(exception_message)
        except Exception, exception_message:
            print 'Exception !!! no session present ... not a valid connection %s' %(exception_message)
        return False
        
    def execute(self, cql_query):
        if not self._is_valid_connection():
            self.create_session()
        return self.session.execute(cql_query)
        
    def execute_async_cassandra_request(self, query):
        if not self._is_valid_connection():
            self.create_session()
        query_response_future = self.session.execute_async(query)
        query_response_future.add_callbacks(self.log_results, self.log_errors)
        return query_response_future.result()

    def log_results(self, results):
        if results:
            print "Cassandra response consists of %s results lines" %(len(results))
            return results

    def log_errors(self, exception_message):
        print "Cassandra Error in excuting async query ---exp-message =  %s" % (exception_message)
        
    def close_cassandra_session(self):
        if self.session:
            try:
                self.session.cluster.shutdown()
                self.session.shutdown()
            except Exception, exception_message:
                print "Error shutting down connection ---exception-message =  %s" % (exception_message)
            finally:
                self.session = None
                
    def __del__(self):
        self.close_cassandra_session()
if __name__ == "__main__":
    cc = CassandraCluster(['127.0.0.1'])
    cc.create_session()
    p = cc._is_valid_connection()
    print p
