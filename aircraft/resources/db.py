from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine import connection

class db:

    def __init__(self):
        # Apache Cassandra connection
        list_of_ip = ['192.168.56.101']
        cluster = Cluster(list_of_ip,load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))

    def getConnection(self):
        session = cluster.connect()
        session.set_keyspace('thirdeye_test')
        return connection.set_session(session)