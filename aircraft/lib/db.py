"""
- Module to manage cassandra connection
"""

from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine import connection
from lib import configloader

class db:
    def __init__(self):
        # Apache Cassandra connection
        parser = configloader.ConfigParser()
        parser.read_ini_file()
        print(parser.getKey('DEFAULT','ip'))
        list_of_ip = ['192.168.56.101']
        self.cluster = Cluster(list_of_ip,load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))

    def getConnection(self):
        session = self.cluster.connect()
        session.set_keyspace('thirdeye_test')
        return connection.set_session(session)
    
    def closeConnection(self):
        self.connection.closeConnection
