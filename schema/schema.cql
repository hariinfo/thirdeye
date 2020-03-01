CREATE KEYSPACE thirdeye_test WITH replication =
  {‘class’: ‘SimpleStrategy’, ‘replication_factor’ : 3};