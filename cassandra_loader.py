# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 18:11:22 2020

@author: harii
"""
#pip install cassandra-driver

# Using tablesplus https://tableplus.com/ as my NoSQL Client to quickly run queries rather than using CQL which is only available on the cassandra node
#SimpleStrategy is used as we assume this to be single DC setup

#https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra/50508046

from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cqlengine.columns import *
from tqdm import tqdm
from cassandra.cqlengine import columns


## Object Mapper
class airlineontime(Model):
#  __keyspace__ = 'thirdeye_test'
  __table_name__ = 'airlineontime'
  id = columns.UUID(primary_key=True)
  flight_date = columns.Date()
  reporting_airline = columns.Text()
  tail_number = columns.Text()
  originairportid = columns.Integer()
  destairportid = columns.Integer()


# Apache Cassandra connection
list_of_ip = ['192.168.56.101']
cluster = Cluster(list_of_ip,load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
session = cluster.connect()
session.set_keyspace('thirdeye_test')
connection.set_session(session)

import glob
import pandas as pd
import datetime as dt

# get data file names
path =r'./input/airlines/performance/2017/test'
filenames = glob.glob(path + "/*.csv")

# ETL Tasks
#1. Convert CRSDepTime and CRSArrTime into hhmm format as certain datasets are single digit E.g. 5 will be translated to 0005
#2. Validate null columns
#3. Validate dattime format
#4. Enrich by adding a few columns

dfs = []
for filename in filenames:
    dfs.append(pd.read_csv(filename, converters={'CRSDepTime': '{:0>4}'.format,'CRSArrTime': '{:0>4}'.format}))

# Concatenate all months of data into one DataFrame
aircraft_performance = pd.concat(dfs, ignore_index=True)
aircraft_performance = aircraft_performance.dropna(subset=['Tail_Number','Reporting_Airline','OriginAirportID','DestAirportID'])

#for ind, row in tqdm(aircraft_performance.iterrows(), total=aircraft_performance.shape[0]):
#    if pd.isna(row['Tail_Number']) == True:
#        print(row['Tail_Number'])

## saving data to database
for ind, row in tqdm(aircraft_performance.iterrows(), total=aircraft_performance.shape[0]):
  airlineontime.create(
    flight_date = row['FlightDate'],      
    reporting_airline = row['Reporting_Airline'],
    tail_number = row['Tail_Number'],
    originairportid = row['OriginAirportID'],
    destairportid = row['DestAirportID']
    )