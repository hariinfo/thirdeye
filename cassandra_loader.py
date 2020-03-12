# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 18:11:22 2020

@author: harii
"""
#pip install cassandra-driver

# Using tablesplus https://tableplus.com/ as my NoSQL Client to quickly run queries rather than using CQL which is only available on the cassandra node
#SimpleStrategy is used as we assume this to be single DC setup

#https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra/50508046
from src.lib import db 
from src.model import airlineontime 
from cassandra.cluster import Cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cqlengine.columns import *
from tqdm import tqdm

#from confluent_kafka import Producer



import glob
import pandas as pd
import datetime as dt
import sys

def airline_ownership (row):
      if row['Reporting_Airline'] == 'DL' :
           return 'Public'
      if row['Reporting_Airline'] == 'AA' :
           return 'Public'
      if row['Reporting_Airline'] == 'UA' :
           return 'Public'
      if row['Reporting_Airline'] == 'LUV' :
          return 'Public'
      return 'Private'

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
#aircraft_performance['CRSDepTime'] = pd.to_datetime(aircraft_performance['CRSDepTime'], format='%H%M', exact=False)
#aircraft_performance['CRSArrTime'] = pd.to_datetime(aircraft_performance['CRSArrTime'], format='%H%M', exact=False)

#aircraft_performance['FlightArrDateTime'] = pd.to_datetime(aircraft_performance.FlightDate.astype(str)+' '+aircraft_performance.CRSArrTime.astype(str))
#aircraft_performance['FlightDepDateTime'] = pd.to_datetime(aircraft_performance.FlightDate.astype(str)+' '+aircraft_performance.CRSDepTime.astype(str))

%timeit aircraft_performance['ownership'] = aircraft_performance.apply(lambda row: airline_ownership(row), axis=1)

# Concatenate all months of data into one DataFrame
#aircraft_performance = pd.concat(dfs, ignore_index=True)
aircraft_performance = aircraft_performance.dropna(subset=['Quarter','Month','FlightDate','Reporting_Airline','OriginAirportID','DestAirportID'])
aircraft_performance["Tail_Number"].fillna("NaN", inplace = True)
aircraft_performance["ArrDelay"].fillna("0", inplace = True)
aircraft_performance["DepDelay"].fillna("0", inplace = True)
aircraft_performance["ArrDelayMinutes"].fillna("0", inplace = True)
aircraft_performance["DepDelayMinutes"].fillna("0", inplace = True) 
aircraft_performance["DepTime"].fillna("0", inplace = True) 
aircraft_performance["ArrTime"].fillna("0", inplace = True)     

#concatenate the airline name
carrier_df =  pd.read_csv('./input/airlines/carriers.csv')
aircraft_performance_carrier = pd.merge(aircraft_performance, carrier_df, left_on='Reporting_Airline', right_on='Code', how='left')

aircraft_df =  pd.read_csv('./input/airlines/plane-data.csv')
aircraft_performance_carrier_aircraft = pd.merge(aircraft_performance_carrier, aircraft_df, left_on='Tail_Number', right_on='tailnum', how='left')
aircraft_performance_carrier_aircraft = aircraft_performance_carrier_aircraft.dropna(subset=['issue_date'])
aircraft_performance_carrier_aircraft = aircraft_performance_carrier_aircraft[aircraft_performance_carrier_aircraft.issue_date != 'None']
aircraft_performance_carrier_aircraft['FlightDate'] = pd.to_datetime(aircraft_performance_carrier_aircraft["FlightDate"]).dt.strftime('%Y-%m-%d')
aircraft_performance_carrier_aircraft['issue_date'] = pd.to_datetime(aircraft_performance_carrier_aircraft["issue_date"]).dt.strftime('%Y-%m-%d')
aircraft_performance_carrier_aircraft['DepDel15'] = aircraft_performance_carrier_aircraft['DepDel15'].fillna(0)
aircraft_performance_carrier_aircraft['ArrDel15'] = aircraft_performance_carrier_aircraft['ArrDel15'].fillna(0)
aircraft_performance_carrier_aircraft['CarrierDelay'] = aircraft_performance_carrier_aircraft['CarrierDelay'].fillna(0)
aircraft_performance_carrier_aircraft['WeatherDelay'] = aircraft_performance_carrier_aircraft['WeatherDelay'].fillna(0)
aircraft_performance_carrier_aircraft['NASDelay'] = aircraft_performance_carrier_aircraft['NASDelay'].fillna(0)
aircraft_performance_carrier_aircraft['SecurityDelay'] = aircraft_performance_carrier_aircraft['SecurityDelay'].fillna(0)
aircraft_performance_carrier_aircraft['LateAircraftDelay'] = aircraft_performance_carrier_aircraft['LateAircraftDelay'].fillna(0)

#for ind, row in tqdm(aircraft_performance.iterrows(), total=aircraft_performance.shape[0]):
#    if pd.isna(row['Tail_Number']) == True:
#        print(row['Tail_Number'])

#Load company stock value
aa_df =  pd.read_csv('./input/airlines/AAL.csv')
aa_df['Code'] = 'AA'
dal_df =  pd.read_csv('./input/airlines/DAL.csv')
dal_df['Code'] = 'DL'
ua_df =  pd.read_csv('./input/airlines/UAL.csv')
ua_df['Code'] = 'UA'
luv_df =  pd.read_csv('./input/airlines/LUV.csv')
luv_df['Code'] = 'LUV'
airline_stock = pd.concat([aa_df, dal_df, ua_df, luv_df]) 

#Drop stock columns not required for our analysis
airline_stock = airline_stock.drop(columns=['Open', 'High', 'Low', 'Adj Close', 'Volume'])

#Merge with the main dataframe
#We are merging the stock value on a given day for a airline code
aircraft_performance_carrier_aircraft_stock = pd.merge(aircraft_performance_carrier_aircraft, airline_stock, left_on=['Reporting_Airline','FlightDate'], right_on=['Code','Date'], how='left')

#Fill all other stock value with 0
aircraft_performance_carrier_aircraft_stock['Close'] = aircraft_performance_carrier_aircraft_stock['Close'].fillna(0)

## Code to load the data into cassandra
## We are making use of cassandra object mapper to map dataframe rows to the object representation
## Object Mapper
class airlineontime(Model):
  __keyspace__ = 'thirdeye_test'
  __table_name__ = 'airlineontime'
  id = columns.UUID(primary_key=True)
  year = columns.Integer()
  quarter = columns.Integer()
  month = columns.Integer()
  dayofmonth = columns.Integer()
  dayofweek = columns.Integer()
  flight_date = columns.Date()
  crsdeptime = columns.Integer()
  actualdeptime = columns.Integer()
  depdelay = columns.Integer()
  depdelayminutes = columns.Integer()
  crsarrtime = columns.Integer()
  actualarrtime = columns.Integer()
  arrdelay = columns.Integer()
  arrdelayminutes = columns.Integer()
  reporting_airline = columns.Text()
  tail_number = columns.Text()
  originairportid = columns.Integer()
  destairportid = columns.Integer()
  carriername =  columns.Text()
  manufacturer =  columns.Text()
  aircraft_issue_date = columns.Date()
  aircraft_model = columns.Text()
  aircraft_type = columns.Text()
  aircraft_engine = columns.Text()
  origincityname = columns.Text()
  originstatename = columns.Text()
  originstatecode = columns.Text()
  destinationcityname = columns.Text()
  destinationstatename = columns.Text()
  destinationstatecode = columns.Text()
  depdel15 = columns.Integer()
  arrdel15 = columns.Integer()
  cancelled = columns.Integer()
  carrierdelay = columns.Integer()
  weatherdelay = columns.Integer()
  nasdelay = columns.Integer()
  securitydelay = columns.Integer()
  lateaircraftdelay = columns.Integer()
  close = columns.Decimal()
  ownership = columns.Text()
  
# Apache Cassandra connection
list_of_ip = (['192.168.56.101', '192.168.56.102', '192.168.56.103'])
cluster = Cluster(list_of_ip,load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
session = cluster.connect()
session.set_keyspace('thirdeye_test')
connection.set_session(session)
#connection.set_session(db.db().getConnection())

# Apache Kafka connection
#p = Producer({'bootstrap.servers': '192.168.56.101:9092'})
#aircraft_performance_carrier_aircraft_stock.to_json(orient='index')

#df.apply(lambda x: print(x.to_json()), axis=1)

#for jdict in aircraft_performance_carrier_aircraft_stock.to_dict(orient='records'):
#    print(jdict)
#    p.produce('thirdeye_raw', jdict)
#    p.flush(30)

## saving data to database
for ind, row in tqdm(aircraft_performance_carrier_aircraft_stock.iterrows(), total=aircraft_performance_carrier_aircraft_stock.shape[0]):
    airlineontime.create(
    year = row['Year'],
    quarter = row['Quarter'],
    month = row['Month'],
    dayofmonth = row['DayofMonth'],
    dayofweek = row['DayOfWeek'],
    flight_date = row['FlightDate'],
    reporting_airline = row['Reporting_Airline'],
    origincityname = row['OriginCityName'],
    originstatename = row['OriginStateName'],
    originstatecode = row['OriginState'],
    destinationcityname = row['DestCityName'],
    destinationstatename = row['DestStateName'],
    destinationstatecode = row['DestState'],
    tail_number = row['Tail_Number'],
    originairportid = row['OriginAirportID'],
    destairportid = row['DestAirportID'],
    crsdeptime = row['CRSDepTime'],
    actualdeptime = row['DepTime'],
    depdelayminutes = row['DepDelayMinutes'],
    depdelay = row['DepDelay'],
    crsarrtime = row['CRSArrTime'],
    actualarrtime = row['ArrTime'],
    arrdelayminutes = row['ArrDelayMinutes'],
    arrdelay = row['ArrDelay'],
    carriername = row['Description'],
    aircraft_issue_date = row['issue_date'],
    aircraft_model = row['model'],
    aircraft_type = row['aircraft_type'],
    aircraft_engine = row['engine_type'],
    depdel15 = row['DepDel15'],
    arrdel15  = row['ArrDel15'],
    cancelled = row['Cancelled'],
    carrierdelay  = row['CarrierDelay'],
    weatherdelay  = row['WeatherDelay'],
    nasdelay  = row['NASDelay'],
    securitydelay  = row['SecurityDelay'],
    lateaircraftdelay  = row['LateAircraftDelay'],
    close  = row['Close'],
    ownership = row['ownership'],
    )
