# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 18:11:22 2020

@author: harii
"""
#pip install cassandra-driver
#pip install elasticsearch
#pip install confluent-kafka


# Using tablesplus https://tableplus.com/ as my NoSQL Client to quickly run queries rather than using CQL which is only available on the cassandra node
#SimpleStrategy is used as we assume this to be single DC setup

#https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra/50508046
from lib import db 
from model import airlineontime 
from cassandra.cluster import Cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cqlengine.columns import *
from tqdm import tqdm
import json

from confluent_kafka import Producer
from elasticsearch import Elasticsearch


import glob
import pandas as pd
import datetime as dt
import sys

#Enrich by adding a column for publicly vs privately managed airlines
def airline_ownership (row):
      if row['Reporting_Airline'] == 'DL' :
           return 'Public'
      if row['Reporting_Airline'] == 'AA' :
           return 'Public'
      if row['Reporting_Airline'] == 'UA' :
           return 'Public'
      if row['Reporting_Airline'] == 'WN    ' :
          return 'Public'
      return 'Private'

# get data file names
path =r'../input/airlines/performance'
filenames = glob.glob(path + "/*.csv")

# ETL Tasks
#1. Convert CRSDepTime and CRSArrTime into hhmm format as certain datasets are single digit E.g. 5 will be translated to 0005
#2. Validate null columns
#3. Validate dattime format

dfs = []
for filename in filenames:
    dfs.append(pd.read_csv(filename, converters={'CRSDepTime': '{:0>4}'.format,'CRSArrTime': '{:0>4}'.format}))

aircraft_performance = pd.concat(dfs, ignore_index=True)
#Enrich by adding a column for publicly vs privately managed airlines
aircraft_performance['ownership'] = aircraft_performance.apply(lambda row: airline_ownership(row), axis=1)

print("Total of {} rows at source".format(aircraft_performance.shape[0]))

#ETL Task: Cleanse the data
# Concatenate all months of data into one DataFrame
aircraft_performance["Tail_Number"].fillna("NaN", inplace = True)

# Difference in minutes between scheduled and actual arrival time.
# Early arrivals show negative, blank columns were replaced with 0, which translates to no arrival delay
aircraft_performance["ArrDelay"].fillna("0", inplace = True)
aircraft_performance["DepDelay"].fillna("0", inplace = True)

#Difference in minutes between scheduled and actual departure time. Early departures set to 0.
aircraft_performance["ArrDelayMinutes"].fillna("0", inplace = True)
aircraft_performance["DepDelayMinutes"].fillna("0", inplace = True) 

#We don't use these columns for analysis, defaulting it to 0 so we don't loose out the records
aircraft_performance["DepTime"].fillna("0", inplace = True) 
aircraft_performance["ArrTime"].fillna("0", inplace = True)     
aircraft_performance["ArrTime"].fillna("0", inplace = True)     

#Drop columns as every field is null in the source data
aircraft_performance.drop(['CancellationCode'], axis=1, inplace = True)
aircraft_performance.drop(['FirstDepTime'], axis=1, inplace = True)
aircraft_performance.drop(['TotalAddGTime'], axis=1, inplace = True)
aircraft_performance.drop(['LongestAddGTime'], axis=1, inplace = True)
aircraft_performance.drop(['DivAirportLandings'], axis=1, inplace = True)
aircraft_performance.drop(['DivReachedDest'], axis=1, inplace = True)
aircraft_performance.drop(['DivActualElapsedTime'], axis=1, inplace = True)
aircraft_performance.drop(['DivArrDelay'], axis=1, inplace = True)
aircraft_performance.drop(['DivDistance'], axis=1, inplace = True)
aircraft_performance.drop(['Div1Airport'], axis=1, inplace = True)
aircraft_performance.drop(['Div1AirportID'], axis=1, inplace = True)
aircraft_performance.drop(['Div1AirportSeqID'], axis=1, inplace = True)
aircraft_performance.drop(['Div1WheelsOn'], axis=1, inplace = True)
aircraft_performance.drop(['Div1TotalGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div1LongestGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div1WheelsOff'], axis=1, inplace = True)
aircraft_performance.drop(['Div1TailNum'], axis=1, inplace = True)
aircraft_performance.drop(['Div2Airport'], axis=1, inplace = True)
aircraft_performance.drop(['Div2AirportID'], axis=1, inplace = True)
aircraft_performance.drop(['Div2AirportSeqID'], axis=1, inplace = True)
aircraft_performance.drop(['Div2WheelsOn'], axis=1, inplace = True)
aircraft_performance.drop(['Div2TotalGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div2LongestGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div2WheelsOff'], axis=1, inplace = True)
aircraft_performance.drop(['Div2TailNum'], axis=1, inplace = True)
aircraft_performance.drop(['Div3Airport'], axis=1, inplace = True)
aircraft_performance.drop(['Div3AirportID'], axis=1, inplace = True)
aircraft_performance.drop(['Div3AirportSeqID'], axis=1, inplace = True)
aircraft_performance.drop(['Div3WheelsOn'], axis=1, inplace = True)
aircraft_performance.drop(['Div3TotalGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div3WheelsOff'], axis=1, inplace = True)
aircraft_performance.drop(['Div3TailNum'], axis=1, inplace = True)
aircraft_performance.drop(['Div4Airport'], axis=1, inplace = True)
aircraft_performance.drop(['Div4AirportID'], axis=1, inplace = True)
aircraft_performance.drop(['Div4AirportSeqID'], axis=1, inplace = True)
aircraft_performance.drop(['Div4WheelsOn'], axis=1, inplace = True)
aircraft_performance.drop(['Div4TotalGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div4LongestGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div4WheelsOff'], axis=1, inplace = True)
aircraft_performance.drop(['Div4TailNum'], axis=1, inplace = True)
aircraft_performance.drop(['Div5Airport'], axis=1, inplace = True)
aircraft_performance.drop(['Div5AirportID'], axis=1, inplace = True)
aircraft_performance.drop(['Div5AirportSeqID'], axis=1, inplace = True)
aircraft_performance.drop(['Div5WheelsOn'], axis=1, inplace = True)
aircraft_performance.drop(['Div5TotalGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div5LongestGTime'], axis=1, inplace = True)
aircraft_performance.drop(['Div5WheelsOff'], axis=1, inplace = True)
aircraft_performance.drop(['Div5TailNum'], axis=1, inplace = True)
aircraft_performance.drop(['Unnamed: 109'], axis=1, inplace = True)
aircraft_performance.drop(['Div3LongestGTime'], axis=1, inplace = True)
aircraft_performance.drop(['ArrivalDelayGroups'], axis=1, inplace = True)
aircraft_performance.drop(['ActualElapsedTime'], axis=1, inplace = True)
aircraft_performance.drop(['AirTime'], axis=1, inplace = True)
aircraft_performance.drop(['DepartureDelayGroups'], axis=1, inplace = True)
aircraft_performance.drop(['TaxiOut'], axis=1, inplace = True)
aircraft_performance.drop(['WheelsOff'], axis=1, inplace = True)
aircraft_performance.drop(['WheelsOn'], axis=1, inplace = True)
aircraft_performance.drop(['TaxiIn'], axis=1, inplace = True)


#concatenate the airline name
carrier_df =  pd.read_csv('../input/airlines/carriers.csv')
aircraft_performance_carrier = pd.merge(aircraft_performance, carrier_df, left_on='Reporting_Airline', right_on='Code', how='left')

aircraft_df =  pd.read_csv('../input/airlines/plane-data.csv')
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
aa_df =  pd.read_csv('../input/airlines/AAL.csv')
aa_df['Code'] = 'AA'
dal_df =  pd.read_csv('../input/airlines/DAL.csv')
dal_df['Code'] = 'DL'
ua_df =  pd.read_csv('../input/airlines/UAL.csv')
ua_df['Code'] = 'UA'
luv_df =  pd.read_csv('../input/airlines/LUV.csv')
luv_df['Code'] = 'LUV'
airline_stock = pd.concat([aa_df, dal_df, ua_df, luv_df]) 

#Drop stock columns not required for our analysis
airline_stock = airline_stock.drop(columns=['Open', 'High', 'Low', 'Adj Close', 'Volume'])

#Merge with the main dataframe
#We are merging the stock value on a given day for a airline code
aircraft_performance_carrier_aircraft_stock = pd.merge(aircraft_performance_carrier_aircraft, airline_stock, left_on=['Reporting_Airline','FlightDate'], right_on=['Code','Date'], how='left')

#Fill all other stock value with 0
aircraft_performance_carrier_aircraft_stock['Close'] = aircraft_performance_carrier_aircraft_stock['Close'].fillna(0)
aircraft_performance_carrier_aircraft_stock.drop(['Date'], axis=1, inplace = True)
aircraft_performance_carrier_aircraft_stock.drop(['Code_y'], axis=1, inplace = True)

records = aircraft_performance_carrier_aircraft_stock.to_dict(orient='records')

conf = {
                'bootstrap.servers':'192.168.56.101:9092',
                'queue.buffering.max.messages': 500000,
                'queue.buffering.max.ms': 60000,
                'batch.num.messages': 100,
                'log.connection.close': False,
                'default.topic.config': {'acks': 'all'}
            }

# Apache Kafka connection
p = Producer(conf)
print("Streaming to kafka........")
for j in tqdm(range(len(records))):
    key = records[j]["FlightDate"] + "-" + records[j]["Reporting_Airline"] + "-" + records[j]["CRSDepTime"] + "-" + records[j]["Tail_Number"]
    #print(key)
    p.produce("airline_raw", key=key, value=json.dumps(records[j]))
    #p.poll(0)
