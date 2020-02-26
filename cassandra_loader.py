# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 18:11:22 2020

@author: harii
"""
#pip install cassandra-driver

# Using tablesplus https://tableplus.com/ as my NoSQL Client to quickly run queries rather than using CQL which is only available on the cassandra node
#SimpleStrategy is used as we assume this to be single DC setup

#https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra/50508046
from aircraft.lib import db 
from aircraft.model import airlineontime 

from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cqlengine.columns import *
from tqdm import tqdm




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
#aircraft_performance['CRSDepTime'] = pd.to_datetime(aircraft_performance['CRSDepTime'], format='%H%M', exact=False)
#aircraft_performance['CRSArrTime'] = pd.to_datetime(aircraft_performance['CRSArrTime'], format='%H%M', exact=False)

#aircraft_performance['FlightArrDateTime'] = pd.to_datetime(aircraft_performance.FlightDate.astype(str)+' '+aircraft_performance.CRSArrTime.astype(str))
#aircraft_performance['FlightDepDateTime'] = pd.to_datetime(aircraft_performance.FlightDate.astype(str)+' '+aircraft_performance.CRSDepTime.astype(str))


# Concatenate all months of data into one DataFrame
#aircraft_performance = pd.concat(dfs, ignore_index=True)
aircraft_performance = aircraft_performance.dropna(subset=['FlightDate','Tail_Number','Reporting_Airline','OriginAirportID','DestAirportID'])

#concatenate the airline name
carrier_df =  pd.read_csv('./input/airlines/carriers.csv')
aircraft_performance_carrier = pd.merge(aircraft_performance, carrier_df, left_on='Reporting_Airline', right_on='Code', how='left')

aircraft_df =  pd.read_csv('./input/airlines/plane-data.csv')
aircraft_performance_carrier_aircraft = pd.merge(aircraft_performance_carrier, aircraft_df, left_on='Tail_Number', right_on='tailnum', how='left')
aircraft_performance_carrier_aircraft = aircraft_performance_carrier_aircraft.dropna(subset=['issue_date'])
aircraft_performance_carrier_aircraft = aircraft_performance_carrier_aircraft[aircraft_performance_carrier_aircraft.issue_date != 'None']

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

#AA Stock value
aa_df =  pd.read_csv('./input/airlines/AAL.csv')
aa_df['Code'] = 'AA'
aa_df = aa_df.drop(columns=['Open', 'High', 'Low', 'Adj Close', 'Volume'])
aircraft_performance_carrier_aircraft_stock = pd.merge(aircraft_performance_carrier_aircraft, aa_df, left_on=['Reporting_Airline','FlightDate'], right_on=['Code','Date'], how='left')

#DL Stock value
dal_df =  pd.read_csv('./input/airlines/DAL.csv')
dal_df['Code'] = 'DL'
dal_df = dal_df.drop(columns=['Open', 'High', 'Low', 'Adj Close', 'Volume'])
aircraft_performance_carrier_aircraft_stock = pd.merge(aircraft_performance_carrier_aircraft, dal_df, left_on=['Reporting_Airline','FlightDate'], right_on=['Code','Date'], how='left')

#United Stock value
ua_df =  pd.read_csv('./input/airlines/UAL.csv')
ua_df['Code'] = 'UA'
ua_df = ua_df.drop(columns=['Open', 'High', 'Low', 'Adj Close', 'Volume'])
aircraft_performance_carrier_aircraft_stock = pd.merge(aircraft_performance_carrier_aircraft, ua_df, left_on=['Reporting_Airline','FlightDate'], right_on=['Code','Date'], how='left')

#southwest Stock value
luv_df =  pd.read_csv('./input/airlines/LUV.csv')
luv_df['Code'] = 'LUV'
luv_df = luv_df.drop(columns=['Open', 'High', 'Low', 'Adj Close', 'Volume'])
aircraft_performance_carrier_aircraft_stock = pd.merge(aircraft_performance_carrier_aircraft, luv_df, left_on=['Reporting_Airline','FlightDate'], right_on=['Code','Date'], how='left')

#Fill all other stock value with 0
aircraft_performance_carrier_aircraft_stock['Close'] = aircraft_performance_carrier_aircraft_stock['Close'].fillna(0)


  
# Apache Cassandra connection
conn = db.db()
connection = conn.getConnection()

## saving data to database
for ind, row in tqdm(aircraft_performance_carrier_aircraft_stock.iterrows(), total=aircraft_performance.shape[0]):
    airlineontime.create(
    flight_date = row['FlightDate'],      
    reporting_airline = row['Reporting_Airline'],
    tail_number = row['Tail_Number'],
    originairportid = row['OriginAirportID'],
    destairportid = row['DestAirportID'],
    crsdeptime = row['CRSDepTime'],
    crsarrtime = row['CRSArrTime'],
    carriername = row['Description'],
    aircraft_issue_date = row['issue_date'],
    aircraft_model = row['model'],
    aircraft_type = row['aircraft_type'],
    aircraft_engine = row['engine_type'],
    origincityname = row['OriginCityName'],
    depdel15 = row['DepDel15'],
    arrdel15  = row['ArrDel15'],
    carrierdelay  = row['CarrierDelay'],
    weatherdelay  = row['WeatherDelay'],
    nasdelay  = row['NASDelay'],
    securitydelay  = row['SecurityDelay'],
    lateaircraftdelay  = row['LateAircraftDelay'],
    close  = row['Close'],
    )
