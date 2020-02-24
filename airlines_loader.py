# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 21:47:42 2020

@author: harii
"""
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
aircraft_performance['CRSDepTime'] = pd.to_datetime(aircraft_performance['CRSDepTime'], format='%H%M', exact=False)
aircraft_performance['CRSArrTime'] = pd.to_datetime(aircraft_performance['CRSArrTime'], format='%H%M', exact=False)

aircraft_performance['FlightArrDateTime'] = pd.to_datetime(aircraft_performance.FlightDate.astype(str)+' '+aircraft_performance.CRSArrTime.astype(str))
aircraft_performance['FlightDepDateTime'] = pd.to_datetime(aircraft_performance.FlightDate.astype(str)+' '+aircraft_performance.CRSDepTime.astype(str))










#left join the weather data
weather = pd.read_csv('./input/airlines/weather_description.csv', header=0, low_memory=False)

