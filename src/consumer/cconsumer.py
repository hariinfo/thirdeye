# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 07:46:49 2020

@author: harii
"""


from cassandra.cluster import Cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cqlengine.columns import *
from confluent_kafka import Consumer
from tqdm import tqdm
import json

running = True
#Consume Kafka message
conf = {
                'bootstrap.servers':'192.168.56.101:9092',
                'group.id': "cgroup",
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
MIN_COMMIT_COUNT = 1000;

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

## Code to load the data into cassandra
## We are making use of cassandra object mapper to map dataframe rows to the object representation
## Object Mapper
class airlineontime_byairline(Model):
  __keyspace__ = 'thirdeye_test'
  __table_name__ = 'airlineontime_byairline'
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

def process(connection, record):
   print("Inserting records")
   row = json.loads(record.value().decode('utf-8'))

   resp = airlineontime.create(
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
   print(resp)

   resp = airlineontime_byairline.create(
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
   print(resp)

    
def basic_consume_loop(consumer, topics, es):
    try:
        msg_count = 0
        consumer.subscribe(topics)
        print("Subscribed to {}...".format(topics))
        while running:
            print("Polling...")
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            
                

            if msg.error():
                print("Error Process Message...")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print("Process Message...")
                process(es, msg)
                msg_count += 1
                print("Message count {}".format(msg_count))
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)
    finally:
        print("Closing...")
        # Close down consumer to commit final offsets.
        consumer.close()


def main():
    # Apache Kafka connection
    consumer = Consumer(conf)
    # Apache Cassandra connection
    list_of_ip = (['192.168.56.101', '192.168.56.102', '192.168.56.103'])
    cluster = Cluster(list_of_ip,load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
    session = cluster.connect()
    session.set_keyspace('thirdeye_test')
    connection.set_session(session)
    basic_consume_loop(consumer, ["thirdeye_raw"], connection)


def shutdown():
    running = False

if __name__ == "__main__":
    main()
