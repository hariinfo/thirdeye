# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 07:46:49 2020

@author: harii
"""


from elasticsearch import Elasticsearch
from elasticsearch import helpers
from confluent_kafka import Consumer
import json

running = True
#Consume Kafka message
conf = {
                'bootstrap.servers':'192.168.56.101:9092',
                'group.id': "esgroup",
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
MIN_COMMIT_COUNT = 1000;

def process(es, record):
   print("Indexing records")
   res = es.index(index="my-index", body=record.value().decode('utf-8'))
   print(res['result'])

    
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
    es = Elasticsearch()
    basic_consume_loop(consumer, ["airline_raw"], es)


def shutdown():
    running = False

if __name__ == "__main__":
    main()
