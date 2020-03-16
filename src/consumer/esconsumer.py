# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 07:46:49 2020

@author: harii
"""


from elasticsearch import Elasticsearch
from elasticsearch import helpers
from confluent_kafka import Consumer


running = True
#Consume Kafka message
conf = {
                'bootstrap.servers':'192.168.56.101:9092',
                'queue.buffering.max.messages': 500000,
                'queue.buffering.max.ms': 60000,
                'batch.num.messages': 100,
                'log.connection.close': False,
                'group.id': "es-group",
                'default.topic.config': {'acks': 'all'}
            }
def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        print("Subscribed to {}...".format(topics))
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

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
                print(msg)
    finally:
        print("Closing...")
        # Close down consumer to commit final offsets.
        consumer.close()


def main():
    # Apache Kafka connection
    consumer = Consumer(conf)
    basic_consume_loop(consumer, ["thirdeye_raw"])


def shutdown():
    running = False

if __name__ == "__main__":
    main()
