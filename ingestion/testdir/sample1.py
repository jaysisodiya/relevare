#
# GDELT data will be published to Kafka
# Topics will be created - one topic per country using the Alpha-2 country codes
#

from kafka import KafkaProducer
import pandas as pd
import pickle
import logging

#logging.basicConfig(level=logging.DEBUG)

#
# Initialize the producer
#
brokerlist=""
producer = KafkaProducer(bootstrap_servers=brokerlist, max_block_ms=10000)

#
# Read the file in, iterate over events and publish
# 1. Get the GDELT field names from a helper file
#
colnames = pd.read_excel('CSV.header.fieldids.xlsx', sheet_name='Sheet1', index_col='Column ID', usecols=1)['Field Name']

#
# 2. Read the events in dataframe
#
df_events = pd.read_csv('20180730.sample.csv', sep='\t', low_memory=False, header=None, dtype=str, names=colnames, index_col=['GLOBALEVENTID'])

for index, row in df_events.iterrows():
    topic=str(row["Actor1Geo_CountryCode"])
    sendmsg=pickle.dumps(row)
    producer.send(topic, sendmsg)
#    producer.send(topic, '%s' % sendmsg)
#    producer.send(topic, b'events %d' % index)

#
# Just to check everthing is still talking to each other
#
#for i in range(100):
#    producer.send('nan',b'dummy %d' % i)

producer.flush()
