#
# GDELT data will be published to Kafka
# Topics will be created - one topic per country using the Alpha-2 country codes
#

from kafka import KafkaProducer
from datetime import datetime
import pandas as pd
import pickle
import s3fs

fs = s3fs.S3FileSystem(anon=False)

#import logging
#logging.basicConfig(level=logging.DEBUG)

#
# Key/Value Serializer
#
def myser(msg):
    return pickle.dumps(msg)

#
# Initialize the producer
#
brokerlist='ec2-54-186-208-110.us-west-2.compute.amazonaws.com:9092,ec2-52-11-172-126.us-west-2.compute.amazonaws.com:9092,ec2-52-88-204-111.us-west-2.compute.amazonaws.com:9092,ec2-52-35-101-204.us-west-2.compute.amazonaws.com:9092'
producer = KafkaProducer(bootstrap_servers=brokerlist, key_serializer=myser, value_serializer=myser)

#
# Read the file in, iterate over events and publish
# 1. Get the GDELT field names from a helper file
#
colnames = pd.read_excel('CSV.header.fieldids.xlsx', sheet_name='Sheet1', index_col='Column ID', usecols=1)['Field Name']
s3gdelt='s3://gdelt-open-data/events/'
fileext='.export.csv'

#
# 2. Read the events in dataframe
#
def getevents(eventday):
    s3eventfile=s3gdelt + eventday + fileext
    print(s3eventfile)
    df_events = pd.read_csv(s3eventfile, sep='\t', low_memory=False, header=None, dtype=str, 
                            names=colnames, index_col=['GLOBALEVENTID'])
    return df_events

def sendevents(df_events):
    cnt = 0
    for index, row in df_events.iterrows():
        topic=str(row["Actor1Geo_CountryCode"])
        producer.send('gdelt_events',key=topic, value=row)
        cnt += 1
    return cnt

beg_time = datetime.now()

event_years=['2018']
event_months=['08']
event_dates=['01','02','03']

for y in event_years:
    for m in event_months:
        for d in event_dates:
             eventday=y+m+d
             df_events=getevents(eventday)
             sendcnt=sendevents(df_events)
             print("For day %s, sent %d events to Kafka." % (eventday, sendcnt))

end_time = datetime.now()
run_time = end_time - beg_time

print('Took %d secs %d microsecs.' % (run_time.seconds, run_time.microseconds))

producer.flush()
