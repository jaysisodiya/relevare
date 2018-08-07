#
# Events Consumer
# Topic: gdelt_events
#

from kafka import KafkaConsumer
from datetime import datetime
import pandas as pd
import pickle

#import logging
#logging.basicConfig(level=logging.DEBUG)

#
# Key/Value DeSerializer
#
def mydser(msg):
    return pickle.loads(msg)

#
# Initialize the Consumer
#
brokerlist='ec2-54-186-208-110.us-west-2.compute.amazonaws.com:9092,ec2-52-11-172-126.us-west-2.compute.amazonaws.com:9092,ec2-52-88-204-111.us-west-2.compute.amazonaws.com:9092,ec2-52-35-101-204.us-west-2.compute.amazonaws.com:9092'
consumer = KafkaConsumer('gdelt_events', bootstrap_servers=brokerlist, key_deserializer=mydser, value_deserializer=mydser, auto_offset_reset='earliest')

#
# Get the GDELT field names from a helper file
#
colnames = pd.read_excel('CSV.header.fieldids.xlsx', sheet_name='Sheet1', index_col='Column ID', usecols=1)['Field Name']

#
# Read the events from topic
#

beg_time = datetime.now()

cnt = 0
for msg in consumer:
    topic=msg.topic
    part=msg.partition
    offset=msg.offset
    key=msg.key
    value=msg.value
    avgtone=int(float(value["AvgTone"]))
    sourceurl=value["SOURCEURL"]
    eventdt=value["DATEADDED"]
    country=value["Actor1Geo_CountryCode"]
    if country == 'MX' and (avgtone >= 10 or avgtone <=-10):
        print("%s event on %s Tone >> %d Source: %s" % (country,eventdt,avgtone,sourceurl))
    cnt += 1
    if cnt >= 100000:
        break

end_time = datetime.now()
run_time = end_time - beg_time

print('Took %d secs %d microsecs.' % (run_time.seconds, run_time.microseconds))

