#
# GDELT data will be published to Kafka
# Topics will be created - one topic per country using the Alpha-2 country codes
#

from kafka import KafkaProducer
import pandas as pd
import pickle
import s3fs
#import logging

#logging.basicConfig(level=logging.DEBUG)

#
# Initialize the producer
#
brokerlist=''
producer = KafkaProducer(bootstrap_servers=brokerlist)

#
# Read the file in, iterate over events and publish
# 1. Get the GDELT field names from a helper file
#
colnames = pd.read_excel('CSV.header.fieldids.xlsx', sheet_name='Sheet1', index_col='Column ID', usecols=1)['Field Name']

#
# 2. Read the events in dataframe
#
fs = s3fs.S3FileSystem(anon=False)
df_events = pd.read_csv('s3://gdelt-open-data/events/20180730.export.csv', sep='\t', low_memory=False, header=None, dtype=str, names=colnames, index_col=['GLOBALEVENTID'])

cnt = 0

for index, row in df_events.iterrows():
    topic=str(row["Actor1Geo_CountryCode"])
    if topic == 'US':
        sendmsg=pickle.dumps(row)
        producer.send(topic, sendmsg)
        cnt += 1

'''
mymetrics = producer.metrics()
print(type(mymetrics))
for k,v in mymetrics.items():
    print(k)
    for k1,v1 in v.items():
        print('   ',k1,'  >>>>  ',v1)
'''

producer.flush()

print("Send %d messages" % cnt)

'''
class MyThreadProducer(threading.Thread): 
    def __init__(self, producer): 
        super(MyThreadProducer, self).__init__() 
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092']) 
    def run(self): 
        process_name = self.name 
        while True: 
            self.producer.send('my-topic', 'thread') time.sleep(5)
'''
