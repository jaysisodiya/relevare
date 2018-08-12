#
# GDELT data will be published to Kafka
# Topics will be created - one topic per country using the Alpha-2 country codes
#

from kafka import KafkaProducer
from datetime import datetime
import pickle

#
# Initialize the producer
#
brokerlist=''
producer = KafkaProducer(bootstrap_servers=brokerlist)

cnt = 0
payload = [ '0123456789', '01234567890123456789012345678901234567890123456789', '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' ]
msgcnt = [ 1000, 10000, 1000000 ]

def testit(pload,n):
    beg_time = datetime.now()
    for i in range(n):
        sendmsg = pickle.dumps(pload)
        producer.send('foobar',sendmsg)

    return datetime.now() - beg_time

for testload in payload:
    for n in msgcnt:
        timetaken = testit(testload,n)
        lenpayload = len(testload)
        print('Payload:\t%d\tMsgCnt:\t%d\tTime:\t%d secs %d microsecs' % (lenpayload, n, timetaken.seconds, timetaken.microseconds))

producer.flush()
