#
# GDELT "mentions" data will be published to Kafka
#
# Input files pulled from for e.g.
# http://data.gdeltproject.org/gdeltv2/20180809213000.mentions.CSV.zip
#

from confluent_kafka import Producer
from datetime import datetime
from datetime import timedelta
import time
from io import BytesIO
from zipfile import ZipFile
import urllib.request
import json
import re
import os
from collections import OrderedDict

beg_time = datetime.now()

#
# Get necessary environemnt variables.
#
brokerlist=os.environ['KAFKA_BROKERS']
urlgdelt=os.environ['URL_GDELT']
mentionsfileext=os.environ['MENTIONS_FILEEXT']

colnames=["GlobalEventID" ,"EventTimeDate" ,"MentionTimeDate" ,"MentionType" ,"MentionSourceName" ,"MentionIdentifier" ,"SentenceID" ,"Actor1CharOffset" ,"Actor2CharOffset" ,"ActionCharOffset" ,"InRawText" ,"Confidence" ,"MentionDocLen" ,"MentionDocTone" ,"MentionDocTranslationInfo" ,"Extras"]

#
# Initialize the producer
#
p_conf = {
   'bootstrap.servers': brokerlist,
   'client.id': 'util1_mentions'
}
p = Producer(p_conf)

#
# Send mentions - line at a time from the files
#
def sendmentions(fname):
    cnt = 0

    url = urllib.request.urlopen(fname)
    with ZipFile(BytesIO(url.read())) as my_zip_file:
        for contained_file in my_zip_file.namelist():
            for line in my_zip_file.open(contained_file).readlines():
                line1=line.decode('utf-8')
                values=line1.split('\t')
                msg = OrderedDict(zip(colnames,values))
                p.poll(0)
                p.produce('gdelt_mentions', json.dumps(msg).encode('utf-8'))
                cnt += 1
    return cnt

#
# Returns string in format YYYYMMDD
#
def datetime_to_str_date(dt):
    return re.sub('-', '', re.sub(r'\T.+$','', dt.isoformat()))

#
# GDELT files are currently available on 15 min resolution but maintain YYYYMMDDHHMMSS format for names
# Get the filename URLs for given dates
#
def mentions_zipnames(dates):
    for d in dates:
        for h in range(24):
            for ms in ['0000','1500','3000','4500']:
                dtstr=d+str(h).rjust(2,'0')+ms
                zname=urlgdelt+dtstr+mentionsfileext
                yield zname

start_date = datetime.strptime('2018-08-08', '%Y-%m-%d')
end_date = datetime.today()
#num_of_days = (end_date - start_date).days
num_of_days = 1

#
# For give start and end date, generate list of dates
#
date_list = map(
        datetime_to_str_date,
        [start_date + timedelta(days=x) for x in range(0, num_of_days)]
)

fname = mentions_zipnames(date_list)

sendcnt=0
for i in fname:
    print("Processing File : ", i)
    sendcnt=sendmentions(i)
    print("    >> For day %s, sent %d mentions to Kafka." % (i, sendcnt))
    time.sleep(15)

end_time = datetime.now()
run_time = end_time - beg_time

print("Total Time: %d secs %d microsecs." % (run_time.seconds, run_time.microseconds))

p.flush()
