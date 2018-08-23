#
# GDELT "GKG" data will be published to Kafka
#
# Input files pulled from for e.g.
# http://data.gdeltproject.org/gdeltv2/20180809213000.gkg.csv.zip
#

from confluent_kafka import Producer
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile
import urllib.request

beg_time = datetime.now()

#
# Initialize the producer
#
brokerlist='ec2-54-186-208-110.us-west-2.compute.amazonaws.com:9092,ec2-52-11-172-126.us-west-2.compute.amazonaws.com:9092,ec2-52-88-204-111.us-west-2.compute.amazonaws.com:9092,ec2-52-35-101-204.us-west-2.compute.amazonaws.com:9092'
p_conf = {
   'bootstrap.servers': brokerlist,
   'client.id': 'util1_gkg'
}
p = Producer(p_conf)

#
# Send gkg - line at a time from the files
#
def sendgkg(fname):
    print(fname)
    cnt = 0

    url = urllib.request.urlopen(fname)
    with ZipFile(BytesIO(url.read())) as my_zip_file:
        for contained_file in my_zip_file.namelist():
            for line in my_zip_file.open(contained_file).readlines():
                cnt += 1
                p.poll(0)
#            p.produce('gdelt_gkg', line)
                p.produce('perftest', line)
    return cnt

years=['2017']
months=['01']
days=['01']
hours=['01','02']
mins=['00','15','30','45']

def gkg_zipnames(years, months, days, hours, mins):
    urlgdelt='http://data.gdeltproject.org/gdeltv2/'
    gkgfileext='.gkg.csv.zip'
    for yr in years:
        for mo in months:
            for dy in days:
                for hr in hours:
                    for mi in mins:
                        zname=urlgdelt+yr+mo+dy+hr+mi+'00'+gkgfileext
                        yield zname

fname = gkg_zipnames(years, months, days, hours, mins)

for i in fname:
    print("Processing File : ", i)
    sendcnt=sendgkg(i)
    print("    >> For day %s, sent %d gkg to Kafka." % (i, sendcnt))

end_time = datetime.now()
run_time = end_time - beg_time

print("Total Time: %d secs %d microsecs." % (run_time.seconds, run_time.microseconds))

p.flush()
