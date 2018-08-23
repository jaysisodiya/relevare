#
# Relevare - main pyspark program that processes the events and mentions from GDELT received
# from the Kafka topics and filtered critical events to publish back on Kafka
#
# Pening:
#  -- is the join with user preferences still feasible inside spark or needs to be handled outside
#  -- exception handling in all key places
# 
from operator import add
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
import os

myuser = os.environ["RELEVARE_USER"]		# MySQL schema user
mypass = os.environ["RELEVARE_PASS"]		# MySQL schema password
dburl = "jdbc:mysql://10.0.0.25/relevare"	# MySQL jdbc address for relevare
dbdrv = "com.mysql.jdbc.Driver"			# MySQL driver

brokerlist = os.environ["KAFKA_BROKERS"]
event_topic = "gdelt_events"			# consuming from this topic
mentions_topic = "gdelt_mentions"		# consuming from this topic
p_topic_notify = "gdelt_notify"			# publishing to this topic
p_topic_discover = "gdelt_discover"		# publishing to this topic
appname = "gdelt_test"				# app name
batchsecs = 20					# time interval for batches

# Deserializer
def mydser(msg):
    return json.loads(msg.decode('utf-8'))

# Get user data from MYSQL
def getusers(myspark):
    t_users = myspark.read.format("jdbc").\
              options(url=dburl, driver=dbdrv, dbtable="users", user=myuser, password=mypass, \
                      partitionColumn="id", numPartitions=100, lowerBound=0, upperBound=120000000).load()
    t_users.registerTempTable("users")
    sqlstmt=''' SELECT id, login, fname, lname, zipcode, statecode, state, countrycode, country 
                FROM users limit 100000 '''
    output = myspark.sql(sqlstmt)
    return output

# Get user preferences data from MYSQL
def getprefs(myspark):
    # user preferences - dynamic can change
    t_prefs = myspark.read.format("jdbc").\
              options(url=dburl, driver=dbdrv, dbtable="prefs", user=myuser, password=mypass, \
                      partitionColumn="id", numPartitions=100, lowerBound=0, upperBound=120000000).load()
    t_prefs.registerTempTable("prefs")
    # Fetch user preferences and return
    sqlstmt=''' SELECT id, usersid, pref1, pref2, pref3, pref4, pref5 
                FROM prefs limit 100000 '''
    output = myspark.sql(sqlstmt)
    return output

# Get user preferences data from MYSQL
def getuserprefs(myspark):
    # user preferences - dynamic can change
    t_prefs = myspark.read.format("jdbc").\
              options(url=dburl, driver=dbdrv, dbtable="userprefs", user=myuser, password=mypass, \
                      partitionColumn="usersid", numPartitions=24, lowerBound=0, upperBound=120000000).load()
    t_prefs.registerTempTable("userprefs")
    # Fetch user preferences and return
    sqlstmt=''' SELECT usersid, fname, lname, countrycode, pref1, pref2, pref3, pref4, pref5 
                FROM userprefs '''
    output = myspark.sql(sqlstmt)
    return output

# Get religion codes from MYSQL
def getreligions(myspark):
    t_religioncodes = myspark.read.format("jdbc").\
              options(url=dburl, driver=dbdrv, dbtable="religion_codes", user=myuser, password=mypass ).load()
    t_religioncodes.registerTempTable("religion_codes")
    sqlstmt=''' SELECT religion_code, religion_name 
                FROM religion_codes '''
    output = myspark.sql(sqlstmt)
    return output

# Get GCAM - global content analysis measures db dictionaries and dimensions reference data from MYSQL
def getdictdim(myspark):
    t_gcamdictdim = myspark.read.format("jdbc").\
              options(url=dburl, driver=dbdrv, dbtable="gcam_dictdim", user=myuser, password=mypass ).load()
    t_gcamdictdim.registerTempTable("gcam_dictdim")
    sqlstmt=''' SELECT dictdim_code, dict_id, dim_id, dict_type, lang_code, dict_humanname, dim_humanname,                                   dict_citation from gcam_dictdim '''
    output = myspark.sql(sqlstmt)
    return output

# Get event_code and event_scale data from MYSQL
def geteventscale(myspark):
    t_eventscale = myspark.read.format("jdbc").\
              options(url=dburl, driver=dbdrv, dbtable="event_scale", user=myuser, password=mypass ).load()
    t_eventscale.registerTempTable("eventscale")
    t_eventcodes = myspark.read.format("jdbc").\
              options(url=dburl, driver=dbdrv, dbtable="event_codes", user=myuser, password=mypass ).load()
    t_eventcodes.registerTempTable("eventcodes")
    sqlstmt=''' SELECT a.event_code eventcode, a.event_scale eventscale, b.event_name eventname \
                FROM eventscale a, eventcodes b \
                WHERE b.event_code = a.event_code '''
    output = myspark.sql(sqlstmt)
    return output

# Send the filtered events to Kafka
def sendevents(rows):
# Note how we have to create the producer instances at executor level.
# This function is being called foreachpartition.
    producer = KafkaProducer(bootstrap_servers=brokerlist)
    for row in rows:
        msg = row.asDict()
        producer.send(p_topic_notify, json.dumps(msg).encode('utf-8'))
        producer.flush()

# Send the key metrics to Kafka
def discoverhandler(message):
    records = message.collect()
    for record in records:
        producer.send(p_topic_discover, json.dumps(record).encode('utf-8'))
        producer.flush()

# Process the events and mentions join results
def processMentions(x):
# Empty batches will produce emptyRDD causing value error for df operations.
    numrows = 0
    if not x.isEmpty():
        mydf = sparkSess.createDataFrame(x)
        mydf.createOrReplaceTempView("mentionstab")
        try:
            sqlstmt = ''' select distinct b.eventid eventid, b.country country, b.url url \
                          from mentionstab a, eventsfiltered b \
                          where a.confidence >= 80 \
                          and a.mentiontype = 1 \
                          and a.globaleventid = b.eventid \
                          and a.mentiontimedate = a.eventtimedate \
                          and (a.mentiondoctone <= -9 or a.mentiondoctone >= 9) ''' 
            output = sparkSess.sql(sqlstmt)
            numrows = output.count()
            print(">>> Filtered events after mentions rules: ", numrows)
        except Exception:
            pass
    else:
        print("No mentions in this batch, check if there are still events to process")
        try:
            sqlstmt = ''' select distinct b.eventid eventid, b.country country, b.url url \
                          from eventsfiltered b '''
            output = sparkSess.sql(sqlstmt)
            numrows = output.count()
        except Exception:
            pass
    if numrows > 0:
        outrdd = output.rdd.foreachPartition(sendevents)
        print("Processed events : ", numrows)
    else:
        print("There are no events or mentions to process")
# When either no records in events or mentions, we need drop those to avoid false notifications
# Pending: See if this can be avoided
    sparkSess.catalog.dropTempView("eventsfiltered")
    sparkSess.catalog.dropTempView("eventstab")
    sparkSess.catalog.dropTempView("mentionstab")

def processEvents(x):
# Empty batches will produce emptyRDD causing value error for df operations.
    if not x.isEmpty():
        mydf = sparkSess.createDataFrame(x)
        mydf.createOrReplaceTempView("eventstab")
        sqlstmt = ''' select a.globaleventid eventid, a.actiongeo_countrycode country, a.sourceurl url \
                      from eventstab a, eventscaletab b \
                      where a.EventCode = b.eventcode \
                      and a.isrootevent = 1 \
                      and a.actiongeo_countrycode is not null \
                      and (b.eventscale <= -8 or b.eventscale >= 8) \
                      and (a.avgtone <= -9 or a.avgtone >= 9) '''
        output = sparkSess.sql(sqlstmt)
        output.createOrReplaceTempView("eventsfiltered")
        print("> Filtered events : ", output.count())
    else:
        print("No events in this batch")

# SparkSession to be used for creating DataFrame, register DataFrame as tables, execute SQL over tables, cache tables
sparkSess = SparkSession.builder.appName(appname).getOrCreate()
sc = sparkSess.sparkContext
ssc = StreamingContext(sc, batchsecs)

# Get users, create table amd cache it
#usersrecs = getusers(sparkSess)
#usersrecs.registerTempTable("usersdftab")
#sparkSess.sql(''' CACHE TABLE usersdftab ''')

# Get preferences, create tabe and cache it
#prefrecs = getprefs(sparkSess)
#prefrecs.registerTempTable("prefdftab")
#sparkSess.sql(''' CACHE TABLE prefdftab ''')

# Get event scale
eventscalerecs = geteventscale(sparkSess)
eventscalerecs.registerTempTable("eventscaletab")
sparkSess.sql(''' CACHE TABLE eventscaletab ''')

# Kafka Consumer Spark Streaming
eventStream = KafkaUtils.createDirectStream(ssc, [event_topic], {"metadata.broker.list": brokerlist, "auto.offset.reset": "smallest"}, valueDecoder=mydser)
mentionsStream = KafkaUtils.createDirectStream(ssc, [mentions_topic], {"metadata.broker.list": brokerlist, "auto.offset.reset": "smallest"}, valueDecoder=mydser)

# Kafka Producer - this will be needed only if producing from driver?
#producer = KafkaProducer(bootstrap_servers=brokerlist)

# Incoming messages, grab the values where we have events
allevents = eventStream.map(lambda v: (v[1]))
allevents.foreachRDD(processEvents)
allevents.count().map(lambda x: 'Events in this batch: %s' % x).pprint()

# Incoming messages, grab the values where we have mentions
allmentions = mentionsStream.map(lambda v: (v[1]))
allmentions.foreachRDD(processMentions)
allmentions.count().map(lambda x: 'Mentions in this batch: %s' % x).pprint()

# Discover
# Show how many per country events in last batch
# Put messages on the output Kafka queue
#countries = allevents.map(lambda event: event["Actor1Geo_CountryCode"])
#cnts = countries.countByValue()
#countriesroot = rootevents.map(lambda event: event["Actor1Geo_CountryCode"])
#cntsroot = countriesroot.countByValue()
# Publish discovered events to Kafka
#cnts.foreachRDD(discoverhandler)
#cnts.pprint()
#cntsroot.pprint()

ssc.start()
ssc.awaitTermination()

