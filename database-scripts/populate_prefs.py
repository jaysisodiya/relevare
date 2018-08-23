import pandas as pd
import string
import random
import MySQLdb
import os

myuser=os.environ['RELEVARE_USER']
mypass=os.environ['RELEVARE_PASS']

db = MySQLdb.connect(host='10.0.0.25', user=myuser, passwd=mypass, db='relevare')
cursor1 = db.cursor()

lowrange=1
batchsize=100000
maxrange=113000000

for i in range(batchsize,maxrange,batchsize):
    hirange=i
    print("Processing userids in range from ", lowrange, " to ", hirange)
    cursor1.execute('''select id, countrycode from users where id between %s and %s''',
                    (lowrange, hirange))
    rows = cursor1.fetchall()
    preftup=()
    preflist=[]
    for row in rows:
        preftup=(row[0],row[1])
        preflist.append(preftup)

    cursor1.executemany('''insert into prefs (usersid, pref1)
                           values (%s, %s)''', preflist)
    db.commit()
    lowrange += batchsize

db.commit()
cursor1.close()
db.close()
