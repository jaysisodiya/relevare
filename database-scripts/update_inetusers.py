import pandas as pd
import MySQLdb as mysqldb
import os

myuser=os.environ['RELEVARE_USER']
mypass=os.environ['RELEVARE_PASS']

db = mysqldb.connect(host='10.0.0.25', user=myuser, passwd=mypass, db='relevare')
cursor1 = db.cursor()

df = pd.read_csv('/tmp/countries_by_internetusers.csv', keep_default_na=False)

print(df.keys())

for index, cols in df.iterrows():
    c = cols['Country_code']
    r = cols['Rank']
    u = cols['Users']

    print(c, r, u)

    cursor1.execute(
        """update country_codes
           set inet_users = %s,
               inet_rank = %s
           where country_code = %s""", (u,r,c)
    )

db.commit()
cursor1.close()
db.close()
