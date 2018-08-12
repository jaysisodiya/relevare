import s3fs
import pandas as pd
import _mysql

fs = s3fs.S3FileSystem(anon=False)

df = pd.read_csv('s3://test-jaysisodiya/geocodes_allCountries.txt',sep=None, header=0, names=('countrycode', 'postalcode', 'placename', 'name1', 'code1', 'name2', 'code2', 'name3', 'code3', 'latitude', 'longitude', 'accuracy'))

db = _mysql.connect(host='10.0.0.25',user='',passwd='',db='relevare')

db.query("""select contcode from continents""")

r=db.store_result()    # collects all results in one shot and brings to client side, use_result() will keep on server

r.fetch_row(maxrows=0)  # can user r.fetch_row() for one row at a time

