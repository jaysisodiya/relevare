import s3fs
import pandas as pd
import _mysql

fs = s3fs.S3FileSystem(anon=False)

#df = pd.read_csv('s3://test-jaysisodiya/geocodes_allCountries.txt',sep=None, header=0, names=('countrycode', 'postalcode', 'placename', 'name1', 'code1', 'name2', 'code2', 'name3', 'code3', 'latitude', 'longitude', 'accuracy'))

# Get the GDELT field names from a helper file
colnames = pd.read_excel('CSV.header.fieldids.xlsx', sheet_name='Sheet1', index_col='Column ID', usecols=1)['Field Name']
#print(colnames)

df = pd.read_csv('20180730.export.csv', sep='\t', low_memory=False, header=None, dtype=str, names=colnames, index_col=['GLOBALEVENTID'])
print(df.head())
print(df.Actor1Geo_CountryCode.unique())
print(df.Actor1Geo_CountryCode.value_counts())


#db = _mysql.connect(host='10.0.0.25',user="",passwd="",db='relevare')
#db.query("""select contcode from continents""")
#r=db.store_result()    # collects all results in one shot and brings to client side, use_result() will keep on server
#r.fetch_row(maxrows=0)  # can user r.fetch_row() for one row at a time

