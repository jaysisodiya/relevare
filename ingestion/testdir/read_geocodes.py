import pandas as pd

df = pd.read_csv('geocodes_allCountries.txt',sep=None, header=0, names=('countrycode', 'postalcode', 'placename', 'name1', 'code1', 'name2', 'code2', 'name3', 'code3', 'latitude', 'longitude', 'accuracy'))

print(df.head())
print(df.countrycode.value_counts())
