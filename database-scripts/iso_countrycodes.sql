create table iso_countrycodes (
  country		varchar(200) not null,
  alpha2code 		varchar(10) not null,
  alpha3code		varchar(10) not null,
  numericcode		INT(10) not null,
  latitude		float(10,6) not null,
  longitude		float(10,6)  not null
)
engine=csv;
