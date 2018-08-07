create table geocodes (
  id	      int not null auto_increment,
  countrycode char(2) not null,
  postalcode  varchar(20) not null,
  placename   varchar(180) not null,
  name1       varchar(100),
  code1       varchar(20),
  name2       varchar(100),
  code2       varchar(20),
  name3       varchar(100),
  code3       varchar(20),
  latitude    float(10, 6),
  longitude   float(10, 6),
  accuracy    int(1),
  primary key (id)
) ;

describe geocodes ;
