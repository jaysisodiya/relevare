create table continents (
  id		int not null auto_increment,
  contcode	char(2) not null,
  contname	varchar(20) not null,
  geonameid	int(10) not null, 
  primary key (id)
) ;

describe continents ;

insert into continents (contcode, contname, geonameid) values ( 'AF', 'Africa', '6255146') ;
insert into continents (contcode, contname, geonameid) values ( 'AS', 'Asia', '6255147') ;
insert into continents (contcode, contname, geonameid) values ( 'EU', 'Europe', '6255148') ;
insert into continents (contcode, contname, geonameid) values ( 'NA', 'North America', '6255149') ;
insert into continents (contcode, contname, geonameid) values ( 'OC', 'Oceania', '6255151') ;
insert into continents (contcode, contname, geonameid) values ( 'SA', 'South America', '6255150') ;
insert into continents (contcode, contname, geonameid) values ( 'AN', 'Antarctica', '6255152') ;

select * from continents ;

create table geonames (
  id		int not null auto_increment,
  geonameid	int not null,
  name		varchar(200),
  asciiname	varchar(200),
  alternames	varchar(10000),
  latitude	float(10, 6),
  longitude	float(10, 6),
  featureclass	char(1),
  featurecode	varchar(10),
  countrycode	char(2),
  cc2		varchar(200),
  code1		varchar(20),
  code2		varchar(80),
  code3		varchar(20),
  code4		varchar(20),
  population	bigint,
  dem		int,
  timezone	varchar(40),
  datemodified	date,
  primary key (id)
) ;

describe geonames ;
