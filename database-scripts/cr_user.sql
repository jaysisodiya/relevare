create table users (
  id 		int not null auto_increment,
  login		varchar(20) not null,
  pass		varchar(100) not null,
  fname		varchar(100) not null,
  lname		varchar(100) not null,
  zipcode	int null,
  statecode	varchar(5) null,
  state		varchar(100) null,
  countrycode	varchar(5) null,
  country	varchar(100) null,
  primary key (id)
)
