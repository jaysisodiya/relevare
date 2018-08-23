create table prefs (
  id 		int not null auto_increment,
  usersid	int(11) not null,
  pref1  	varchar(100) null,
  pref2		varchar(100) null,
  pref3		varchar(100) null,
  pref4		varchar(100) null,
  pref5		varchar(100) null,
  primary key (id)
)
