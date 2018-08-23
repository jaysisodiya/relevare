create view userprefs 
as 
select usersid, fname, lname, countrycode, pref1, pref2, pref3, pref4, pref5 
from prefs p, users u 
where p.usersid = u.id;
