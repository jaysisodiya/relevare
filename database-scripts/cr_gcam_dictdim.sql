create table gcam_dictdim (
  dictdim_code		varchar(16) not null,
  dict_id		varchar(8)  not null,
  dim_id		varchar(8)  not null,
  dict_type		varchar(24),
  lang_code		varchar(12),
  dict_humanname	varchar(100),
  dim_humanname		varchar(100),
  dict_citation		varchar(400)
) ;
