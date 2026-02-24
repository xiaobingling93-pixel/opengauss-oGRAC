drop table if exists cast_to_raw_t1;
create table cast_to_raw_t1(c1 varchar(20), c2 int);
insert into cast_to_raw_t1 values('a', 1);
select utl_raw.cast_to_raw('a');
select to_blob(utl_raw.cast_to_raw('a'));
select utl_raw.cast_to_raw(c1), utl_raw.cast_to_raw('a') from cast_to_raw_t1;
select to_blob(utl_raw.cast_to_raw(c1)), to_blob(utl_raw.cast_to_raw('a')) from cast_to_raw_t1;
select utl_raw.cast_to_raw(1);
drop table if exists cast_to_raw_t1;