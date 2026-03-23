drop table if exists update_push_down_t1;
create table update_push_down_t1 (c1 int not null, c2 int);
insert into update_push_down_t1 values (1,1),(2,2);

begin
execute immediate 'update
(update_push_down_t1 as tab_0) 
left join 
((select 1 as c0 from sys_dummy as tab_1) as subqry_0) on (subqry_0.c0 = (select 2 from sys_dummy))
set tab_0.c1 = (select 3 from sys_dummy)';
end;
/

explain update
(update_push_down_t1 as tab_0) 
left join 
((select 1 as c0 from sys_dummy as tab_1) as subqry_0) on (subqry_0.c0 = (select 2 from sys_dummy))
set tab_0.c1 = (select 3 from sys_dummy);

update
(update_push_down_t1 as tab_0) 
left join 
((select 1 as c0 from sys_dummy as tab_1) as subqry_0) on (subqry_0.c0 = (select 2 from sys_dummy))
set tab_0.c1 = (select 3 from sys_dummy);

select * from update_push_down_t1 order by 1, 2;

drop table if exists update_push_down_t1;
create table update_push_down_t1 (c1 int not null, c2 int);
insert into update_push_down_t1 values (1,1),(2,2);

begin
execute immediate 'update
(update_push_down_t1 as tab_0) 
left join 
((select 1 as c0 from sys_dummy as tab_1) as subqry_0) on (subqry_0.c0 = (select 2 from sys_dummy))
set tab_0.c1 = (select 3 from sys_dummy)';
end;
/

update
(update_push_down_t1 as tab_0) 
left join 
((select 1 as c0 from sys_dummy as tab_1) as subqry_0) on (subqry_0.c0 = (select 2 from sys_dummy))
set tab_0.c1 = (select 3 from sys_dummy);

select * from update_push_down_t1 order by 1, 2;
drop table if exists update_push_down_t1;

drop table if exists push_down_table_t1;
create table push_down_table_t1(div1 int, div2 int);
insert into push_down_table_t1 values(1, 0);
insert into push_down_table_t1 values(2, 1);
commit;
alter system set _OPTIM_SUBQUERY_ELIMINATION=false;
select * from (select div1/div2 avg from push_down_table_t1 where div1 > 0 and div2 > 0) where avg < 3;
alter system set _OPTIM_SUBQUERY_ELIMINATION=true;
alter system flush sqlpool;
select * from (select div1/div2 avg from push_down_table_t1 where div1 > 0 and div2 > 0) where avg < 3;
drop table if exists push_down_table_t1;

drop table if exists push_down_t1;
create table push_down_t1(c1 int, c2 int);
insert into push_down_t1 values(1, 0);
insert into push_down_t1 values(2, 1);
create table push_down_t2 as select * from push_down_t1;

EXPLAIN SELECT * FROM (SELECT * FROM push_down_t1 WHERE c2 > 20) ORDER BY c1 DESC;
SELECT * FROM (SELECT * FROM push_down_t1 WHERE c2 > 20) ORDER BY c1 DESC;

EXPLAIN SELECT * FROM (SELECT * FROM push_down_t1) ORDER BY c1 ASC;
SELECT * FROM (SELECT * FROM push_down_t1) ORDER BY c1 ASC;

EXPLAIN SELECT * FROM (SELECT * FROM push_down_t1 WHERE c2 > 20 ORDER BY c1) ORDER BY c2 DESC;
SELECT * FROM (SELECT * FROM push_down_t1 WHERE c2 > 20 ORDER BY c1) ORDER BY c2 DESC;

EXPLAIN SELECT * FROM (SELECT * FROM push_down_t1 WHERE c1 > 2 UNION ALL SELECT * FROM push_down_t2 WHERE c2 < 5) t ORDER BY c1;
SELECT * FROM (SELECT * FROM push_down_t1 WHERE c1 > 2 UNION ALL SELECT * FROM push_down_t2 WHERE c2 < 5) t ORDER BY c1;

EXPLAIN SELECT * FROM (SELECT * FROM push_down_t1 WHERE c1 > 2 UNION SELECT * FROM push_down_t2 WHERE c2 < 5) t ORDER BY c1;
SELECT * FROM (SELECT * FROM push_down_t1 WHERE c1 > 2 UNION SELECT * FROM push_down_t2 WHERE c2 < 5) t ORDER BY c1;

EXPLAIN SELECT * FROM (SELECT * FROM push_down_t1 WHERE c1 > 2 EXCEPT SELECT * FROM push_down_t2 WHERE c2 < 5) t ORDER BY c1;
SELECT * FROM (SELECT * FROM push_down_t1 WHERE c1 > 2 EXCEPT SELECT * FROM push_down_t2 WHERE c2 < 5) t ORDER BY c1;

EXPLAIN SELECT * FROM (SELECT * FROM push_down_t1 WHERE c1 > 2 INTERSECT SELECT * FROM push_down_t2 WHERE c2 < 5) t ORDER BY c1;
SELECT * FROM (SELECT * FROM push_down_t1 WHERE c1 > 2 INTERSECT SELECT * FROM push_down_t2 WHERE c2 < 5) t ORDER BY c1;

EXPLAIN SELECT * FROM (SELECT c1, COUNT(*) cnt FROM push_down_t1 GROUP BY c1) ORDER BY 2 DESC;
SELECT * FROM (SELECT c1, COUNT(*) cnt FROM push_down_t1 GROUP BY c1) ORDER BY 2 DESC;

EXPLAIN SELECT * FROM (SELECT c1, COUNT(*) cnt FROM push_down_t1 GROUP BY c1 LIMIT 2) ORDER BY 1 DESC;
SELECT * FROM (SELECT c1, COUNT(*) cnt FROM push_down_t1 GROUP BY c1 LIMIT 2) ORDER BY 1 DESC;
