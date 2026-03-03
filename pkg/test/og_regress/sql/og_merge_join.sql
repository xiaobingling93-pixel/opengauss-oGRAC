--- prepare
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;
drop table if exists t6;
drop table if exists t7;
drop table if exists t8;
drop table if exists t9;
create table t1 (c1 int, c2 int, c3 int);
create table t2 (c1 int, c2 int, c3 int);
create table t3 (c1 int);
create table t4 (c1 varchar(20));
create table t5 (c1 int, c2 int, c3 int);
create table t6 (c1 int, c2 int, c3 int);
create table t7 (c1 int, c2 int, c3 int);
create table t8 (c1 int, c2 int, c3 int);
create table t9 (c1 int, c2 int, c3 int);

create or replace procedure batch_insert(tbl varchar, dx int, cnt int) as
i int;
x int;
v_sql varchar(100);
begin
    i := 0;
    x := 0;
    while i < cnt loop
        v_sql := 'INSERT INTO ' ||tbl|| ' VALUES(' ||x|| ',' ||x|| ',' ||x|| ')';
        execute immediate v_sql;
        i := i+1;
        x := x+dx;
    end loop;
    commit;
end;
/
exec batch_insert('t5', 1, 1000);
exec batch_insert('t6', 1, 1000);
exec batch_insert('t7', 1, 1000);
exec batch_insert('t5', -1, 1000);
exec batch_insert('t6', -1, 1000);
exec batch_insert('t7', -1, 1000);
exec batch_insert('t8', 1, 3000);
exec batch_insert('t9', 1, 6000);

insert into t1 values (1,4,4),(2,3,3),(3,2,2),(4,1,1);
insert into t2 values (1,4,4),(2,3,3),(3,2,2),(4,1,1);
insert into t3 values (3);
insert into t4 values ('11111'),('3');

analyze table  t1  compute STATISTICS;
analyze table  t2  compute STATISTICS;
analyze table  t3  compute STATISTICS;
analyze table  t4  compute STATISTICS;
analyze table  t5  compute STATISTICS;
analyze table  t6  compute STATISTICS;
analyze table  t7  compute STATISTICS;

--- disable nl join
alter system set ENABLE_NESTLOOP_JOIN=false;

explain select t1.c1, t2.c1 from t1 join t2 on t1.c1 = t2.c1 ;

--- use_merge hint
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 = t2.c1;

--- merge join plan requirements
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 = t2.c1;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 < t2.c1;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 > t2.c1;

explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1,t2 where t1.c1 = t2.c1;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 cross join t2 where t1.c1 = t2.c1;

explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 left join t2 on t1.c1 = t2.c1;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 right join t2 on t1.c1 = t2.c1;

explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on true;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 = t2.c1 or true;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 = t2.c1 and true;

explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 <> t2.c1 and t1.c2 = t2.c2;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c2 = t2.c2 and t1.c1 <> t2.c1;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 <> t2.c1;
explain select /*+ use_merge(t1 t2) */ t1.c1, t2.c1 from t1 join t2 on t1.c1 <> t2.c1 where t1.c2 = t2.c2;

--- Inconsistent Left and Right Data Types
explain select /*+ use_merge(t3 t4) */ t3.c1, t4.c1 from t3 join t4 on t3.c1 = t4.c1;
select /*+ use_merge(t3 t4) */ t3.c1, t4.c1 from t3 join t4 on t3.c1 = t4.c1;

--- sub query rewrite
explain select  /*+ use_merge(t1 t2) */ count(*) from t1 where t1.c1 in (select c1 from t2);
select  /*+ use_merge(t1 t2) */ count(*) from t1 where t1.c1 in (select c1 from t2);

--- filter pushdown
explain select * from t5 join t6 on t5.c1 = t6.c1 and t5.c2 = t6.c2  join t7 on t5.c1 = t7.c1;

--- single column index

create index idx_t5_c1 on t5(c1);
analyze table  t5  compute STATISTICS;

explain select count(*) from t5 join t6 on t5.c1 = t6.c1;
select count(*) from t5 join t6 on t5.c1 = t6.c1;

explain select count(*) from t5 join t6 on floor(t5.c1) = t6.c1;
select count(*) from t5 join t6 on floor(t5.c1) = t6.c1;

explain select count(*) from t5 join t6 on t6.c1 = t5.c1;
select count(*) from t5 join t6 on t6.c1 = t5.c1;

explain select count(*) from t5 join t6 on t5.c2 = t6.c2;
select count(*) from t5 join t6 on t5.c2 = t6.c2;

explain select count(*) from t5 join t6 on t5.c2 = t6.c2 where t5.c1 = 1;
select count(*) from t5 join t6 on t5.c2 = t6.c2 where t5.c1 = 1;

explain select count(*) from t5 join t6 on t5.c2 = t6.c2 and t5.c1 = t6.c1;
select count(*) from t5 join t6 on t5.c2 = t6.c2 and t5.c1 = t6.c1;

explain select count(*) from t5 join t6 on t5.c1 = t6.c1 and t5.c2 = t6.c2;
select count(*) from t5 join t6 on t5.c1 = t6.c1 and t5.c2 = t6.c2;

drop index idx_t5_c1 on t5;

alter system flush sqlpool;

---  the order of columns in composite index
create index idx_t5_c12 on t5(c1,c2);
analyze table  t5  compute STATISTICS;

explain select count(*) from t5 join t6 on t5.c1 = t6.c1 ;
select count(*) from t5 join t6 on t5.c1 = t6.c1 ;

explain select count(*) from t5 join t6 on t6.c1 = t5.c1 ;
select count(*) from t5 join t6 on t6.c1 = t5.c1 ;

explain select count(*) from t5 join t6 on t5.c2 = t6.c2 ;
select count(*) from t5 join t6 on t5.c2 = t6.c2 ;

explain select count(*) from t5 join t6 on t5.c2 = t6.c2 and t5.c1 = t6.c1 ;
select count(*) from t5 join t6 on t5.c2 = t6.c2 and t5.c1 = t6.c1 ;

explain select count(*) from t5 join t6 on t5.c1 = t6.c1 and t5.c2 = t6.c2 ;
select count(*) from t5 join t6 on t5.c1 = t6.c1 and t5.c2 = t6.c2 ;

drop index idx_t5_c12 on t5;

alter system flush sqlpool;

--- index scan direction
create index idx_t5_c1 on t5(c1);
create index idx_t6_c1 on t6(c1);
analyze table  t5  compute STATISTICS;
analyze table  t6  compute STATISTICS;

explain select count(*) from t5 join t6 on t5.c1 < t6.c1;
select count(*) from t5 join t6 on t5.c1 < t6.c1;

explain select count(*) from t5 join t6 on t5.c1 = t6.c1;
select count(*) from t5 join t6 on t5.c1 = t6.c1;

explain select count(*) from t5 join t6 on t5.c1 > t6.c1;
select count(*) from t5 join t6 on t5.c1 > t6.c1;

drop index idx_t5_c1 on t5;
drop index idx_t6_c1 on t6;

alter system flush sqlpool;

--- multi join

create index idx_t5_c1 on t5(c1);
create index idx_t6_c1 on t6(c1);
create index idx_t7_c1 on t7(c1);
analyze table  t5  compute STATISTICS;
analyze table  t6  compute STATISTICS;
analyze table  t7  compute STATISTICS;

explain select count(*) from t5 join t6 on t5.c1 < t6.c1 join t7 on t6.c1 < t7.c1;

explain select count(*) from t5 join t6 on t5.c1 > t6.c1 join t7 on t6.c1 > t7.c1;

explain select count(*) from t5 join t6 on t5.c1 = t6.c1 join t7 on t6.c1 = t7.c1;

explain select count(*) from t5 join t6 on t5.c1 < t6.c1 join t7 on t6.c1 > t7.c1;

explain select count(*) from t5 join t6 on t5.c1 > t6.c1 join t7 on t6.c1 < t7.c1;

explain select count(*) from t5 join ( t6 join t7 on t6.c1  > t7.c1) on t5.c1  > t7.c1;

--- t8,t9 build enough data to across merge mtrl pages.
insert into t8 select * from t8;
--- make key for the case of no-need sort mtrl rows
create index idx_t8_c1 on t8(c1);
analyze table t8 compute STATISTICS;
analyze table t9 compute STATISTICS;
explain select /*+use_merge(t8, t9)*/ count(t8.c1) from t8 inner join t9 on t8.c1 = t9.c3;
select /*+use_merge(t8, t9)*/ count(t8.c1) from t8 inner join t9 on t8.c1 = t9.c3;

drop index idx_t5_c1 on t5;
drop index idx_t6_c1 on t6;
drop index idx_t7_c1 on t7;

alter system flush sqlpool;

--- clean
alter system set ENABLE_NESTLOOP_JOIN=true;
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;
drop table if exists t6;
drop table if exists t7;
drop table if exists t8;
drop table if exists t9;