drop table if exists sse_t1;
drop table if exists sse_t2;
create table sse_t1(a int, b int, c int);
create table sse_t2(a int, b int, c int);

insert into sse_t1 values(1, 1, 1);
insert into sse_t1 values(2, 2, 2);
insert into sse_t1 values(3, 3, 3);
insert into sse_t2 values(1, 1, 1);
insert into sse_t2 values(2, 2, 2);
insert into sse_t2 values(3, 3, 3);
COMMIT;

explain select t0.*,t1.* from (select a, b from sse_t1) t0 left join (select a, b from sse_t2) t1 on t0.a = t1.a where t0.b=2;

select t0.*,t1.* from (select a, b from sse_t1) t0 left join (select a, b from sse_t2) t1 on t0.a = t1.a where t0.b=2;

explain select c1,c2 from (select t1.a c1, t2.b c2 from sse_t1 t1 join sse_t2 t2 on t1.a = t2.a where t1.b < 20 group by t1.a, t2.b) where c2 < 20 order by 1;

explain select * from (select count(a), b from sse_t1 group by b) sub_qry join sse_t2 t2 on sub_qry.b = t2.b;

explain select count(a) over() from (select a, b from sse_t1);

explain select count(a), b from (select a, b from sse_t1) group by cube(b);

explain select * from (select a , b from sse_t1) pivot(max(a) for b in ('1'));

explain select a, b from (select a, b from sse_t1 ) sub_qry where a in (select b from sse_t2 t2 where b = sub_qry.b);

explain select * from (select a, b from sse_t1 union all select a , b from sse_t2);

explain select * from (select * from dual);

explain select * from (select t1.a , t2.b from sse_t1 t1 inner join sse_t2 t2 on t1.a = t2.a);

explain select * from (select distinct a, b from sse_t1);

explain select * from (select count(a) from sse_t1);

explain select * from (select a,b from sse_t1 group by a,b);

explain select * from (select a, b from sse_t1 connect by a < b);

explain select * from (select a, max(b) over() from sse_t1 );

explain select * from (select a, b from sse_t1 order by a);

explain select * from (select a, b from sse_t1) pivot(max(a) for b in ('1'));

explain select * from (select a, b from sse_t1 limit 10);

explain select * from (select a, b from sse_t1 where rownum < 10);

explain select * from (select a, b from sse_t1 where b < 20) sub_qry connect by sub_qry.a < sub_qry.b;

explain select * from sse_t2 left join (select a, b from sse_t1 where b < 20) sub_qry on 1=1;

explain select * from (select a+b from sse_t1);