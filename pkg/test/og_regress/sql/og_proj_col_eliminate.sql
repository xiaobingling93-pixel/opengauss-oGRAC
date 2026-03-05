-- test result result is no match dute to aggr func rs_col is eliminated
drop table if exists rs_col_t1;
drop table if exists rs_col_t2;
create table rs_col_t1 (id int, c_int int, c_varchar varchar(100));
create table rs_col_t2 (col1 int primary key, col2 int, col3 int, col8 number);
insert into rs_col_t1 values (1,2,'canscsaucdvdcsaicn');
insert into rs_col_t1 values (2,3,'scajeucheuicic');
insert into rs_col_t1 values (3,4,'cahjbyuvuckbusainv');
insert into rs_col_t2 values (1, 2, 3, 4);
insert into rs_col_t2 values (2, 3, 4, 5);
insert into rs_col_t2 values (3, 4, 5, 6);

create index idx_rs_col_t1_index_1 on rs_col_t1(id);
create index idx_rs_col_t1_index_2 on rs_col_t1(id,c_int);
create index idx_rs_col_t1_index_3 on rs_col_t1(upper(c_varchar));
analyze table rs_col_t1 compute statistics;
analyze table rs_col_t2 compute statistics;

drop table rs_col_t1;
drop table rs_col_t2;

-- test proj win col eliminate
drop table if exists proj_win_col_t;
create table proj_win_col_t (id int);
insert into proj_win_col_t values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11);
commit;
select temp.id from
    (select
         avg(id) over (partition by id order by id) new_id, id
     from proj_win_col_t where id < 10 group by id
    ) temp order by 1;

select temp.id, temp.sum_id from
    (select
         sum(id) over (partition by id order by id) sum_id,
         avg(id) over (partition by id order by id) avg_id,
         id
     from proj_win_col_t where id < 10 group by id
    ) temp order by 1, 2;

drop table if exists proj_win_col_t;

drop table if exists t_proj_over_limit;
create table t_proj_over_limit(id int, c_int int, c_vchar varchar(10));
insert into t_proj_over_limit values (1,0,'abc123'),(2,1,'abc1231'),(3,2,'abc1232'),(4,3,'abc1233'),(5,4,'abc1234'),(6,5,'abc1235'),(7,6,'abc1236'),(8,7,'abc1237');
insert into t_proj_over_limit values (9,8,'abc1238'),(10,9,'abc1239'),(11,10,'abc12310'),(12,11,'abc12311'),(13,12,'abc12312'),(14,13,'abc12313'),(15,14,'abc12314'),(16,15,'abc12315'),(17,16,'abc12316');
commit;

select t0.id,t0.c_int from (
                               select a.id, a.c_int, a.c_vchar, dense_rank() over (partition by a.c_vchar order by a.id) win_col
                               from t_proj_over_limit a group by a.id,a.c_int,a.c_vchar limit 10) t0 order by t0.id,t0.c_int;
-- test case_cond + winsort
select id,c_int from
    (select a.id,a.id+a.c_int c_int,
            case when (lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id) +100)*10 >250
                     then lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id)
                 else lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id)
                end
     from t_proj_over_limit a limit 10) order by 1,2;
-- test calc + winsort
select id,c_int from
    (select a.id,a.id+a.c_int c_int, (lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id) + 100) * 10
     from t_proj_over_limit a limit 10) order by id;
-- test func + winsort
select id,c_int from
    (select a.id,a.id+a.c_int c_int, cast (lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id) as bigint)
     from t_proj_over_limit a limit 10) order by id;

-- subquery has sort items and limit, the winsort rs_col can be eliminated
select t0.id,t0.c_int from (
                               select a.id, a.c_int, a.c_vchar, dense_rank() over (partition by a.c_vchar order by a.id) win_col
                               from t_proj_over_limit a group by a.id,a.c_int,a.c_vchar order by 1,2 limit 10) t0 order by t0.id,t0.c_int;

select id,c_int from
    (select a.id,a.id+a.c_int c_int,
            case when (lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id) +100)*10 >250
                     then lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id)
                 else lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id)
                end
     from t_proj_over_limit a order by 1,2 limit 10) order by 1,2;

select id,c_int from
    (select a.id,a.id+a.c_int c_int, cast (lag(a.c_int,0,null) OVER(PARTITION BY a.id,a.c_int order by a.id) as bigint)
     from t_proj_over_limit a order by 1,2 limit 10) order by id;
drop table if exists t_proj_over_limit;