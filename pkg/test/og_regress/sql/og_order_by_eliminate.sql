--test order array element error
create table t_order_array_1(id number(5,5), c_int int[]);
select id, c_int from t_order_array_1 union all select id, c_int from t_order_array_1 order by id desc, c_int[2] + c_int[3] desc limit 1; -- error
select id, c_int from t_order_array_1 union all select id, c_int from t_order_array_1 order by id desc, c_int[2], c_int[3] desc limit 1; -- error
select id, c_int[1] from t_order_array_1 union all select id, c_int[1] from t_order_array_1 order by id desc, c_int[1] desc limit 1; -- success
select id, c_int[2] from t_order_array_1 union all select id, c_int[2] from t_order_array_1 order by id desc, c_int[3] desc limit 1; -- error
drop table if exists t_order_array_1;

-- test the aggr func that order sensitive
drop table if exists test_order_test_order_t1;
create table test_order_test_order_t1 (id int, col1 varchar(10));
insert into test_order_test_order_t1 values (1,'cantian'),(2,'mysql'),(3,'Gauss');
explain select group_concat(col1 separator '|') from (select * from test_order_test_order_t1 order by id desc); -- no eliminate
select group_concat(col1 separator '|') from (select * from test_order_test_order_t1 order by id desc);
select array_agg(col1) from (select col1 from test_order_test_order_t1 order by id desc);
drop table if exists test_order_test_order_t1;

--test order array element error
drop table if exists t_order_array_1;
create table t_order_array_1(id number(5,5), c_int int[]);
select id, c_int from t_order_array_1 union all select id, c_int from t_order_array_1 order by id desc, c_int[2] + c_int[3] desc limit 1; -- error
select id, c_int from t_order_array_1 union all select id, c_int from t_order_array_1 order by id desc, c_int[2], c_int[3] desc limit 1; -- error
select id, c_int[1] from t_order_array_1 union all select id, c_int[1] from t_order_array_1 order by id desc, c_int[1] desc limit 1; -- success
select id, c_int[2] from t_order_array_1 union all select id, c_int[2] from t_order_array_1 order by id desc, c_int[3] desc limit 1; -- error
drop table if exists t_order_array_1;

--test order by with nested aggregate functions
drop table if exists test_order_t1;
create table test_order_t1 (id int);
insert into test_order_t1 values(1);
insert into test_order_t1 values(2);
select id from test_order_t1 group by id order by max(nvl2(nullif(3,id),3,id));
select id from test_order_t1 group by id order by max(id) + id;
select id from test_order_t1 group by id order by max(id) ,id;
drop table if exists test_order_t1;

drop table if exists test_order_t1;
create table test_order_t1 (
	id INT,
	quantity INT,
	price DECIMAL(10,2),
	discount DECIMAL(10,2)
);
insert into test_order_t1 values
(1, 5, 100, 10),
(2, 8, 120, 5),
(3, NULL, 150, NULL),
(4, 0, 200, 0);

select id from test_order_t1 group by id order by avg(coalesce(round(nullif(id + quantity, 0), 2 ), id ));
select id from test_order_t1 group by id order by max(abs(id));
select id from test_order_t1 group by id order by sum(round(abs(id * 2), 0));

create table test_order_t2 as select * from test_order_t1;

EXPLAIN SELECT t1.id, t1.quantity, t1.price, t2.discount FROM (SELECT * FROM test_order_t1 WHERE price > 100 ORDER BY id) t1 JOIN test_order_t2 t2 ON t1.id = t2.id;
SELECT t1.id, t1.quantity, t1.price, t2.discount FROM (SELECT * FROM test_order_t1 WHERE price > 100 ORDER BY id) t1 JOIN test_order_t2 t2 ON t1.id = t2.id;

EXPLAIN SELECT SUM(quantity) total_qty, AVG(price) avg_price FROM (SELECT * FROM test_order_t1 WHERE price > 100 ORDER BY id) t;
SELECT SUM(quantity) total_qty, AVG(price) avg_price FROM (SELECT * FROM test_order_t1 WHERE price > 100 ORDER BY id) t;

EXPLAIN SELECT discount, COUNT(*) cnt, SUM(quantity) total_qty FROM (SELECT * FROM test_order_t1 WHERE price > 50 ORDER BY id) t GROUP BY discount;
SELECT discount, COUNT(*) cnt, SUM(quantity) total_qty FROM (SELECT * FROM test_order_t1 WHERE price > 50 ORDER BY id) t GROUP BY discount;

EXPLAIN SELECT DISTINCT id FROM (SELECT DISTINCT id FROM test_order_t1 order by 1) t; 
SELECT DISTINCT id FROM (SELECT DISTINCT id FROM test_order_t1 order by 1) t;

EXPLAIN SELECT * FROM (SELECT * FROM test_order_t1 WHERE price > 100 ORDER BY id) t ORDER BY id;
SELECT * FROM (SELECT * FROM test_order_t1 WHERE price > 100 ORDER BY id) t ORDER BY id;

EXPLAIN SELECT * FROM (SELECT * FROM test_order_t1 WHERE price > 100 ORDER BY id) t ORDER BY id DESC;
SELECT * FROM (SELECT * FROM test_order_t1 WHERE price > 100 ORDER BY id) t ORDER BY id DESC;

EXPLAIN SELECT * FROM (SELECT * FROM test_order_t1 WHERE quantity IS NOT NULL ORDER BY price) t ORDER BY id;
SELECT * FROM (SELECT * FROM test_order_t1 WHERE quantity IS NOT NULL ORDER BY price) t ORDER BY id;

drop table if exists test_order_t1;
drop table if exists test_order_t2;