drop table if exists test_mtrl_dept;
drop table if exists test_mtrl_emp;
drop table if exists test_mtrl_proj;

create table test_mtrl_dept (
    dept_id int primary key,
    dept_name varchar(50),
    budget numeric(15,2)
);

create table test_mtrl_emp (
    emp_id int primary key,
    emp_name varchar(50),
    salary numeric(10,2),
    hire_date date,
    dept_id int
);

create table test_mtrl_proj (
    proj_id int primary key,
    dept_id int,
    emp_id int,
    start_date date,
    end_date date,
    cost numeric(15,2)
);

INSERT INTO test_mtrl_dept (dept_id, dept_name, budget) VALUES
(1, '技术部', 5000000.00),
(2, '销售部', 3000000.00),
(3, '市场部', 2000000.00),
(4, '人力资源部', 1500000.00),
(5, '财务部', 2500000.00),
(6, '研发部', 8000000.00),
(7, '产品部', 3500000.00),
(8, '客户服务部', 1800000.00),
(9, '运营部', 2200000.00),
(10, '采购部', 1200000.00);

INSERT INTO test_mtrl_emp (emp_id, emp_name, salary, hire_date, dept_id) VALUES
(1, '张三', 15000.00, '2020-03-15', 1),
(2, '李四', 12000.00, '2021-05-20', 2),
(3, '王五', 18000.00, '2019-11-10', 1),
(4, '赵六', 9000.00, '2022-02-28', 3),
(5, '钱七', 22000.00, '2018-07-01', 6),
(6, '孙八', 13000.00, '2021-09-15', 4),
(7, '周九', 11000.00, '2022-06-30', 2),
(8, '吴十', 16000.00, '2020-12-05', 5),
(9, '郑十一', 14000.00, '2021-08-22', 7),
(10, '王十二', 19000.00, '2019-04-18', 6);

INSERT INTO test_mtrl_proj (proj_id, dept_id, emp_id, start_date, end_date, cost) VALUES
(1, 1, 1, '2023-01-10', '2023-06-30', 500000.00),
(2, 2, 2, '2023-02-15', '2023-08-15', 300000.00),
(3, 6, 5, '2023-03-01', '2023-12-31', 1500000.00),
(4, 1, 3, '2023-04-10', '2023-09-30', 800000.00),
(5, 3, 4, '2023-05-20', '2023-11-20', 200000.00),
(6, 6, 10, '2023-06-01', '2024-02-28', 1200000.00),
(7, 5, 8, '2023-07-15', '2023-12-15', 400000.00),
(8, 7, 9, '2023-08-01', '2024-01-31', 600000.00),
(9, 4, 6, '2023-09-10', '2024-03-10', 250000.00),
(10, 2, 7, '2023-10-05', '2024-04-05', 350000.00);

explain select * from test_mtrl_emp e where salary > (select avg(salary) from test_mtrl_emp where dept_id = e.dept_id) order by e.emp_id;
select * from test_mtrl_emp e where salary > (select avg(salary) from test_mtrl_emp where dept_id = e.dept_id) order by e.emp_id;

explain select emp_name,(select sum(p_cost) from (select cost + (select avg(cost) from test_mtrl_proj where dept_id = p.dept_id
and emp_id = p.emp_id) as p_cost from test_mtrl_proj p where p.dept_id = e.dept_id and p.emp_id = e.emp_id) sub ) as total_cost
from test_mtrl_emp e order by emp_name;
select emp_name,(select sum(p_cost) from (select cost + (select avg(cost) from test_mtrl_proj where dept_id = p.dept_id
and emp_id = p.emp_id) as p_cost from test_mtrl_proj p where p.dept_id = e.dept_id and p.emp_id = e.emp_id) sub ) as total_cost
from test_mtrl_emp e order by emp_name;

explain select dept_id, sum(cost) from test_mtrl_proj p group by dept_id having sum(cost) > (select max(cost) - min(cost) from 
test_mtrl_proj where dept_id = p.dept_id) order by dept_id;
select dept_id, sum(cost) from test_mtrl_proj p group by dept_id having sum(cost) > (select max(cost) - min(cost) from 
test_mtrl_proj where dept_id = p.dept_id) order by dept_id;

explain select (select count(*) from test_mtrl_proj where dept_id = d.dept_id and emp_id = (select emp_id from test_mtrl_emp where 
dept_id = d.dept_id order by salary desc limit 1)) as emp_count, dept_name from test_mtrl_dept d where 
budget > (select avg(p.cost) from test_mtrl_proj p where p.dept_id = d.dept_id and p.cost > (select count(*)*1000 
from test_mtrl_emp where dept_id = p.dept_id and emp_id = p.emp_id)) order by d.dept_name;
select (select count(*) from test_mtrl_proj where dept_id = d.dept_id and emp_id = (select emp_id from test_mtrl_emp where 
dept_id = d.dept_id order by salary desc limit 1)) as emp_count, dept_name from test_mtrl_dept d where 
budget > (select avg(p.cost) from test_mtrl_proj p where p.dept_id = d.dept_id and p.cost > (select count(*)*1000 
from test_mtrl_emp where dept_id = p.dept_id and emp_id = p.emp_id)) order by d.dept_name;

explain select dept_id, avg(salary) from test_mtrl_emp e where salary > (select avg(salary) from test_mtrl_emp where dept_id = e.dept_id) 
group by dept_id having avg(salary) > (select avg(salary) from test_mtrl_emp where dept_id = e.dept_id) order by dept_id;
select dept_id, avg(salary) from test_mtrl_emp e where salary > (select avg(salary) from test_mtrl_emp where dept_id = e.dept_id) 
group by dept_id having avg(salary) > (select avg(salary) from test_mtrl_emp where dept_id = e.dept_id) order by dept_id;

explain SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary > (SELECT AVG(salary) FROM test_mtrl_emp e2 WHERE e2.dept_id = e.dept_id);
SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary > (SELECT AVG(salary) FROM test_mtrl_emp e2 WHERE e2.dept_id = e.dept_id);

explain SELECT d.dept_name FROM test_mtrl_dept d WHERE d.budget > (SELECT AVG(budget) FROM test_mtrl_dept d2 WHERE d2.dept_id <> d.dept_id);
SELECT d.dept_name FROM test_mtrl_dept d WHERE d.budget > (SELECT AVG(budget) FROM test_mtrl_dept d2 WHERE d2.dept_id <> d.dept_id);

explain SELECT p.proj_id FROM test_mtrl_proj p WHERE p.cost > (SELECT AVG(cost) FROM test_mtrl_proj p2 WHERE p2.dept_id = p.dept_id);
SELECT p.proj_id FROM test_mtrl_proj p WHERE p.cost > (SELECT AVG(cost) FROM test_mtrl_proj p2 WHERE p2.dept_id = p.dept_id);

explain SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary > (SELECT AVG(salary) FROM test_mtrl_emp e2 WHERE e2.dept_id = e.dept_id) 
AND e.emp_id IN (SELECT p.emp_id FROM test_mtrl_proj p WHERE p.cost > (SELECT AVG(cost) FROM test_mtrl_proj p2 WHERE p2.dept_id = p.dept_id));
SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary > (SELECT AVG(salary) FROM test_mtrl_emp e2 WHERE e2.dept_id = e.dept_id) 
AND e.emp_id IN (SELECT p.emp_id FROM test_mtrl_proj p WHERE p.cost > (SELECT AVG(cost) FROM test_mtrl_proj p2 WHERE p2.dept_id = p.dept_id));

explain SELECT d.dept_name FROM test_mtrl_dept d WHERE d.budget > (SELECT AVG(budget) * 1.5 FROM test_mtrl_dept d2 WHERE d2.dept_id <> d.dept_id);
SELECT d.dept_name FROM test_mtrl_dept d WHERE d.budget > (SELECT AVG(budget) * 1.5 FROM test_mtrl_dept d2 WHERE d2.dept_id <> d.dept_id);

explain SELECT e.emp_name FROM test_mtrl_emp e WHERE EXISTS (SELECT 1 FROM test_mtrl_proj p WHERE p.emp_id = e.emp_id AND 
p.cost > (SELECT AVG(cost) FROM test_mtrl_proj p2 WHERE p2.dept_id = p.dept_id));
SELECT e.emp_name FROM test_mtrl_emp e WHERE EXISTS (SELECT 1 FROM test_mtrl_proj p WHERE p.emp_id = e.emp_id AND 
p.cost > (SELECT AVG(cost) FROM test_mtrl_proj p2 WHERE p2.dept_id = p.dept_id));

explain SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary > (SELECT MAX(salary) * 0.8 FROM test_mtrl_emp e2 WHERE e2.dept_id = e.dept_id);
SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary > (SELECT MAX(salary) * 0.8 FROM test_mtrl_emp e2 WHERE e2.dept_id = e.dept_id);

explain SELECT p.proj_id FROM test_mtrl_proj p WHERE p.cost > (SELECT MIN(budget) * 0.2 FROM test_mtrl_dept d WHERE d.dept_id = p.dept_id);
SELECT p.proj_id FROM test_mtrl_proj p WHERE p.cost > (SELECT MIN(budget) * 0.2 FROM test_mtrl_dept d WHERE d.dept_id = p.dept_id);

explain SELECT e.emp_name FROM test_mtrl_emp e WHERE e.hire_date > (SELECT MIN(hire_date) FROM test_mtrl_emp e2 WHERE 
e2.dept_id = e.dept_id) AND e.hire_date < (SELECT MAX(hire_date) FROM test_mtrl_emp e3 WHERE e3.dept_id = e.dept_id);
SELECT e.emp_name FROM test_mtrl_emp e WHERE e.hire_date > (SELECT MIN(hire_date) FROM test_mtrl_emp e2 WHERE 
e2.dept_id = e.dept_id) AND e.hire_date < (SELECT MAX(hire_date) FROM test_mtrl_emp e3 WHERE e3.dept_id = e.dept_id);

explain SELECT d.dept_name FROM test_mtrl_dept d WHERE d.budget > (SELECT SUM(cost) FROM test_mtrl_proj p WHERE p.dept_id = d.dept_id);
SELECT d.dept_name FROM test_mtrl_dept d WHERE d.budget > (SELECT SUM(cost) FROM test_mtrl_proj p WHERE p.dept_id = d.dept_id);

explain SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary > (SELECT AVG(salary) FROM test_mtrl_emp e2 WHERE 
e2.dept_id = e.dept_id AND e2.hire_date < e.hire_date);
SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary > (SELECT AVG(salary) FROM test_mtrl_emp e2 WHERE 
e2.dept_id = e.dept_id AND e2.hire_date < e.hire_date);

explain SELECT p.proj_id FROM test_mtrl_proj p WHERE p.cost > (SELECT AVG(cost) * 2 FROM test_mtrl_proj p2 WHERE 
p2.dept_id = p.dept_id AND p2.start_date < p.start_date);
SELECT p.proj_id FROM test_mtrl_proj p WHERE p.cost > (SELECT AVG(cost) * 2 FROM test_mtrl_proj p2 WHERE 
p2.dept_id = p.dept_id AND p2.start_date < p.start_date);

explain SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary < (SELECT MIN(salary) FROM test_mtrl_emp e2 WHERE 
e2.dept_id = e.dept_id AND e2.emp_id IN (SELECT p.emp_id FROM test_mtrl_proj p WHERE p.cost > (SELECT AVG(cost) FROM test_mtrl_proj p2 WHERE p2.dept_id = p.dept_id)));
SELECT e.emp_name FROM test_mtrl_emp e WHERE e.salary < (SELECT MIN(salary) FROM test_mtrl_emp e2 WHERE 
e2.dept_id = e.dept_id AND e2.emp_id IN (SELECT p.emp_id FROM test_mtrl_proj p WHERE p.cost > (SELECT AVG(cost) FROM test_mtrl_proj p2 WHERE p2.dept_id = p.dept_id)));


explain SELECT d.dept_name FROM test_mtrl_dept d WHERE (SELECT COUNT(*) FROM test_mtrl_emp e WHERE 
e.dept_id = d.dept_id) > (SELECT AVG(emp_count) FROM (SELECT COUNT(*) as emp_count FROM test_mtrl_emp e2 GROUP BY e2.dept_id) sub);
SELECT d.dept_name FROM test_mtrl_dept d WHERE (SELECT COUNT(*) FROM test_mtrl_emp e WHERE 
e.dept_id = d.dept_id) > (SELECT AVG(emp_count) FROM (SELECT COUNT(*) as emp_count FROM test_mtrl_emp e2 GROUP BY e2.dept_id) sub);