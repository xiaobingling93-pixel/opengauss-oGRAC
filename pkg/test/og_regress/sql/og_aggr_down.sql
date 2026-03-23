-- 部门表
DROP TABLE IF EXISTS departments;
CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50),
    location VARCHAR(50)
);

-- 员工表
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    dept_id INT,
    salary DECIMAL(10,2),
    hire_date DATE
);

-- 订单表
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    cust_id INT,
    emp_id INT,
    amount DECIMAL(10,2),
    order_date DATE
);

-- 客户表
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    cust_id INT PRIMARY KEY,
    cust_name VARCHAR(50),
    city VARCHAR(50)
);

-- 产品表
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    prod_id INT PRIMARY KEY,
    prod_name VARCHAR(50),
    category VARCHAR(30),
    price DECIMAL(10,2)
);

-- 销售表
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    sale_id INT PRIMARY KEY,
    prod_id INT,
    emp_id INT,
    quantity INT,
    sale_date DATE
);

-- 部门数据
INSERT INTO departments VALUES
(1, 'Sales', 'New York'),
(2, 'Engineering', 'San Francisco'),
(3, 'Marketing', 'Chicago'),
(4, 'HR', 'Boston'),
(5, 'Finance', 'New York');

-- 员工数据
INSERT INTO employees VALUES
(101, 'Alice', 1, 8000.00, '2020-01-15'),
(102, 'Bob', 1, 7500.00, '2020-03-20'),
(103, 'Charlie', 2, 12000.00, '2019-06-10'),
(104, 'David', 2, 11000.00, '2019-08-05'),
(105, 'Eve', 3, 6500.00, '2021-02-28'),
(106, 'Frank', 3, 6200.00, '2021-05-12'),
(107, 'Grace', 4, 5500.00, '2022-01-10'),
(108, 'Henry', 4, 5300.00, '2022-03-15'),
(109, 'Ivy', 5, 7000.00, '2020-11-01'),
(110, 'Jack', 5, 6800.00, '2021-07-19');

-- 客户数据
INSERT INTO customers VALUES
(201, 'ABC Corp', 'New York'),
(202, 'XYZ Inc', 'San Francisco'),
(203, 'Tech Solutions', 'Chicago'),
(204, 'Global Trade', 'Boston'),
(205, 'Best Buy', 'New York');

-- 订单数据
INSERT INTO orders VALUES
(301, 201, 101, 1500.00, '2023-01-10'),
(302, 201, 102, 2300.00, '2023-01-15'),
(303, 202, 103, 5000.00, '2023-02-05'),
(304, 202, 104, 4200.00, '2023-02-10'),
(305, 203, 105, 1800.00, '2023-03-01'),
(306, 203, 106, 2100.00, '2023-03-05'),
(307, 204, 107, 900.00, '2023-04-12'),
(308, 204, 108, 1100.00, '2023-04-15'),
(309, 205, 109, 3200.00, '2023-05-20'),
(310, 205, 110, 2800.00, '2023-05-25');

-- 产品数据
INSERT INTO products VALUES
(401, 'Laptop', 'Electronics', 800.00),
(402, 'Mouse', 'Electronics', 25.00),
(403, 'Keyboard', 'Electronics', 45.00),
(404, 'Monitor', 'Electronics', 300.00),
(405, 'Desk', 'Furniture', 150.00),
(406, 'Chair', 'Furniture', 200.00);

-- 销售数据
INSERT INTO sales VALUES
(501, 401, 101, 5, '2023-01-05'),
(502, 402, 101, 20, '2023-01-06'),
(503, 403, 102, 15, '2023-01-10'),
(504, 404, 103, 3, '2023-02-01'),
(505, 401, 104, 2, '2023-02-15'),
(506, 405, 105, 4, '2023-03-01'),
(507, 406, 106, 6, '2023-03-10'),
(508, 401, 107, 1, '2023-04-01'),
(509, 402, 108, 10, '2023-04-15'),
(510, 403, 109, 8, '2023-05-01');

EXPLAIN SELECT d.dept_name, MIN(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date;
SELECT d.dept_name, MIN(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date;

EXPLAIN SELECT d.dept_name, MAX(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date;
SELECT d.dept_name, MAX(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date;

EXPLAIN SELECT d.dept_name, SUM(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date;
SELECT d.dept_name, SUM(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date;

EXPLAIN SELECT d.dept_name, MIN(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date LIMIT 10;
SELECT d.dept_name, MIN(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date LIMIT 10;

EXPLAIN SELECT d.dept_name, MIN(e.salary) FROM employees e cross join  departments d WHERE e.ROWID > 100 GROUP BY d.dept_name, e.hire_date;
SELECT d.dept_name, MIN(e.salary) FROM employees e cross join  departments d WHERE e.ROWID > 100 GROUP BY d.dept_name, e.hire_date;

EXPLAIN SELECT d.dept_name, MIN(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name;

EXPLAIN SELECT d.dept_name, MIN(e.salary), MAX(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date;
SELECT d.dept_name, MIN(e.salary), MAX(e.salary) FROM employees e cross join  departments d GROUP BY d.dept_name, e.hire_date;

EXPLAIN SELECT d.dept_name, MIN(d.dept_id) FROM employees e cross join  departments d GROUP BY d.dept_name;
SELECT d.dept_name, MIN(d.dept_id) FROM employees e cross join  departments d GROUP BY d.dept_name;

EXPLAIN SELECT d.dept_name, MIN(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.dept_name;

EXPLAIN SELECT c.city, MAX(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.city;
SELECT c.city, MAX(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.city;

EXPLAIN SELECT p.category, MAX(s.quantity) FROM sales s, products p WHERE s.prod_id = p.prod_id GROUP BY p.category;
SELECT p.category, MAX(s.quantity) FROM sales s, products p WHERE s.prod_id = p.prod_id GROUP BY p.category;

EXPLAIN SELECT d.location, MIN(e.salary), MAX(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.location;
SELECT d.location, MIN(e.salary), MAX(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.location;

EXPLAIN SELECT c.cust_name, MIN(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.cust_name;
SELECT c.cust_name, MIN(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.cust_name;

EXPLAIN SELECT p.prod_name, MAX(s.quantity) FROM sales s, products p WHERE s.prod_id = p.prod_id GROUP BY p.prod_name;
SELECT p.prod_name, MAX(s.quantity) FROM sales s, products p WHERE s.prod_id = p.prod_id GROUP BY p.prod_name;

EXPLAIN SELECT d.dept_name, MIN(e.hire_date) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.hire_date) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.dept_name;

EXPLAIN SELECT c.city, MAX(o.order_date) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.city;
SELECT c.city, MAX(o.order_date) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.city;

EXPLAIN SELECT p.category, MIN(p.price), MAX(s.quantity) FROM products p, sales s WHERE p.prod_id = s.prod_id GROUP BY p.category;
SELECT p.category, MIN(p.price), MAX(s.quantity) FROM products p, sales s WHERE p.prod_id = s.prod_id GROUP BY p.category;

EXPLAIN SELECT d.location, MAX(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id AND e.salary > 6000 GROUP BY d.location;
SELECT d.location, MAX(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id AND e.salary > 6000 GROUP BY d.location;

EXPLAIN SELECT c.city, MIN(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id AND o.amount > 1000 GROUP BY c.city;
SELECT c.city, MIN(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id AND o.amount > 1000 GROUP BY c.city;

EXPLAIN SELECT p.category, MAX(s.quantity) FROM sales s, products p WHERE s.prod_id = p.prod_id AND s.quantity > 5 GROUP BY p.category;
SELECT p.category, MAX(s.quantity) FROM sales s, products p WHERE s.prod_id = p.prod_id AND s.quantity > 5 GROUP BY p.category;

EXPLAIN SELECT d.dept_name, MIN(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id AND d.location = 'New York' GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id AND d.location = 'New York' GROUP BY d.dept_name;

EXPLAIN SELECT c.cust_name, MAX(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id AND c.city = 'Chicago' GROUP BY c.cust_name;
SELECT c.cust_name, MAX(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id AND c.city = 'Chicago' GROUP BY c.cust_name;

EXPLAIN SELECT p.prod_name, MIN(s.sale_date) FROM sales s, products p WHERE s.prod_id = p.prod_id AND p.category = 'Electronics' GROUP BY p.prod_name;
SELECT p.prod_name, MIN(s.sale_date) FROM sales s, products p WHERE s.prod_id = p.prod_id AND p.category = 'Electronics' GROUP BY p.prod_name;

EXPLAIN SELECT d.dept_name, COUNT(e.emp_id) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.dept_name;
SELECT d.dept_name, COUNT(e.emp_id) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.dept_name;

EXPLAIN SELECT c.city, SUM(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.city;
SELECT c.city, SUM(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.city;

EXPLAIN SELECT p.category, AVG(s.quantity) FROM sales s, products p WHERE s.prod_id = p.prod_id GROUP BY p.category;
SELECT p.category, AVG(s.quantity) FROM sales s, products p WHERE s.prod_id = p.prod_id GROUP BY p.category;

EXPLAIN SELECT d.dept_name, MIN(e.salary), MAX(e.salary), AVG(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.salary), MAX(e.salary), AVG(e.salary) FROM employees e, departments d WHERE e.dept_id = d.dept_id GROUP BY d.dept_name;

EXPLAIN SELECT c.city, MIN(o.amount), MAX(o.amount), SUM(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.city;
SELECT c.city, MIN(o.amount), MAX(o.amount), SUM(o.amount) FROM orders o, customers c WHERE o.cust_id = c.cust_id GROUP BY c.city;

EXPLAIN SELECT d.dept_name, MIN(e.salary) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.salary) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id GROUP BY d.dept_name;

EXPLAIN SELECT c.city, MAX(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id GROUP BY c.city;
SELECT c.city, MAX(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id GROUP BY c.city;

EXPLAIN SELECT p.category, MAX(s.quantity) FROM sales s, products p, employees e WHERE s.prod_id = p.prod_id AND s.emp_id = e.emp_id GROUP BY p.category;
SELECT p.category, MAX(s.quantity) FROM sales s, products p, employees e WHERE s.prod_id = p.prod_id AND s.emp_id = e.emp_id GROUP BY p.category;

EXPLAIN SELECT d.location, MIN(e.salary) FROM employees e, departments d, sales s WHERE e.dept_id = d.dept_id AND e.emp_id = s.emp_id GROUP BY d.location;
SELECT d.location, MIN(e.salary) FROM employees e, departments d, sales s WHERE e.dept_id = d.dept_id AND e.emp_id = s.emp_id GROUP BY d.location;

EXPLAIN SELECT c.cust_name, MAX(o.amount) FROM orders o, customers c, products p WHERE o.cust_id = c.cust_id AND o.order_id = p.prod_id GROUP BY c.cust_name;
SELECT c.cust_name, MAX(o.amount) FROM orders o, customers c, products p WHERE o.cust_id = c.cust_id AND o.order_id = p.prod_id GROUP BY c.cust_name;

EXPLAIN SELECT p.prod_name, MIN(s.sale_date) FROM sales s, products p, employees e WHERE s.prod_id = p.prod_id AND s.emp_id = e.emp_id GROUP BY p.prod_name;
SELECT p.prod_name, MIN(s.sale_date) FROM sales s, products p, employees e WHERE s.prod_id = p.prod_id AND s.emp_id = e.emp_id GROUP BY p.prod_name;

EXPLAIN SELECT d.dept_name, MIN(e.hire_date), MAX(e.salary) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.hire_date), MAX(e.salary) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id GROUP BY d.dept_name;

EXPLAIN SELECT c.city, MIN(o.order_date), MAX(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id GROUP BY c.city;
SELECT c.city, MIN(o.order_date), MAX(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id GROUP BY c.city;

EXPLAIN SELECT p.category, MIN(p.price), MAX(s.quantity) FROM products p, sales s, employees e WHERE p.prod_id = s.prod_id AND s.emp_id = e.emp_id GROUP BY p.category;
SELECT p.category, MIN(p.price), MAX(s.quantity) FROM products p, sales s, employees e WHERE p.prod_id = s.prod_id AND s.emp_id = e.emp_id GROUP BY p.category;

EXPLAIN SELECT d.location, MAX(e.salary) FROM employees e, departments d, sales s WHERE e.dept_id = d.dept_id AND e.emp_id = s.emp_id AND e.salary > 6000 GROUP BY d.location;
SELECT d.location, MAX(e.salary) FROM employees e, departments d, sales s WHERE e.dept_id = d.dept_id AND e.emp_id = s.emp_id AND e.salary > 6000 GROUP BY d.location;

EXPLAIN SELECT c.city, MIN(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id AND o.amount > 1000 GROUP BY c.city;
SELECT c.city, MIN(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id AND o.amount > 1000 GROUP BY c.city;

EXPLAIN SELECT p.category, MAX(s.quantity) FROM sales s, products p, departments d WHERE s.prod_id = p.prod_id AND s.emp_id = d.dept_id AND s.quantity > 5 GROUP BY p.category;
SELECT p.category, MAX(s.quantity) FROM sales s, products p, departments d WHERE s.prod_id = p.prod_id AND s.emp_id = d.dept_id AND s.quantity > 5 GROUP BY p.category;

EXPLAIN SELECT d.dept_name, MIN(e.salary) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id AND d.location = 'New York' GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.salary) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id AND d.location = 'New York' GROUP BY d.dept_name;

EXPLAIN SELECT c.cust_name, MAX(o.amount) FROM orders o, customers c, products p WHERE o.cust_id = c.cust_id AND o.order_id = p.prod_id AND c.city = 'Chicago' GROUP BY c.cust_name;
SELECT c.cust_name, MAX(o.amount) FROM orders o, customers c, products p WHERE o.cust_id = c.cust_id AND o.order_id = p.prod_id AND c.city = 'Chicago' GROUP BY c.cust_name;

EXPLAIN SELECT p.prod_name, MIN(s.sale_date) FROM sales s, products p, departments d WHERE s.prod_id = p.prod_id AND s.emp_id = d.dept_id AND p.category = 'Electronics' GROUP BY p.prod_name;
SELECT p.prod_name, MIN(s.sale_date) FROM sales s, products p, departments d WHERE s.prod_id = p.prod_id AND s.emp_id = d.dept_id AND p.category = 'Electronics' GROUP BY p.prod_name;

EXPLAIN SELECT d.dept_name, COUNT(e.emp_id) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id GROUP BY d.dept_name;
SELECT d.dept_name, COUNT(e.emp_id) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id GROUP BY d.dept_name;

EXPLAIN SELECT c.city, SUM(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id GROUP BY c.city;
SELECT c.city, SUM(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id GROUP BY c.city;

EXPLAIN SELECT p.category, AVG(s.quantity) FROM sales s, products p, departments d WHERE s.prod_id = p.prod_id AND s.emp_id = d.dept_id GROUP BY p.category;
SELECT p.category, AVG(s.quantity) FROM sales s, products p, departments d WHERE s.prod_id = p.prod_id AND s.emp_id = d.dept_id GROUP BY p.category;

EXPLAIN SELECT d.dept_name, MIN(e.salary), MAX(e.salary), AVG(e.salary) FROM employees e, departments d, sales s WHERE e.dept_id = d.dept_id AND e.emp_id = s.emp_id GROUP BY d.dept_name;
SELECT d.dept_name, MIN(e.salary), MAX(e.salary), AVG(e.salary) FROM employees e, departments d, sales s WHERE e.dept_id = d.dept_id AND e.emp_id = s.emp_id GROUP BY d.dept_name;

EXPLAIN SELECT c.city, MIN(o.amount), MAX(o.amount), SUM(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id GROUP BY c.city;
SELECT c.city, MIN(o.amount), MAX(o.amount), SUM(o.amount) FROM orders o, customers c, employees e WHERE o.cust_id = c.cust_id AND o.emp_id = e.emp_id GROUP BY c.city;

EXPLAIN SELECT d.location, MIN(e.hire_date), MAX(o.order_date) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id GROUP BY d.location;
SELECT d.location, MIN(e.hire_date), MAX(o.order_date) FROM employees e, departments d, orders o WHERE e.dept_id = d.dept_id AND e.emp_id = o.emp_id GROUP BY d.location;

EXPLAIN SELECT p.category, MAX(s.sale_date), MIN(p.price) FROM products p, sales s, employees e WHERE p.prod_id = s.prod_id AND s.emp_id = e.emp_id GROUP BY p.category;
SELECT p.category, MAX(s.sale_date), MIN(p.price) FROM products p, sales s, employees e WHERE p.prod_id = s.prod_id AND s.emp_id = e.emp_id GROUP BY p.category;

EXPLAIN SELECT c.cust_name, MIN(o.order_date), MAX(o.amount) FROM customers c, orders o, employees e WHERE c.cust_id = o.cust_id AND o.emp_id = e.emp_id GROUP BY c.cust_name;
SELECT c.cust_name, MIN(o.order_date), MAX(o.amount) FROM customers c, orders o, employees e WHERE c.cust_id = o.cust_id AND o.emp_id = e.emp_id GROUP BY c.cust_name;

EXPLAIN SELECT d.dept_name, COUNT(o.order_id), MIN(e.salary) FROM departments d, employees e, orders o WHERE d.dept_id = e.dept_id AND e.emp_id = o.emp_id GROUP BY d.dept_name;
SELECT d.dept_name, COUNT(o.order_id), MIN(e.salary) FROM departments d, employees e, orders o WHERE d.dept_id = e.dept_id AND e.emp_id = o.emp_id GROUP BY d.dept_name;