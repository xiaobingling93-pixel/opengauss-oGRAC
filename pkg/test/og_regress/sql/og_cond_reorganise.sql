-- ==========================================
-- 1. Environment Setup (DDL)
-- ==========================================

-- Drop tables if they exist
DROP TABLE IF EXISTS t_opt_order;
DROP TABLE IF EXISTS t_opt_customer;

-- Create Customer Table
CREATE TABLE t_opt_customer (
    cust_id INT PRIMARY KEY,
    name VARCHAR(50),
    region VARCHAR(20),
    credit_score INT
);

-- Create Order Table
CREATE TABLE t_opt_order (
    order_id INT PRIMARY KEY,
    cust_id INT,
    amount DECIMAL(10, 2),
    order_date DATE,
    status VARCHAR(20)
);

-- Create Indexes to support optimization
-- Region index for filtering
CREATE INDEX idx_cust_region ON t_opt_customer(region);
-- Foreign key index (implicit or explicit usually good for joins)
CREATE INDEX idx_order_cust ON t_opt_order(cust_id);

-- ==========================================
-- 2. Data Preparation (DML)
-- ==========================================

INSERT INTO t_opt_customer VALUES (1, 'Alice', 'North', 750);
INSERT INTO t_opt_customer VALUES (2, 'Bob', 'South', 600);
INSERT INTO t_opt_customer VALUES (3, 'Charlie', 'North', 800);
INSERT INTO t_opt_customer VALUES (4, 'David', 'East', 550);
INSERT INTO t_opt_customer VALUES (5, 'Eve', 'West', 900);

INSERT INTO t_opt_order VALUES (101, 1, 150.00, DATE '2023-01-10', 'COMPLETED');
INSERT INTO t_opt_order VALUES (102, 1, 50.00, DATE '2023-01-15', 'PENDING');
INSERT INTO t_opt_order VALUES (103, 2, 200.00, DATE '2023-02-01', 'COMPLETED');
INSERT INTO t_opt_order VALUES (104, 3, 500.00, DATE '2023-02-10', 'SHIPPED');
INSERT INTO t_opt_order VALUES (105, 3, 120.00, DATE '2023-02-12', 'COMPLETED');
INSERT INTO t_opt_order VALUES (106, 4, 300.00, DATE '2023-03-05', 'CANCELLED');

-- ==========================================
-- 3. Condition Reorganisation Scenarios
-- ==========================================

-- Scenario 1: Reordering multiple Filters and Joins
--
-- Input Order:
-- 1. Join Condition
-- 2. Single Column Filter (on t2)
-- 3. Single Column Filter (on t1)
--
-- Expected Optimization:
-- Filters on t1 and t2 should be pushed up/evaluated before the join where possible,
-- or at least prioritized in the condition list if all are applied at the same level.

EXPLAIN SELECT t1.order_id, t2.region
FROM t_opt_order t1, t_opt_customer t2
WHERE t1.cust_id = t2.cust_id      -- Join
  AND t2.region = 'North'          -- Filter 1
  AND t1.status = 'COMPLETED';     -- Filter 2


-- Scenario 2: Mixed Types with "False" Condition (if applicable)
-- Some optimizers prioritize conditions that evaluate to FALSE literals to short-circuit.
--
-- Input Order:
-- 1. Join
-- 2. Complex Expression
-- 3. 1=0 (False)

EXPLAIN SELECT *
FROM t_opt_order t1, t_opt_customer t2
WHERE t1.cust_id = t2.cust_id
  AND t1.amount * 2 > 100
  AND 1 = 0;

