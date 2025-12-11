-- =========================
-- 1 环境准备
-- =========================

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1(
                   id INT,
                   a  INT,
                   b  INT
);

CREATE TABLE t2(
                   id INT,
                   a  INT
);

INSERT INTO t1 VALUES
                   (1,1,10),
                   (2,1,10),
                   (3,2,20),
                   (4,2,20),
                   (5,3,30);

INSERT INTO t2 VALUES
                   (1,1),
                   (2,2),
                   (3,3);


-- =========================
-- 2 DISTINCT 可消除场景
-- =========================

-- Case1 基本场景
EXPLAIN
SELECT DISTINCT a
FROM (
         SELECT DISTINCT a
         FROM t1
     ) s
ORDER BY a;

SELECT DISTINCT a
FROM (
         SELECT DISTINCT a
         FROM t1
     ) s
ORDER BY a;


-- Case2 多列 DISTINCT
EXPLAIN
SELECT DISTINCT a,b
FROM (
         SELECT DISTINCT a,b
         FROM t1
     ) s
ORDER BY a,b;

SELECT DISTINCT a,b
FROM (
         SELECT DISTINCT a,b
         FROM t1
     ) s
ORDER BY a,b;


-- Case3 父查询含常量表达式
EXPLAIN
SELECT DISTINCT 1,a
FROM (
         SELECT DISTINCT a
         FROM t1
     ) s
ORDER BY a;

SELECT DISTINCT 1,a
FROM (
         SELECT DISTINCT a
         FROM t1
     ) s
ORDER BY a;


-- Case4 父查询常量表达式
EXPLAIN
SELECT DISTINCT a+0
FROM (
         SELECT DISTINCT a
         FROM t1
     ) s
ORDER BY a;

SELECT DISTINCT a+0
FROM (
         SELECT DISTINCT a
         FROM t1
     ) s
ORDER BY a;


-- =========================
-- 3 SELECT_AS_LIST 场景
-- =========================

-- Case5 IN 子查询
EXPLAIN
SELECT *
FROM t2
WHERE a IN (
    SELECT DISTINCT a
    FROM t1
);

SELECT *
FROM t2
WHERE a IN (
    SELECT DISTINCT a
    FROM t1
);


-- Case6 EXISTS 子查询
EXPLAIN
SELECT *
FROM t2 t
WHERE EXISTS (
    SELECT DISTINCT a
    FROM t1
    WHERE t.a = t1.a
);

SELECT *
FROM t2 t
WHERE EXISTS (
    SELECT DISTINCT a
    FROM t1
    WHERE t.a = t1.a
);


-- =========================
-- 4 DISTINCT 不允许消除
-- =========================

-- Case7 子查询含 GROUP BY
EXPLAIN
SELECT DISTINCT a
FROM (
         SELECT DISTINCT a
         FROM t1
         GROUP BY a
     ) s
ORDER BY a;

SELECT DISTINCT a
FROM (
         SELECT DISTINCT a
         FROM t1
         GROUP BY a
     ) s
ORDER BY a;


-- Case8 子查询含 LIMIT
EXPLAIN
SELECT DISTINCT a
FROM (
         SELECT DISTINCT a
         FROM t1
                  LIMIT 2
     ) s
ORDER BY a;

SELECT DISTINCT a
FROM (
         SELECT DISTINCT a
         FROM t1
                  LIMIT 2
     ) s
ORDER BY a;


-- Case9 父查询表达式不是列
EXPLAIN
SELECT DISTINCT a+1
FROM (
         SELECT DISTINCT a
         FROM t1
     ) s
ORDER BY a;

SELECT DISTINCT a+1
FROM (
         SELECT DISTINCT a
         FROM t1
     ) s
ORDER BY a;


-- Case10 子查询 JOIN
EXPLAIN
SELECT DISTINCT s.a
FROM (
         SELECT DISTINCT t1.a
         FROM t1
                  JOIN t2 ON t1.a=t2.a
     ) s
ORDER BY s.a;

SELECT DISTINCT s.a
FROM (
         SELECT DISTINCT t1.a
         FROM t1
                  JOIN t2 ON t1.a=t2.a
     ) s
ORDER BY s.a;