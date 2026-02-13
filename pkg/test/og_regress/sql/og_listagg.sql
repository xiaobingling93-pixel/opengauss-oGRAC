-- Clean up previous test table
DROP TABLE IF EXISTS t_listagg_check;

-- Create test table
CREATE TABLE t_listagg_check (
                                 id INT,
                                 group_col INT,
                                 val VARCHAR(10),
                                 sort_num INT
);

-- Insert data designed to expose string vs numeric sort differences
-- Numeric order: 1, 2, 10
-- String order:  1, 10, 2
INSERT INTO t_listagg_check VALUES (1, 100, 'One', 1);
INSERT INTO t_listagg_check VALUES (2, 100, 'Two', 2);
INSERT INTO t_listagg_check VALUES (3, 100, 'Ten', 10);

-- Second group for verification
INSERT INTO t_listagg_check VALUES (4, 200, 'Five', 5);
INSERT INTO t_listagg_check VALUES (5, 200, 'Fifty', 50);
INSERT INTO t_listagg_check VALUES (6, 200, 'Six', 6);

-- Test 1: Sort by Numeric Column
-- Expected Result for group 100: "One,Two,Ten" (Ordered by 1, 2, 10)
-- If bug exists (sorted as string): "One,Ten,Two" (Ordered by '1', '10', '2')
SELECT group_col,
       LISTAGG(val, ',') WITHIN GROUP (ORDER BY sort_num) as list_asc
FROM t_listagg_check
GROUP BY group_col;
--
-- -- Test 2: Sort by Numeric Column DESC
-- -- Expected Result for group 100: "Ten,Two,One" (Ordered by 10, 2, 1)
SELECT group_col,
       LISTAGG(val, ',') WITHIN GROUP (ORDER BY sort_num DESC) as list_desc
FROM t_listagg_check
GROUP BY group_col;

-- Test 3: Sort by Expression (Force type inference logic)
-- Verifies that expressions also have their types correctly inferred
SELECT group_col,
       LISTAGG(val, ',') WITHIN GROUP (ORDER BY sort_num + 0) as list_expr
FROM t_listagg_check
GROUP BY group_col;

-- Cleanup
DROP TABLE t_listagg_check;

-- 模拟 “字符串排序” 的异常场景（用于对比）
-- 创建字符串类型排序列的测试表
CREATE TABLE t_listagg_bug (
                               group_col NUMBER,
                               val VARCHAR2(10),
                               sort_str VARCHAR2(10)  -- 字符串类型的“数字”
);

-- 插入数据
INSERT INTO t_listagg_bug (group_col, val, sort_str) VALUES (100, 'One', '1');
INSERT INTO t_listagg_bug (group_col, val, sort_str) VALUES (100, 'Two', '2');
INSERT INTO t_listagg_bug (group_col, val, sort_str) VALUES (100, 'Ten', '10');

-- 按字符串排序（触发 bug）
SELECT group_col,
       LISTAGG(val, ',') WITHIN GROUP (ORDER BY sort_str) as list_bug
FROM t_listagg_bug
GROUP BY group_col;

DROP TABLE t_listagg_bug;