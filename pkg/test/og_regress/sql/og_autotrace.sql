DROP TABLE IF EXISTS autotrace_test;
CREATE TABLE autotrace_test(c1 int);
INSERT INTO autotrace_test VALUES(1);
COMMIT;

SET AUTOTRACE ON;
select * from autotrace_test;
SET AUTOTRACE TRACEONLY;
select * from autotrace_test;
SET AUTOTRACE OFF;
select * from autotrace_test;

DROP TABLE IF EXISTS autotrace_test;