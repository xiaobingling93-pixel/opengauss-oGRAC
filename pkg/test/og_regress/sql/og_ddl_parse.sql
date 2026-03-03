-- the default col and check can not using user define func
CREATE OR REPLACE FUNCTION func1(c1 int) return int is
begin
return 123;
end;
/
-- DEFAULT using user define func
drop table if exists users_status;
CREATE TABLE users_status (id int, status int DEFAULT func1(1)); -- error
-- CHECK using user define func
drop table if exists users_name;
CREATE TABLE users_name (
    id int, age int,
    CONSTRAINT age_check CHECK (age < func1(1))); -- error
CREATE TABLE users_name (
    id int, age int, birth int,
    CONSTRAINT age_check CHECK (birth < 100 and age < func1(1))); -- error

-- alter default 
CREATE TABLE users_status (id int, status int);
alter table users_status modify (status int default  func1(1)); -- error
alter table users_status add CONSTRAINT check_status check (id < func1(1)); -- error
alter table users_status add CONSTRAINT check_status check (status > 100 and id < func1(1)); -- error
DROP TABLE users_status;
DROP FUNCTION func1;

DROP TABLE IF EXISTS T_RG_HS_01;
CREATE TABLE T_RG_HS_01 (
                            id INT, c_num NUMBER, c_vchar VARCHAR(100), c_char CHAR(20), c_clob CLOB,c_blob BLOB, c_date DATE,c_ts TIMESTAMP(6)
)PARTITION BY RANGE (c_date)

SUBPARTITION BY HASH (id) (
    PARTITION p1 VALUES LESS THAN (TO_DATE('2021-01-31', 'yyyy-mm-dd')) (
        SUBPARTITION sp1_1
    )
);

EXPLAIN
SELECT *
FROM SYS.DB_TAB_PARTITIONS X
         INNER JOIN (SELECT Y.PARENTPART_NAME,
                            GROUP_CONCAT(Y.SUB_PARTITION_STRING ORDER BY Y.PARTITION_POSITION ASC SEPARATOR ',') AS SUB_PARTITION_VALUE
                     FROM (SELECT O.PARENTPART_NAME,
                                  O.PARTITION_POSITION,
                                  CONCAT(O.PARTITION_NAME, O.TABLESPACE_NAME) AS SUB_PARTITION_STRING
                           FROM SYS.DB_TAB_SUBPARTITIONS O
                                    INNER JOIN SYS.DB_PART_TABLES P ON O.TABLE_NAME = P.TABLE_NAME
                           WHERE P.TABLE_NAME = 'T_RG_HS_01') Y
                     GROUP BY Y.PARENTPART_NAME) Z
                    ON X.PARTITION_NAME = Z.PARENTPART_NAME;

DROP TABLE IF EXISTS T_RG_HS_01;
