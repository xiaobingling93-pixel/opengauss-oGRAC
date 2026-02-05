-- test the result set after union of null
drop table if exists t_union_1;
create table t_union_1(id int, c_inter interval day(7) to second(5));
insert into t_union_1 values (1, '1231 12:3:4.1234'),(2, 'P1231DT16H3.333S'),(3,'PT12H'),
						   (4,'-P99DT655M999.99S'),(5,'-1234 0:0:0.0004'),(5,'-1234 0:0:0.0004'),
						   (6,'-1234 0:0:0.0004'),(7,'-1234 0:0:0.0004');
SELECT c_inter, NULL FROM t_union_1 UNION SELECT NULL, c_inter FROM t_union_1 ORDER BY 1,2;
DESC -Q SELECT c_inter, NULL FROM t_union_1 UNION SELECT NULL, c_inter FROM t_union_1;
drop table if exists t_union_1;
-- test the result datatype of union
drop table if exists t_data_union_1;
drop table if exists t_data_union_2;
create table t_data_union_1 (c_int int, c_char char(10), c_varchar varchar(20), c_lob clob, c_raw raw(30), c_binary binary(10), c_varbinary varbinary(11), c_bool bool, c_number number(12), c_number2 number2(13), c_decimal decimal(10,3), c_date date, c_timestamp timestamp(6));
create table t_data_union_2 (c_int int, c_char char(10), c_varchar varchar(20), c_lob clob, c_raw raw(30), c_binary binary(10), c_varbinary varbinary(11), c_bool bool, c_number number(12), c_number2 number2(13), c_decimal decimal(10,3), c_date date, c_timestamp timestamp(6));
desc -q select c_char from t_data_union_1 union select c_varchar from t_data_union_2;
desc -q select c_char from t_data_union_1 union select c_binary from t_data_union_2;
desc -q select c_char from t_data_union_1 union select c_lob from t_data_union_2;
desc -q select c_char from t_data_union_1 union select c_raw from t_data_union_2;
desc -q select c_char from t_data_union_1 union select c_varbinary from t_data_union_2;
desc -q select c_char from t_data_union_1 union select c_bool from t_data_union_2;
desc -q select c_char from t_data_union_1 union select c_date from t_data_union_2;

desc -q select c_varchar from t_data_union_1 union select c_char from t_data_union_2;
desc -q select c_varchar from t_data_union_1 union select c_lob from t_data_union_2;
desc -q select c_varchar from t_data_union_1 union select c_raw from t_data_union_2;
desc -q select c_varchar from t_data_union_1 union select c_binary from t_data_union_2;
desc -q select c_varchar from t_data_union_1 union select c_varbinary from t_data_union_2;
desc -q select c_varchar from t_data_union_1 union select c_date from t_data_union_2;
desc -q select c_varchar from t_data_union_1 union select c_bool from t_data_union_2;

desc -q select c_int from t_data_union_1 union select c_char from t_data_union_2;
desc -q select c_int from t_data_union_1 union select c_raw from t_data_union_2;
desc -q select c_int from t_data_union_1 union select c_lob from t_data_union_2;
desc -q select c_int from t_data_union_1 union select c_bool from t_data_union_2;
desc -q select c_int from t_data_union_1 union select c_number from t_data_union_2;
desc -q select c_int from t_data_union_1 union select c_decimal from t_data_union_2;

desc -q select c_number from t_data_union_1 union select c_number2 from t_data_union_2;
desc -q select c_number from t_data_union_1 union select c_decimal from t_data_union_2;
desc -q select c_number2 from t_data_union_1 union select c_decimal from t_data_union_2;

desc -q select c_date from t_data_union_1 union select c_timestamp from t_data_union_2;

desc -q select NULL from t_data_union_1 union select c_binary from t_data_union_2;
desc -q select NULL from t_data_union_1 union select c_timestamp from t_data_union_2;
desc -q select NULL from t_data_union_1 union select c_varchar from t_data_union_2;
desc -q select c_bool from t_data_union_1 union select NULL from t_data_union_2;
desc -q select c_number from t_data_union_1 union select NULL from t_data_union_2;
desc -q select c_decimal from t_data_union_1 union select NULL from t_data_union_2;

drop table if exists t_data_union_1;
drop table if exists t_data_union_2;