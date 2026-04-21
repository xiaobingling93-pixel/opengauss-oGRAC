-- Create table
drop table if exists og_rowid_insert_t;
create table og_rowid_insert_t(a varchar(15), b rowid);

-- Test 0: valid rowid values should succeed.
insert into og_rowid_insert_t values('test0', 'D/////AP/AAP///H//'),('test0', 'BAAAAAAAAAAAAAAAaa');

-- Test 1: all-zero rowid should fail.
insert into og_rowid_insert_t values('test1', 'AAAAAAAAAAAAAAAAAA');

-- Test 2: invalid rowid length should fail.
insert into og_rowid_insert_t values('test2', 'BAAAAAAAAAAAAAAA');
insert into og_rowid_insert_t values('test2', 'BAAAAAAAAAAAAAAAAAAA');

-- Test 3: invalid rowid characters should fail.
insert into og_rowid_insert_t values('test3', 'BAAAAAAAAAAAAAAAa@');
insert into og_rowid_insert_t values('test3', 'BAAAAAAAAAAAAAAA A');

-- Test 4: mixed alphanumeric rowid values should succeed.
insert into og_rowid_insert_t values('test4', 'A023s3AAAAAAAAAA2s'),('test4', 'ABbBbBAC9AAAAAAA2s'),('test4', 'BhelloAHiAAHa99A2s');

-- Test 5: one valid and one invalid value should fail as a whole statement.
insert into og_rowid_insert_t values('test5', 'A023s3AAAAAAAAAA2s'), ('test5', 'BAAAAAAAAAAAAAAAa@');

-- Test 6: NULL should succeed.
insert into og_rowid_insert_t values('test6', null);

-- Test 7: base64 characters '+' and '/' should succeed.
insert into og_rowid_insert_t values('test7', 'B++///AB1AAB/+9Aa/');

-- Test 8: boundary overflow in each rowid component should fail.
insert into og_rowid_insert_t values('test8', 'EAAAAAAAAAAAAAAAAA');
insert into og_rowid_insert_t values('test8', 'AAAAAAAQAAAAAAAAAA');
insert into og_rowid_insert_t values('test8', 'AAAAAAAAAAAQAAAAAA');
insert into og_rowid_insert_t values('test8', 'AAAAAAAAAAAAAAAIAA');

-- Test 9: numeric input should be implicitly converted, then fail boundary check.
insert into og_rowid_insert_t values('test9', 12345678901234567+98765432106543211);

-- Query table contents
select * from og_rowid_insert_t;
