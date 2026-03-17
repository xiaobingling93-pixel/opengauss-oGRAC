-- foreign key constraint self
drop table if exists t_emp;
create table t_emp (empno number(4), mgr number(4), ename varchar2(10));
alter table t_emp add constraint t_emp_pk primary key(empno);
alter table t_emp add constraint t_emp_fk foreign key(mgr) references t_emp(empno);
insert into t_emp values (0,null,'root');
insert into t_emp (empno,mgr) values (1,0);
insert into t_emp (empno,mgr) values (2,1);
insert into t_emp (empno,mgr) values (3,2);
commit;
-- insert error
insert into t_emp (empno,mgr) values (10,23);
insert into t_emp (empno,mgr) values (5,4);
-- update
update t_emp set mgr = 11 where empno = 3;   -- error
update t_emp set mgr = null where empno = 3; -- success
update t_emp set mgr = 3 where empno = 0;    -- success
commit;
insert into  t_emp values (-1,null,'oGRAC'); -- success
commit;
delete from t_emp where empno = 1; -- error
delete from t_emp where empno = -1; -- success
commit;
drop table t_emp;

-- drop referenced primary key should fail
drop table if exists emp;
drop table if exists dept;
create table dept(deptno number(2), dname varchar2(20), constraint deptno_id primary key(deptno));
create table emp(empno number(4) primary key, ename varchar2(20), deptno number(2));
alter table emp add constraint emp_deptno_fk foreign key(deptno) references dept(deptno);

insert into dept values(10, 'Test');
insert into dept values(20, 'Develop');
insert into dept values(30, 'HR');
insert into dept values(40, 'Sales');
insert into dept values(50, 'Finance');

insert into emp values(0001, 'Nancy', 10);
insert into emp values(0002, 'Tom', 10);
insert into emp values(0003, 'Anne', 20);
insert into emp values(0004, 'Alice', 30);
insert into emp values(0005, 'Gaby', 40);
insert into emp values(0006, 'Lynette', 30);

alter table dept drop constraint deptno_id;

drop table emp;
drop table dept;
