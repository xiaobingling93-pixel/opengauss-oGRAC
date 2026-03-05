-- sequence restart gate case
drop sequence if exists seq17;
create sequence seq17 start with 10 increment by 2 minvalue 1 maxvalue 100 cache 20;

select seq17.nextval from dual;
select seq17.nextval from dual;

alter sequence seq17 restart;
select seq17.nextval from dual;

alter sequence seq17 restart start with 50;
select seq17.nextval from dual;

drop sequence if exists seq17;

drop sequence if exists seq17_desc;
create sequence seq17_desc increment by -1 minvalue -10 maxvalue -1 start with -3 cache 20;

select seq17_desc.nextval from dual;
alter sequence seq17_desc restart;
select seq17_desc.nextval from dual;

drop sequence if exists seq17_desc;
