create or replace type pl_nt_t1 is table of int;
/

set serveroutput on;

declare
x pl_nt_t1 := pl_nt_t1(6,158, 59,2222,59,0,0, 33, 22, 33, 105,1000,120,158,158, 105,33, 33,209,999,258,258,10,1024,1024,33, 59, 6, 102, 102, 6, 206, 33, 102, 102, 6, 22,1040,26,65,65,77,8,9,1120,15,0,3,4,5,6,7,8,9,10,11,12,13,14,15,null,17,18,133, 59, 6, 102,102,6,20);
begin
dbe_output.print_line(x.count);
x.delete(80,102);
dbe_output.print_line(x.count);
end;
/

drop type pl_nt_t1;

