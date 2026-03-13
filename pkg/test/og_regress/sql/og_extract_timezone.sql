select extract(timezone_hour from to_timestamp('2023-05-19 10:20:30 +8:00')) from dual;
select extract(timezone_minute from to_timestamp('2023-05-19 10:20:30 +8:00')) from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00 +00:00')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00 +00:00')) as tzm
from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00 -00:00')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00 -00:00')) as tzm
from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00 -14:00')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00 -14:00')) as tzm
from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00 +12:00')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00 +12:00')) as tzm
from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00 +5:45')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00 +5:45')) as tzm
from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00.001 +08:30')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00.001 +08:30')) as tzm
from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00.001 +08')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00.001 +08')) as tzm
from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00 +08')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00 +08')) as tzm
from dual;

select
  extract(timezone_hour   from to_timestamp('2026-02-26 00:00:00 +08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM')) as tzh,
  extract(timezone_minute from to_timestamp('2026-02-26 00:00:00 +08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM')) as tzm
from dual;

select extract(-1) from dual;
select extract(-1 from timestamp '2023-06-15 14:30:00') from dual;

