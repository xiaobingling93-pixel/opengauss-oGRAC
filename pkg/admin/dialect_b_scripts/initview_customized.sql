CREATE OR REPLACE VIEW EVENTS
(
   event_catalog,
   event_schema,
   event_name,
   definer,
   time_zone,
   event_body,
   event_definition,
   event_type,
   execute_at,
   interval_value,
   interval_field,
   sql_mode,
   starts,
   ends,
   status,
   on_completion,
   created,
   last_altered,
   last_executed,
   event_comment,
   originator,
   character_set_client,
   collation_connection,
   database_collation
)
AS  
SELECT 
   cast(NULL AS varchar(64)) AS event_catalog,
   cast(NULL AS varchar(64)) AS event_schema,
   cast(NULL AS varchar(64)) AS event_name,
   cast(NULL AS varchar(288)) AS definer,
   cast(NULL AS varchar(64)) AS time_zone,
   cast(NULL AS varchar(3)) AS event_body,
   cast(NULL AS varchar(256)) AS event_definition,
   cast(NULL AS varchar(256)) AS event_type,
   cast(NULL AS timestamp) AS execute_at,
   cast(NULL AS varchar(256)) AS interval_value,
   cast(NULL AS varchar(256)) AS interval_field,
   cast(NULL AS varchar(256)) AS sql_mode,
   cast(NULL AS timestamp) AS starts,
   cast(NULL AS timestamp) AS ends,
   cast(NULL AS varchar(21)) AS status,
   cast(NULL AS timestamp) AS on_completion,
   cast(NULL AS timestamp) AS created,
   cast(NULL AS timestamp) AS last_altered,
   cast(NULL AS timestamp) AS last_executed,
   cast(NULL AS varchar(10)) AS event_comment,
   cast(NULL AS int) AS originator,
   cast(NULL AS varchar(64)) AS character_set_client,
   cast(NULL AS varchar(64)) AS collation_connection,
   cast(NULL AS varchar(64)) AS database_collation
from SYS.SYS_DUMMY where 1 = 0
/
                                                                
CREATE OR REPLACE PUBLIC SYNONYM  EVENTS         FOR SYS.EVENTS
/                                                               
