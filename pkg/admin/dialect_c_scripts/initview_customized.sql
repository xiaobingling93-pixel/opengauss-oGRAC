CREATE OR REPLACE VIEW schemata
(
   catalog_name,
   schema_name,
   schema_owner,
   default_character_set_catalog,
   default_character_set_schema,
   default_character_set_name,
   sql_path
)
AS  
SELECT 
   cast('ograc' AS varchar(64)) AS catalog_name,
   cast('SYS' AS varchar(64)) AS schema_name,
   cast('SYS' AS varchar(64)) AS schema_owner,
   cast(NULL AS varchar(288)) AS definer,
   cast(NULL AS varchar(64)) AS time_zone,
   cast(NULL AS varchar(3)) AS event_body,
   cast(NULL AS varchar(3)) AS sql_path
from SYS.SYS_DUMMY
/
                                                                
CREATE OR REPLACE PUBLIC SYNONYM  schemata         FOR SYS.schemata
/                                                               
