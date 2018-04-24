-- Create rep new table for cdc
IF OBJECT_ID('rep_jde.f3112_new') IS NOT NULL
    DROP TABLE rep_jde.f3112_new


CREATE TABLE rep_jde.f3112_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde.f3112
OPTION ( LABEL = 'CREATE_rep_jde.f3112_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f3112_sys_integration_id ON rep_jde.f3112_new(sys_integration_id); 
