-- Create rep new table for cdc
IF OBJECT_ID('rep_jde.f3411_new') IS NOT NULL
    DROP TABLE rep_jde.f3411_new


CREATE TABLE rep_jde.f3411_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde.f3411
OPTION ( LABEL = 'CREATE_rep_jde.f3411_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f3411_sys_integration_id ON rep_jde.f3411_new(sys_integration_id); 
