-- Create rep new table for cdc
IF OBJECT_ID('rep_jde.f9210_new') IS NOT NULL
    DROP TABLE rep_jde.f9210_new


CREATE TABLE rep_jde.f9210_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde.f9210
OPTION ( LABEL = 'CREATE_rep_jde.f9210_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f9210_sys_integration_id ON rep_jde.f9210_new(sys_integration_id); 
