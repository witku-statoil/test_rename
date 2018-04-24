-- Create rep new table for cdc
IF OBJECT_ID('rep_jde.f554231h_new') IS NOT NULL
    DROP TABLE rep_jde.f554231h_new


CREATE TABLE rep_jde.f554231h_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde.f554231h
OPTION ( LABEL = 'CREATE_rep_jde.f554231h_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554231h_sys_integration_id ON rep_jde.f554231h_new(sys_integration_id); 
