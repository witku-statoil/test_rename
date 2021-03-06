-- Create rep new table for cdc
IF OBJECT_ID('rep_jde_qat.f40540_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f40540_new


CREATE TABLE rep_jde_qat.f40540_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde_qat.f40540
OPTION ( LABEL = 'CREATE_rep_jde_qat.f40540_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f40540_sys_integration_id ON rep_jde_qat.f40540_new(sys_integration_id); 
