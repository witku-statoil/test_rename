-- Create rep new table for cdc
IF OBJECT_ID('rep_jde_qat.f4960_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4960_new


CREATE TABLE rep_jde_qat.f4960_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde_qat.f4960
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4960_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4960_sys_integration_id ON rep_jde_qat.f4960_new(sys_integration_id); 
