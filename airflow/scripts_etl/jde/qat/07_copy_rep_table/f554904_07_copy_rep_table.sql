-- Create rep new table for cdc
IF OBJECT_ID('rep_jde_qat.f554904_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f554904_new


CREATE TABLE rep_jde_qat.f554904_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde_qat.f554904
OPTION ( LABEL = 'CREATE_rep_jde_qat.f554904_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554904_sys_integration_id ON rep_jde_qat.f554904_new(sys_integration_id); 
