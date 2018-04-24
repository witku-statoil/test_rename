-- Create rep new table for cdc
IF OBJECT_ID('rep_jde_qat.f03012_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f03012_new


CREATE TABLE rep_jde_qat.f03012_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde_qat.f03012
OPTION ( LABEL = 'CREATE_rep_jde_qat.f03012_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f03012_sys_integration_id ON rep_jde_qat.f03012_new(sys_integration_id); 
