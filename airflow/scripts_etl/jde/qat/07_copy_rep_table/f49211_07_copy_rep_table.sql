-- Create rep new table for cdc
IF OBJECT_ID('rep_jde_qat.f49211_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f49211_new


CREATE TABLE rep_jde_qat.f49211_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    *
FROM 
    rep_jde_qat.f49211
OPTION ( LABEL = 'CREATE_rep_jde_qat.f49211_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f49211_sys_integration_id ON rep_jde_qat.f49211_new(sys_integration_id); 
