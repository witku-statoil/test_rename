-- Create stg table for init
IF OBJECT_ID('stg_jde_qat.tmp_f554904_init') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_f554904_init


CREATE TABLE stg_jde_qat.tmp_f554904_init
WITH
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id)
)
AS
SELECT * FROM stg_jde_qat.ext_f554904_init
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_f554904_init AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_f554904_init_sys_integration_id ON stg_jde_qat.tmp_f554904_init(sys_integration_id); 
