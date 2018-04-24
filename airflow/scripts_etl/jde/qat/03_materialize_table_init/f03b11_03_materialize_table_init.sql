-- Create stg table for init
IF OBJECT_ID('stg_jde_qat.tmp_f03b11_init') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_f03b11_init


CREATE TABLE stg_jde_qat.tmp_f03b11_init
WITH
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id)
)
AS
SELECT * FROM stg_jde_qat.ext_f03b11_init
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_f03b11_init AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_f03b11_init_sys_integration_id ON stg_jde_qat.tmp_f03b11_init(sys_integration_id); 
