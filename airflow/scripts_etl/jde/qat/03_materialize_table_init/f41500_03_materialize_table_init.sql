-- Create stg table for init
IF OBJECT_ID('stg_jde_qat.tmp_f41500_init') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_f41500_init


CREATE TABLE stg_jde_qat.tmp_f41500_init
WITH
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id)
)
AS
SELECT * FROM stg_jde_qat.ext_f41500_init
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_f41500_init AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_f41500_init_sys_integration_id ON stg_jde_qat.tmp_f41500_init(sys_integration_id); 
