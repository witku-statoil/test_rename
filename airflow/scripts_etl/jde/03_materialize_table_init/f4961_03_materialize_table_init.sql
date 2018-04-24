-- Create stg table for init
IF OBJECT_ID('stg_jde.tmp_f4961_init') IS NOT NULL
    DROP TABLE stg_jde.tmp_f4961_init


CREATE TABLE stg_jde.tmp_f4961_init
WITH
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id)
)
AS
SELECT * FROM stg_jde.ext_f4961_init
OPTION ( LABEL = 'CREATE_stg_jde.tmp_f4961_init AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_f4961_init_sys_integration_id ON stg_jde.tmp_f4961_init(sys_integration_id); 
