-- Create stg table for init
IF OBJECT_ID('stg_jde.tmp_f554103_init') IS NOT NULL
    DROP TABLE stg_jde.tmp_f554103_init


CREATE TABLE stg_jde.tmp_f554103_init
WITH
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id)
)
AS
SELECT * FROM stg_jde.ext_f554103_init
OPTION ( LABEL = 'CREATE_stg_jde.tmp_f554103_init AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_f554103_init_sys_integration_id ON stg_jde.tmp_f554103_init(sys_integration_id); 
