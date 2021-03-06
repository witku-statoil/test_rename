-- Create stg table for cdc
IF OBJECT_ID('stg_jde.tmp_f554901_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_f554901_cdc

    
CREATE TABLE stg_jde.tmp_f554901_cdc
WITH
(
    DISTRIBUTION = HASH(sys_integration_id)
    ,HEAP
)
AS
SELECT * FROM stg_jde.ext_f554901_cdc
OPTION ( LABEL = 'CREATE_stg_jde.tmp_f554901_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_f554901_cdc_sys_integration_id ON stg_jde.tmp_f554901_cdc(sys_integration_id); 
CREATE STATISTICS stat_tmp_f554901_cdc_sys_cdc_scn ON stg_jde.tmp_f554901_cdc(sys_cdc_scn); 
CREATE STATISTICS stat_tmp_f554901_cdc_sys_cdc_operation_type ON stg_jde.tmp_f554901_cdc(sys_cdc_operation_type); 
CREATE STATISTICS stat_tmp_f554901_cdc_sys_cdc_before_after ON stg_jde.tmp_f554901_cdc(sys_cdc_before_after); 

-- INSERT TO HIST TABLE?
