-- Create stg table for cdc
IF OBJECT_ID('stg_jde.tmp_f4101_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_f4101_cdc

    
CREATE TABLE stg_jde.tmp_f4101_cdc
WITH
(
    DISTRIBUTION = HASH(sys_integration_id)
    ,HEAP
)
AS
SELECT * FROM stg_jde.ext_f4101_cdc
OPTION ( LABEL = 'CREATE_stg_jde.tmp_f4101_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_f4101_cdc_sys_integration_id ON stg_jde.tmp_f4101_cdc(sys_integration_id); 
CREATE STATISTICS stat_tmp_f4101_cdc_sys_cdc_scn ON stg_jde.tmp_f4101_cdc(sys_cdc_scn); 
CREATE STATISTICS stat_tmp_f4101_cdc_sys_cdc_operation_type ON stg_jde.tmp_f4101_cdc(sys_cdc_operation_type); 
CREATE STATISTICS stat_tmp_f4101_cdc_sys_cdc_before_after ON stg_jde.tmp_f4101_cdc(sys_cdc_before_after); 

-- INSERT TO HIST TABLE?
