-- Create stg table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_f4104_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_f4104_cdc

    
CREATE TABLE stg_jde_qat.tmp_f4104_cdc
WITH
(
    DISTRIBUTION = HASH(sys_integration_id)
    ,HEAP
)
AS
SELECT * FROM stg_jde_qat.ext_f4104_cdc
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_f4104_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_f4104_cdc_sys_integration_id ON stg_jde_qat.tmp_f4104_cdc(sys_integration_id); 
CREATE STATISTICS stat_tmp_f4104_cdc_sys_cdc_scn ON stg_jde_qat.tmp_f4104_cdc(sys_cdc_scn); 
CREATE STATISTICS stat_tmp_f4104_cdc_sys_cdc_operation_type ON stg_jde_qat.tmp_f4104_cdc(sys_cdc_operation_type); 
CREATE STATISTICS stat_tmp_f4104_cdc_sys_cdc_before_after ON stg_jde_qat.tmp_f4104_cdc(sys_cdc_before_after); 

-- INSERT TO HIST TABLE?
