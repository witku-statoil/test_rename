-- Create deletes table from stg table
IF OBJECT_ID('stg_jde.tmp_delete_f0101_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_delete_f0101_cdc
    
CREATE TABLE stg_jde.tmp_delete_f0101_cdc
WITH 
(
    DISTRIBUTION = HASH(sys_integration_id) 
    ,HEAP
) AS 
SELECT 
    sys_integration_id
    ,MAX(sys_cdc_scn) as max_scn
FROM stg_jde.tmp_f0101_cdc
WHERE 1=1 
    AND (  (sys_cdc_operation_type IN ( 'DELETE','TRUNCATE') ) 
        OR (sys_cdc_operation_type IN ('SQL CO','SQL COMPUPDATE') AND sys_cdc_before_after='BEFORE' ) 
        ) 
GROUP BY 
    sys_integration_id
OPTION ( LABEL = 'CREATE_stg_jde.tmp_delete_f0101_cdc AF:{{ task_instance_key_str }}' ) 
    
CREATE STATISTICS stat_tmp_delete_f0101_cdc_sys_integration_id ON stg_jde.tmp_delete_f0101_cdc(sys_integration_id); 