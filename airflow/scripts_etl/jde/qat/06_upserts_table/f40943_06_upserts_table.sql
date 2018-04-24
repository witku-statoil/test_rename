-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f40943_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f40943_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f40943_cdc
WITH 
(
    DISTRIBUTION = HASH(sys_integration_id) 
    ,HEAP
) AS 
SELECT
    	cast(sys_file_name as [NVARCHAR](100)) as sys_file_name
	,cast(sys_file_ln as [INT]) as sys_file_ln
	,cast(sys_integration_id as [NVARCHAR](200)) as sys_integration_id
	,cast(sys_extract_dt as [NVARCHAR](40)) as sys_extract_dt
	,cast(sys_cdc_dt as [NVARCHAR](40)) as sys_cdc_dt
	,cast(sys_cdc_scn as [NVARCHAR](14)) as sys_cdc_scn
	,cast(sys_cdc_operation_type as [NVARCHAR](15)) as sys_cdc_operation_type
	,cast(sys_cdc_before_after as [NVARCHAR](10)) as sys_cdc_before_after
	,cast(sys_line_modified_ind as [INT]) as sys_line_modified_ind
	,cast(oksdgr as [NVARCHAR](8)) as oksdgr
	,oksdv1 as oksdv1
	,ltrim(rtrim(oksdv1)) as oksdv1_conv
	,oksdv2 as oksdv2
	,ltrim(rtrim(oksdv2)) as oksdv2_conv
	,oksdv3 as oksdv3
	,ltrim(rtrim(oksdv3)) as oksdv3_conv
	,oksdv4 as oksdv4
	,ltrim(rtrim(oksdv4)) as oksdv4_conv
	,oksdv5 as oksdv5
	,ltrim(rtrim(oksdv5)) as oksdv5_conv
	,oksdv6 as oksdv6
	,ltrim(rtrim(oksdv6)) as oksdv6_conv
	,oksdv7 as oksdv7
	,ltrim(rtrim(oksdv7)) as oksdv7_conv
	,oksdv8 as oksdv8
	,ltrim(rtrim(oksdv8)) as oksdv8_conv
	,cast(okogid as [DECIMAL](38, 4)) as okogid
	,cast(okogid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as okogid_conv
FROM (
    SELECT 
        	sys_file_name
	,sys_file_ln
	,sys_integration_id
	,sys_extract_dt
	,sys_cdc_dt
	,sys_cdc_scn
	,sys_cdc_operation_type
	,sys_cdc_before_after
	,sys_line_modified_ind
	,oksdgr
	,oksdv1
	,oksdv2
	,oksdv3
	,oksdv4
	,oksdv5
	,oksdv6
	,oksdv7
	,oksdv8
	,okogid,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f40943_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f40943_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f40943_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f40943_cdc(sys_integration_id); 
