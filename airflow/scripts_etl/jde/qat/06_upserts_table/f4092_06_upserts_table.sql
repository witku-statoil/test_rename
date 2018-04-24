-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4092_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4092_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4092_cdc
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
	,gpgpty as gpgpty
	,ltrim(rtrim(gpgpty)) as gpgpty_conv
	,gpgpc as gpgpc
	,ltrim(rtrim(gpgpc)) as gpgpc_conv
	,gpdl01 as gpdl01
	,ltrim(rtrim(gpdl01)) as gpdl01_conv
	,gpgpk1 as gpgpk1
	,ltrim(rtrim(gpgpk1)) as gpgpk1_conv
	,gpgpk2 as gpgpk2
	,ltrim(rtrim(gpgpk2)) as gpgpk2_conv
	,gpgpk3 as gpgpk3
	,ltrim(rtrim(gpgpk3)) as gpgpk3_conv
	,gpgpk4 as gpgpk4
	,ltrim(rtrim(gpgpk4)) as gpgpk4_conv
	,gpgpk5 as gpgpk5
	,ltrim(rtrim(gpgpk5)) as gpgpk5_conv
	,gpgpk6 as gpgpk6
	,ltrim(rtrim(gpgpk6)) as gpgpk6_conv
	,gpgpk7 as gpgpk7
	,ltrim(rtrim(gpgpk7)) as gpgpk7_conv
	,gpgpk8 as gpgpk8
	,ltrim(rtrim(gpgpk8)) as gpgpk8_conv
	,gpgpk9 as gpgpk9
	,ltrim(rtrim(gpgpk9)) as gpgpk9_conv
	,gpgpk10 as gpgpk10
	,ltrim(rtrim(gpgpk10)) as gpgpk10_conv
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
	,gpgpty
	,gpgpc
	,gpdl01
	,gpgpk1
	,gpgpk2
	,gpgpk3
	,gpgpk4
	,gpgpk5
	,gpgpk6
	,gpgpk7
	,gpgpk8
	,gpgpk9
	,gpgpk10,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4092_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4092_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4092_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4092_cdc(sys_integration_id); 
