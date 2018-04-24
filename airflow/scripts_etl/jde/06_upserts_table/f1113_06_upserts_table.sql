-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f1113_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f1113_cdc


CREATE TABLE stg_jde.tmp_upsert_f1113_cdc
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
	,cast(c1rtty as [NVARCHAR](2)) as c1rtty
	,c1crdc as c1crdc
	,ltrim(rtrim(c1crdc)) as c1crdc_conv
	,c1crcd as c1crcd
	,ltrim(rtrim(c1crcd)) as c1crcd_conv
	,cast(c1eft as [INT]) as c1eft
	,case when cast(c1eft as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(c1eft as [INT]) %1000, dateadd(year, cast(c1eft as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as c1eft_conv
	,cast(c1crr as [DECIMAL](38, 4)) as c1crr
	,cast(c1crrd as [DECIMAL](38, 4)) as c1crrd
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
	,c1rtty
	,c1crdc
	,c1crcd
	,c1eft
	,c1crr
	,c1crrd,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f1113_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f1113_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f1113_cdc_sys_integration_id ON stg_jde.tmp_upsert_f1113_cdc(sys_integration_id); 
