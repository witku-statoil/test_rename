-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4075_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4075_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4075_cdc
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
	,cast(vbvbt as [NVARCHAR](10)) as vbvbt
	,vbcrcd as vbcrcd
	,ltrim(rtrim(vbcrcd)) as vbcrcd_conv
	,cast(vbuom as [NVARCHAR](2)) as vbuom
	,cast(vbuprc as [DECIMAL](38, 4)) as vbuprc
	,cast(vbuprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vbuprc_conv
	,cast(vbeftj as [INT]) as vbeftj
	,case when cast(vbeftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vbeftj as [INT]) %1000, dateadd(year, cast(vbeftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vbeftj_conv
	,cast(vbexdj as [INT]) as vbexdj
	,case when cast(vbexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vbexdj as [INT]) %1000, dateadd(year, cast(vbexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vbexdj_conv
	,cast(vbaprs as [NVARCHAR](1)) as vbaprs
	,vbuser as vbuser
	,ltrim(rtrim(vbuser)) as vbuser_conv
	,vbpid as vbpid
	,ltrim(rtrim(vbpid)) as vbpid_conv
	,vbjobn as vbjobn
	,ltrim(rtrim(vbjobn)) as vbjobn_conv
	,cast(vbupmj as [INT]) as vbupmj
	,case when cast(vbupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vbupmj as [INT]) %1000, dateadd(year, cast(vbupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vbupmj_conv
	,cast(vbtday as [DECIMAL](38, 4)) as vbtday
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
	,vbvbt
	,vbcrcd
	,vbuom
	,vbuprc
	,vbeftj
	,vbexdj
	,vbaprs
	,vbuser
	,vbpid
	,vbjobn
	,vbupmj
	,vbtday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4075_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4075_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4075_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4075_cdc(sys_integration_id); 
