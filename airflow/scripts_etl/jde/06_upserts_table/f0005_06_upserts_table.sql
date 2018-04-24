-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0005_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0005_cdc


CREATE TABLE stg_jde.tmp_upsert_f0005_cdc
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
	,cast(drsy as [NVARCHAR](4)) as drsy
	,drrt as drrt
	,ltrim(rtrim(drrt)) as drrt_conv
	,drky as drky
	,ltrim(rtrim(drky)) as drky_conv
	,drdl01 as drdl01
	,ltrim(rtrim(drdl01)) as drdl01_conv
	,drdl02 as drdl02
	,ltrim(rtrim(drdl02)) as drdl02_conv
	,drsphd as drsphd
	,ltrim(rtrim(drsphd)) as drsphd_conv
	,cast(drudco as [NVARCHAR](1)) as drudco
	,drhrdc as drhrdc
	,ltrim(rtrim(drhrdc)) as drhrdc_conv
	,druser as druser
	,ltrim(rtrim(druser)) as druser_conv
	,drpid as drpid
	,ltrim(rtrim(drpid)) as drpid_conv
	,cast(drupmj as [INT]) as drupmj
	,case when cast(drupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(drupmj as [INT]) %1000, dateadd(year, cast(drupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as drupmj_conv
	,drjobn as drjobn
	,ltrim(rtrim(drjobn)) as drjobn_conv
	,cast(drupmt as [DECIMAL](38, 4)) as drupmt
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
	,drsy
	,drrt
	,drky
	,drdl01
	,drdl02
	,drsphd
	,drudco
	,drhrdc
	,druser
	,drpid
	,drupmj
	,drjobn
	,drupmt,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0005_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0005_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0005_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0005_cdc(sys_integration_id); 
