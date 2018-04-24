-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0150_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0150_cdc


CREATE TABLE stg_jde.tmp_upsert_f0150_cdc
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
	,cast(maostp as [NVARCHAR](3)) as maostp
	,cast(mapa8 as [DECIMAL](38, 4)) as mapa8
	,cast(maan8 as [DECIMAL](38, 4)) as maan8
	,cast(madss7 as [DECIMAL](38, 4)) as madss7
	,cast(mabefd as [INT]) as mabefd
	,case when cast(mabefd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mabefd as [INT]) %1000, dateadd(year, cast(mabefd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mabefd_conv
	,cast(maeefd as [INT]) as maeefd
	,case when cast(maeefd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(maeefd as [INT]) %1000, dateadd(year, cast(maeefd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as maeefd_conv
	,marmk as marmk
	,ltrim(rtrim(marmk)) as marmk_conv
	,mauser as mauser
	,ltrim(rtrim(mauser)) as mauser_conv
	,cast(maupmj as [INT]) as maupmj
	,case when cast(maupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(maupmj as [INT]) %1000, dateadd(year, cast(maupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as maupmj_conv
	,mapid as mapid
	,ltrim(rtrim(mapid)) as mapid_conv
	,majobn as majobn
	,ltrim(rtrim(majobn)) as majobn_conv
	,cast(maupmt as [DECIMAL](38, 4)) as maupmt
	,cast(masyncs as [DECIMAL](38, 4)) as masyncs
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
	,maostp
	,mapa8
	,maan8
	,madss7
	,mabefd
	,maeefd
	,marmk
	,mauser
	,maupmj
	,mapid
	,majobn
	,maupmt
	,masyncs,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0150_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0150_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0150_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0150_cdc(sys_integration_id); 
