-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0115_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0115_cdc


CREATE TABLE stg_jde.tmp_upsert_f0115_cdc
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
	,cast(wpan8 as [DECIMAL](38, 4)) as wpan8
	,cast(wpidln as [DECIMAL](38, 4)) as wpidln
	,cast(wpidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wpidln_conv
	,cast(wprck7 as [DECIMAL](38, 4)) as wprck7
	,cast(wpcnln as [DECIMAL](38, 4)) as wpcnln
	,cast(wpcnln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wpcnln_conv
	,cast(wpphtp as [NVARCHAR](4)) as wpphtp
	,wpar1 as wpar1
	,ltrim(rtrim(wpar1)) as wpar1_conv
	,wpph1 as wpph1
	,ltrim(rtrim(wpph1)) as wpph1_conv
	,wpuser as wpuser
	,ltrim(rtrim(wpuser)) as wpuser_conv
	,wppid as wppid
	,ltrim(rtrim(wppid)) as wppid_conv
	,cast(wpupmj as [INT]) as wpupmj
	,case when cast(wpupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wpupmj as [INT]) %1000, dateadd(year, cast(wpupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wpupmj_conv
	,wpjobn as wpjobn
	,ltrim(rtrim(wpjobn)) as wpjobn_conv
	,cast(wpupmt as [DECIMAL](38, 4)) as wpupmt
	,cast(wpcfno1 as [DECIMAL](38, 4)) as wpcfno1
	,cast(wpcfno1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wpcfno1_conv
	,wpgen1 as wpgen1
	,ltrim(rtrim(wpgen1)) as wpgen1_conv
	,wpfalge as wpfalge
	,ltrim(rtrim(wpfalge)) as wpfalge_conv
	,cast(wpsyncs as [DECIMAL](38, 4)) as wpsyncs
	,cast(wpcaad as [DECIMAL](38, 4)) as wpcaad
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
	,wpan8
	,wpidln
	,wprck7
	,wpcnln
	,wpphtp
	,wpar1
	,wpph1
	,wpuser
	,wppid
	,wpupmj
	,wpjobn
	,wpupmt
	,wpcfno1
	,wpgen1
	,wpfalge
	,wpsyncs
	,wpcaad,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0115_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0115_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0115_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0115_cdc(sys_integration_id); 
