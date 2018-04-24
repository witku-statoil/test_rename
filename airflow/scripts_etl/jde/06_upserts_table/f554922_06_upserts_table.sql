-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f554922_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f554922_cdc


CREATE TABLE stg_jde.tmp_upsert_f554922_cdc
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
	,scmcu as scmcu
	,ltrim(rtrim(scmcu)) as scmcu_conv
	,cast(scctr as [NVARCHAR](3)) as scctr
	,scaddz as scaddz
	,ltrim(rtrim(scaddz)) as scaddz_conv
	,cast(sceftj as [INT]) as sceftj
	,case when cast(sceftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sceftj as [INT]) %1000, dateadd(year, cast(sceftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sceftj_conv
	,cast(scexdj as [INT]) as scexdj
	,case when cast(scexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(scexdj as [INT]) %1000, dateadd(year, cast(scexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as scexdj_conv
	,cast(scdstn as [DECIMAL](38, 4)) as scdstn
	,cast(scum as [NVARCHAR](2)) as scum
	,scy55char1 as scy55char1
	,ltrim(rtrim(scy55char1)) as scy55char1_conv
	,scy55char2 as scy55char2
	,ltrim(rtrim(scy55char2)) as scy55char2_conv
	,scy55strg1 as scy55strg1
	,ltrim(rtrim(scy55strg1)) as scy55strg1_conv
	,scy55strg2 as scy55strg2
	,ltrim(rtrim(scy55strg2)) as scy55strg2_conv
	,cast(scy55amnt1 as [DECIMAL](38, 4)) as scy55amnt1
	,cast(scy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as scy55amnt1_conv
	,cast(scy55amnt2 as [DECIMAL](38, 4)) as scy55amnt2
	,cast(scy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as scy55amnt2_conv
	,cast(scy55time1 as [DECIMAL](38, 4)) as scy55time1
	,cast(scy55time2 as [DECIMAL](38, 4)) as scy55time2
	,cast(scy55date1 as [INT]) as scy55date1
	,case when cast(scy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(scy55date1 as [INT]) %1000, dateadd(year, cast(scy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as scy55date1_conv
	,cast(scy55date2 as [INT]) as scy55date2
	,case when cast(scy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(scy55date2 as [INT]) %1000, dateadd(year, cast(scy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as scy55date2_conv
	,scurrf as scurrf
	,ltrim(rtrim(scurrf)) as scurrf_conv
	,cast(scurdt as [INT]) as scurdt
	,case when cast(scurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(scurdt as [INT]) %1000, dateadd(year, cast(scurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as scurdt_conv
	,scurcd as scurcd
	,ltrim(rtrim(scurcd)) as scurcd_conv
	,cast(scurab as [DECIMAL](38, 4)) as scurab
	,cast(scurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as scurab_conv
	,cast(scurat as [DECIMAL](38, 4)) as scurat
	,cast(scurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as scurat_conv
	,scuser as scuser
	,ltrim(rtrim(scuser)) as scuser_conv
	,scpid as scpid
	,ltrim(rtrim(scpid)) as scpid_conv
	,scjobn as scjobn
	,ltrim(rtrim(scjobn)) as scjobn_conv
	,cast(scupmj as [INT]) as scupmj
	,case when cast(scupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(scupmj as [INT]) %1000, dateadd(year, cast(scupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as scupmj_conv
	,cast(scupmt as [DECIMAL](38, 4)) as scupmt
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
	,scmcu
	,scctr
	,scaddz
	,sceftj
	,scexdj
	,scdstn
	,scum
	,scy55char1
	,scy55char2
	,scy55strg1
	,scy55strg2
	,scy55amnt1
	,scy55amnt2
	,scy55time1
	,scy55time2
	,scy55date1
	,scy55date2
	,scurrf
	,scurdt
	,scurcd
	,scurab
	,scurat
	,scuser
	,scpid
	,scjobn
	,scupmj
	,scupmt,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f554922_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f554922_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f554922_cdc_sys_integration_id ON stg_jde.tmp_upsert_f554922_cdc(sys_integration_id); 
