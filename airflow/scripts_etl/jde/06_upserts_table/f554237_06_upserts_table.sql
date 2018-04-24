-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f554237_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f554237_cdc


CREATE TABLE stg_jde.tmp_upsert_f554237_cdc
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
	,cast(rsaddj as [INT]) as rsaddj
	,case when cast(rsaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsaddj as [INT]) %1000, dateadd(year, cast(rsaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsaddj_conv
	,rsvr01 as rsvr01
	,ltrim(rtrim(rsvr01)) as rsvr01_conv
	,rsmcu as rsmcu
	,ltrim(rtrim(rsmcu)) as rsmcu_conv
	,rstpc as rstpc
	,ltrim(rtrim(rstpc)) as rstpc_conv
	,cast(rstax1 as [NVARCHAR](1)) as rstax1
	,rsback as rsback
	,ltrim(rtrim(rsback)) as rsback_conv
	,rsurrf as rsurrf
	,ltrim(rtrim(rsurrf)) as rsurrf_conv
	,rssbal as rssbal
	,ltrim(rtrim(rssbal)) as rssbal_conv
	,rsaa18 as rsaa18
	,ltrim(rtrim(rsaa18)) as rsaa18_conv
	,rsvr02 as rsvr02
	,ltrim(rtrim(rsvr02)) as rsvr02_conv
	,cast(rsacttime as [DECIMAL](38, 4)) as rsacttime
	,cast(rsacttime as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rsacttime_conv
	,cast(rsobsz as [DECIMAL](38, 4)) as rsobsz
	,rsaa30 as rsaa30
	,ltrim(rtrim(rsaa30)) as rsaa30_conv
	,cast(rsctr as [NVARCHAR](3)) as rsctr
	,rsaa20 as rsaa20
	,ltrim(rtrim(rsaa20)) as rsaa20_conv
	,cast(rssbtm as [DECIMAL](38, 4)) as rssbtm
	,rsev01 as rsev01
	,ltrim(rtrim(rsev01)) as rsev01_conv
	,rsev02 as rsev02
	,ltrim(rtrim(rsev02)) as rsev02_conv
	,rsev03 as rsev03
	,ltrim(rtrim(rsev03)) as rsev03_conv
	,rsev04 as rsev04
	,ltrim(rtrim(rsev04)) as rsev04_conv
	,rsev05 as rsev05
	,ltrim(rtrim(rsev05)) as rsev05_conv
	,rsev06 as rsev06
	,ltrim(rtrim(rsev06)) as rsev06_conv
	,rsev07 as rsev07
	,ltrim(rtrim(rsev07)) as rsev07_conv
	,rsev08 as rsev08
	,ltrim(rtrim(rsev08)) as rsev08_conv
	,rsev09 as rsev09
	,ltrim(rtrim(rsev09)) as rsev09_conv
	,rsaurst2 as rsaurst2
	,ltrim(rtrim(rsaurst2)) as rsaurst2_conv
	,rsaurst3 as rsaurst3
	,ltrim(rtrim(rsaurst3)) as rsaurst3_conv
	,rsy55char1 as rsy55char1
	,ltrim(rtrim(rsy55char1)) as rsy55char1_conv
	,rsy55char2 as rsy55char2
	,ltrim(rtrim(rsy55char2)) as rsy55char2_conv
	,rsy55strg1 as rsy55strg1
	,ltrim(rtrim(rsy55strg1)) as rsy55strg1_conv
	,rsy55strg2 as rsy55strg2
	,ltrim(rtrim(rsy55strg2)) as rsy55strg2_conv
	,cast(rsy55amnt1 as [DECIMAL](38, 4)) as rsy55amnt1
	,cast(rsy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rsy55amnt1_conv
	,cast(rsy55amnt2 as [DECIMAL](38, 4)) as rsy55amnt2
	,cast(rsy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rsy55amnt2_conv
	,cast(rsy55time1 as [DECIMAL](38, 4)) as rsy55time1
	,cast(rsy55time2 as [DECIMAL](38, 4)) as rsy55time2
	,cast(rsy55date1 as [INT]) as rsy55date1
	,case when cast(rsy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsy55date1 as [INT]) %1000, dateadd(year, cast(rsy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsy55date1_conv
	,cast(rsy55date2 as [INT]) as rsy55date2
	,case when cast(rsy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsy55date2 as [INT]) %1000, dateadd(year, cast(rsy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsy55date2_conv
	,rsuser as rsuser
	,ltrim(rtrim(rsuser)) as rsuser_conv
	,rspid as rspid
	,ltrim(rtrim(rspid)) as rspid_conv
	,rsjobn as rsjobn
	,ltrim(rtrim(rsjobn)) as rsjobn_conv
	,cast(rsupmj as [INT]) as rsupmj
	,case when cast(rsupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsupmj as [INT]) %1000, dateadd(year, cast(rsupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsupmj_conv
	,cast(rsupmt as [DECIMAL](38, 4)) as rsupmt
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
	,rsaddj
	,rsvr01
	,rsmcu
	,rstpc
	,rstax1
	,rsback
	,rsurrf
	,rssbal
	,rsaa18
	,rsvr02
	,rsacttime
	,rsobsz
	,rsaa30
	,rsctr
	,rsaa20
	,rssbtm
	,rsev01
	,rsev02
	,rsev03
	,rsev04
	,rsev05
	,rsev06
	,rsev07
	,rsev08
	,rsev09
	,rsaurst2
	,rsaurst3
	,rsy55char1
	,rsy55char2
	,rsy55strg1
	,rsy55strg2
	,rsy55amnt1
	,rsy55amnt2
	,rsy55time1
	,rsy55time2
	,rsy55date1
	,rsy55date2
	,rsuser
	,rspid
	,rsjobn
	,rsupmj
	,rsupmt,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f554237_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f554237_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f554237_cdc_sys_integration_id ON stg_jde.tmp_upsert_f554237_cdc(sys_integration_id); 
