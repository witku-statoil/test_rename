-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f554904_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f554904_cdc


CREATE TABLE stg_jde.tmp_upsert_f554904_cdc
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
	,fdvr01 as fdvr01
	,ltrim(rtrim(fdvr01)) as fdvr01_conv
	,cast(fdy55fcode as [NVARCHAR](15)) as fdy55fcode
	,cast(fdy55msgno as [DECIMAL](38, 4)) as fdy55msgno
	,cast(fdy55msgno as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fdy55msgno_conv
	,cast(fdy55insdj as [INT]) as fdy55insdj
	,case when cast(fdy55insdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fdy55insdj as [INT]) %1000, dateadd(year, cast(fdy55insdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fdy55insdj_conv
	,cast(fdy55insdt as [DECIMAL](38, 4)) as fdy55insdt
	,cast(fdldnm as [DECIMAL](38, 4)) as fdldnm
	,cast(fdlnid as [DECIMAL](38, 4)) as fdlnid
	,cast(fdlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as fdlnid_conv
	,cast(fdy55fdst as [DECIMAL](38, 4)) as fdy55fdst
	,cast(fdumd1 as [NVARCHAR](2)) as fdumd1
	,cast(fdy55fwait as [DECIMAL](38, 4)) as fdy55fwait
	,cast(fdfrcg as [DECIMAL](38, 4)) as fdfrcg
	,cast(fdfrcg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as fdfrcg_conv
	,cast(fdy55fwaitc as [DECIMAL](38, 4)) as fdy55fwaitc
	,cast(fdy55fwaitc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as fdy55fwaitc_conv
	,fdedbt as fdedbt
	,ltrim(rtrim(fdedbt)) as fdedbt_conv
	,fdedtn as fdedtn
	,ltrim(rtrim(fdedtn)) as fdedtn_conv
	,fdflag as fdflag
	,ltrim(rtrim(fdflag)) as fdflag_conv
	,fduser as fduser
	,ltrim(rtrim(fduser)) as fduser_conv
	,fdpid as fdpid
	,ltrim(rtrim(fdpid)) as fdpid_conv
	,fdjobn as fdjobn
	,ltrim(rtrim(fdjobn)) as fdjobn_conv
	,cast(fdupmj as [INT]) as fdupmj
	,case when cast(fdupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fdupmj as [INT]) %1000, dateadd(year, cast(fdupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fdupmj_conv
	,cast(fdupmt as [DECIMAL](38, 4)) as fdupmt
	,fdurrf as fdurrf
	,ltrim(rtrim(fdurrf)) as fdurrf_conv
	,fdurcd as fdurcd
	,ltrim(rtrim(fdurcd)) as fdurcd_conv
	,cast(fdurab as [DECIMAL](38, 4)) as fdurab
	,cast(fdurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fdurab_conv
	,cast(fdurat as [DECIMAL](38, 4)) as fdurat
	,cast(fdurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fdurat_conv
	,cast(fdurdt as [INT]) as fdurdt
	,case when cast(fdurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fdurdt as [INT]) %1000, dateadd(year, cast(fdurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fdurdt_conv
	,cast(fdrcd as [NVARCHAR](3)) as fdrcd
	,fdy55char1 as fdy55char1
	,ltrim(rtrim(fdy55char1)) as fdy55char1_conv
	,fdy55char2 as fdy55char2
	,ltrim(rtrim(fdy55char2)) as fdy55char2_conv
	,cast(fdy55date1 as [INT]) as fdy55date1
	,case when cast(fdy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fdy55date1 as [INT]) %1000, dateadd(year, cast(fdy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fdy55date1_conv
	,cast(fdy55date2 as [INT]) as fdy55date2
	,case when cast(fdy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fdy55date2 as [INT]) %1000, dateadd(year, cast(fdy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fdy55date2_conv
	,fdy55strg1 as fdy55strg1
	,ltrim(rtrim(fdy55strg1)) as fdy55strg1_conv
	,fdy55strg2 as fdy55strg2
	,ltrim(rtrim(fdy55strg2)) as fdy55strg2_conv
	,fdy55strg3 as fdy55strg3
	,ltrim(rtrim(fdy55strg3)) as fdy55strg3_conv
	,fdy55strg4 as fdy55strg4
	,ltrim(rtrim(fdy55strg4)) as fdy55strg4_conv
	,fdy55strg5 as fdy55strg5
	,ltrim(rtrim(fdy55strg5)) as fdy55strg5_conv
	,fdy55strg6 as fdy55strg6
	,ltrim(rtrim(fdy55strg6)) as fdy55strg6_conv
	,fdy55strg7 as fdy55strg7
	,ltrim(rtrim(fdy55strg7)) as fdy55strg7_conv
	,fdy55strg8 as fdy55strg8
	,ltrim(rtrim(fdy55strg8)) as fdy55strg8_conv
	,fdy55strg9 as fdy55strg9
	,ltrim(rtrim(fdy55strg9)) as fdy55strg9_conv
	,fdy55strg10 as fdy55strg10
	,ltrim(rtrim(fdy55strg10)) as fdy55strg10_conv
	,fdy55flg1 as fdy55flg1
	,ltrim(rtrim(fdy55flg1)) as fdy55flg1_conv
	,fdy55flg2 as fdy55flg2
	,ltrim(rtrim(fdy55flg2)) as fdy55flg2_conv
	,cast(fdy55num3 as [DECIMAL](38, 4)) as fdy55num3
	,cast(fdy55num3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as fdy55num3_conv
	,cast(fdy55num4 as [DECIMAL](38, 4)) as fdy55num4
	,cast(fdy55num4 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as fdy55num4_conv
	,cast(fdy55time1 as [DECIMAL](38, 4)) as fdy55time1
	,cast(fdy55time2 as [DECIMAL](38, 4)) as fdy55time2
	,cast(fdy55amnt2 as [DECIMAL](38, 4)) as fdy55amnt2
	,cast(fdy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fdy55amnt2_conv
	,cast(fdy55amnt1 as [DECIMAL](38, 4)) as fdy55amnt1
	,cast(fdy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fdy55amnt1_conv
	,cast(fdukid as [DECIMAL](38, 4)) as fdukid
	,fdflg2 as fdflg2
	,ltrim(rtrim(fdflg2)) as fdflg2_conv
	,fdflg3 as fdflg3
	,ltrim(rtrim(fdflg3)) as fdflg3_conv
	,cast(fdxpitraid as [DECIMAL](38, 4)) as fdxpitraid
	,cast(fdxpitraid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fdxpitraid_conv
	,fdflg01 as fdflg01
	,ltrim(rtrim(fdflg01)) as fdflg01_conv
	,cast(fdy55fwttm as [DECIMAL](38, 4)) as fdy55fwttm
	,cast(fdy55fwttm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fdy55fwttm_conv
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
	,fdvr01
	,fdy55fcode
	,fdy55msgno
	,fdy55insdj
	,fdy55insdt
	,fdldnm
	,fdlnid
	,fdy55fdst
	,fdumd1
	,fdy55fwait
	,fdfrcg
	,fdy55fwaitc
	,fdedbt
	,fdedtn
	,fdflag
	,fduser
	,fdpid
	,fdjobn
	,fdupmj
	,fdupmt
	,fdurrf
	,fdurcd
	,fdurab
	,fdurat
	,fdurdt
	,fdrcd
	,fdy55char1
	,fdy55char2
	,fdy55date1
	,fdy55date2
	,fdy55strg1
	,fdy55strg2
	,fdy55strg3
	,fdy55strg4
	,fdy55strg5
	,fdy55strg6
	,fdy55strg7
	,fdy55strg8
	,fdy55strg9
	,fdy55strg10
	,fdy55flg1
	,fdy55flg2
	,fdy55num3
	,fdy55num4
	,fdy55time1
	,fdy55time2
	,fdy55amnt2
	,fdy55amnt1
	,fdukid
	,fdflg2
	,fdflg3
	,fdxpitraid
	,fdflg01
	,fdy55fwttm,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f554904_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f554904_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f554904_cdc_sys_integration_id ON stg_jde.tmp_upsert_f554904_cdc(sys_integration_id); 
