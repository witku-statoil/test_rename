-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4960_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4960_cdc


CREATE TABLE stg_jde.tmp_upsert_f4960_cdc
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
	,tmvmcu as tmvmcu
	,ltrim(rtrim(tmvmcu)) as tmvmcu_conv
	,cast(tmldnm as [DECIMAL](38, 4)) as tmldnm
	,tmnmcu as tmnmcu
	,ltrim(rtrim(tmnmcu)) as tmnmcu_conv
	,cast(tmorgn as [DECIMAL](38, 4)) as tmorgn
	,tmmcux as tmmcux
	,ltrim(rtrim(tmmcux)) as tmmcux_conv
	,cast(tmancc as [DECIMAL](38, 4)) as tmancc
	,cast(tmanid as [DECIMAL](38, 4)) as tmanid
	,cast(tmovod as [NVARCHAR](1)) as tmovod
	,tmcty1 as tmcty1
	,ltrim(rtrim(tmcty1)) as tmcty1_conv
	,cast(tmadds as [NVARCHAR](3)) as tmadds
	,tmaddz as tmaddz
	,ltrim(rtrim(tmaddz)) as tmaddz_conv
	,cast(tmctr as [NVARCHAR](3)) as tmctr
	,cast(tmzon as [NVARCHAR](3)) as tmzon
	,cast(tmldty as [NVARCHAR](2)) as tmldty
	,cast(tmldls as [NVARCHAR](2)) as tmldls
	,cast(tmload as [INT]) as tmload
	,case when cast(tmload as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(tmload as [INT]) %1000, dateadd(year, cast(tmload as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as tmload_conv
	,cast(tmshft as [NVARCHAR](1)) as tmshft
	,cast(tmseq as [DECIMAL](38, 4)) as tmseq
	,cast(tmtmls as [DECIMAL](38, 4)) as tmtmls
	,cast(tmppdj as [INT]) as tmppdj
	,case when cast(tmppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(tmppdj as [INT]) %1000, dateadd(year, cast(tmppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as tmppdj_conv
	,cast(tmpmdt as [DECIMAL](38, 4)) as tmpmdt
	,cast(tmaddj as [INT]) as tmaddj
	,case when cast(tmaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(tmaddj as [INT]) %1000, dateadd(year, cast(tmaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as tmaddj_conv
	,cast(tmadtm as [DECIMAL](38, 4)) as tmadtm
	,cast(tmmot as [NVARCHAR](3)) as tmmot
	,tmovrm as tmovrm
	,ltrim(rtrim(tmovrm)) as tmovrm_conv
	,tmvtyp as tmvtyp
	,ltrim(rtrim(tmvtyp)) as tmvtyp_conv
	,tmpveh as tmpveh
	,ltrim(rtrim(tmpveh)) as tmpveh_conv
	,tmrlno as tmrlno
	,ltrim(rtrim(tmrlno)) as tmrlno_conv
	,tmdumv as tmdumv
	,ltrim(rtrim(tmdumv)) as tmdumv_conv
	,tmcnnv as tmcnnv
	,ltrim(rtrim(tmcnnv)) as tmcnnv_conv
	,cast(tmcars as [DECIMAL](38, 4)) as tmcars
	,cast(tmcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as tmcars_conv
	,tmovrc as tmovrc
	,ltrim(rtrim(tmovrc)) as tmovrc_conv
	,cast(tmrout as [NVARCHAR](3)) as tmrout
	,cast(tmrtn as [DECIMAL](38, 4)) as tmrtn
	,cast(tmfrsc as [NVARCHAR](8)) as tmfrsc
	,cast(tmczon as [NVARCHAR](10)) as tmczon
	,cast(tmdsgp as [NVARCHAR](3)) as tmdsgp
	,cast(tmdaty as [NVARCHAR](1)) as tmdaty
	,cast(tmdscd as [NVARCHAR](1)) as tmdscd
	,tmldle as tmldle
	,ltrim(rtrim(tmldle)) as tmldle_conv
	,cast(tmseal as [DECIMAL](38, 4)) as tmseal
	,cast(tmseal as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as tmseal_conv
	,tmlmcu as tmlmcu
	,ltrim(rtrim(tmlmcu)) as tmlmcu_conv
	,cast(tmltrp as [DECIMAL](38, 4)) as tmltrp
	,cast(tmdstn as [DECIMAL](38, 4)) as tmdstn
	,cast(tmumd1 as [NVARCHAR](2)) as tmumd1
	,cast(tmdsrc as [NVARCHAR](1)) as tmdsrc
	,cast(tmodst as [DECIMAL](38, 4)) as tmodst
	,cast(tmeltm as [DECIMAL](38, 4)) as tmeltm
	,cast(tmum as [NVARCHAR](2)) as tmum
	,cast(tmcvum as [NVARCHAR](2)) as tmcvum
	,cast(tmwtum as [NVARCHAR](2)) as tmwtum
	,cast(tmvlum as [NVARCHAR](2)) as tmvlum
	,tmvr01 as tmvr01
	,ltrim(rtrim(tmvr01)) as tmvr01_conv
	,cast(tmnstp as [DECIMAL](38, 4)) as tmnstp
	,cast(tmnstp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as tmnstp_conv
	,cast(tmrrtr as [NVARCHAR](1)) as tmrrtr
	,cast(tmratr as [NVARCHAR](1)) as tmratr
	,cast(tmfrvc as [DECIMAL](38, 4)) as tmfrvc
	,cast(tmfrvc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as tmfrvc_conv
	,cast(tmfrvf as [DECIMAL](38, 4)) as tmfrvf
	,cast(tmfrvf as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as tmfrvf_conv
	,tmcrcp as tmcrcp
	,ltrim(rtrim(tmcrcp)) as tmcrcp_conv
	,tmibrs as tmibrs
	,ltrim(rtrim(tmibrs)) as tmibrs_conv
	,tmdkid as tmdkid
	,ltrim(rtrim(tmdkid)) as tmdkid_conv
	,cast(tmdepu as [INT]) as tmdepu
	,case when cast(tmdepu as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(tmdepu as [INT]) %1000, dateadd(year, cast(tmdepu as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as tmdepu_conv
	,cast(tmdlpu as [INT]) as tmdlpu
	,case when cast(tmdlpu as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(tmdlpu as [INT]) %1000, dateadd(year, cast(tmdlpu as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as tmdlpu_conv
	,cast(tmtpuf as [DECIMAL](38, 4)) as tmtpuf
	,cast(tmtput as [DECIMAL](38, 4)) as tmtput
	,cast(tmlslt as [DECIMAL](38, 4)) as tmlslt
	,cast(tmlsut as [DECIMAL](38, 4)) as tmlsut
	,cast(tmlalt as [DECIMAL](38, 4)) as tmlalt
	,cast(tmlaut as [DECIMAL](38, 4)) as tmlaut
	,tmurcd as tmurcd
	,ltrim(rtrim(tmurcd)) as tmurcd_conv
	,cast(tmurdt as [INT]) as tmurdt
	,case when cast(tmurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(tmurdt as [INT]) %1000, dateadd(year, cast(tmurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as tmurdt_conv
	,cast(tmurat as [DECIMAL](38, 4)) as tmurat
	,cast(tmurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as tmurat_conv
	,cast(tmurab as [DECIMAL](38, 4)) as tmurab
	,cast(tmurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as tmurab_conv
	,tmurrf as tmurrf
	,ltrim(rtrim(tmurrf)) as tmurrf_conv
	,tmuser as tmuser
	,ltrim(rtrim(tmuser)) as tmuser_conv
	,tmpid as tmpid
	,ltrim(rtrim(tmpid)) as tmpid_conv
	,tmjobn as tmjobn
	,ltrim(rtrim(tmjobn)) as tmjobn_conv
	,cast(tmupmj as [INT]) as tmupmj
	,case when cast(tmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(tmupmj as [INT]) %1000, dateadd(year, cast(tmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as tmupmj_conv
	,cast(tmtday as [DECIMAL](38, 4)) as tmtday
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
	,tmvmcu
	,tmldnm
	,tmnmcu
	,tmorgn
	,tmmcux
	,tmancc
	,tmanid
	,tmovod
	,tmcty1
	,tmadds
	,tmaddz
	,tmctr
	,tmzon
	,tmldty
	,tmldls
	,tmload
	,tmshft
	,tmseq
	,tmtmls
	,tmppdj
	,tmpmdt
	,tmaddj
	,tmadtm
	,tmmot
	,tmovrm
	,tmvtyp
	,tmpveh
	,tmrlno
	,tmdumv
	,tmcnnv
	,tmcars
	,tmovrc
	,tmrout
	,tmrtn
	,tmfrsc
	,tmczon
	,tmdsgp
	,tmdaty
	,tmdscd
	,tmldle
	,tmseal
	,tmlmcu
	,tmltrp
	,tmdstn
	,tmumd1
	,tmdsrc
	,tmodst
	,tmeltm
	,tmum
	,tmcvum
	,tmwtum
	,tmvlum
	,tmvr01
	,tmnstp
	,tmrrtr
	,tmratr
	,tmfrvc
	,tmfrvf
	,tmcrcp
	,tmibrs
	,tmdkid
	,tmdepu
	,tmdlpu
	,tmtpuf
	,tmtput
	,tmlslt
	,tmlsut
	,tmlalt
	,tmlaut
	,tmurcd
	,tmurdt
	,tmurat
	,tmurab
	,tmurrf
	,tmuser
	,tmpid
	,tmjobn
	,tmupmj
	,tmtday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4960_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4960_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4960_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4960_cdc(sys_integration_id); 
