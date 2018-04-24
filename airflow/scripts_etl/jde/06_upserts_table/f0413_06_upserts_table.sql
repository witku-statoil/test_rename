-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0413_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0413_cdc


CREATE TABLE stg_jde.tmp_upsert_f0413_cdc
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
	,cast(rmpyid as [DECIMAL](38, 4)) as rmpyid
	,cast(rmdctm as [NVARCHAR](2)) as rmdctm
	,cast(rmdocm as [DECIMAL](38, 4)) as rmdocm
	,cast(rmpye as [DECIMAL](38, 4)) as rmpye
	,rmglba as rmglba
	,ltrim(rtrim(rmglba)) as rmglba_conv
	,cast(rmdmtj as [INT]) as rmdmtj
	,case when cast(rmdmtj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rmdmtj as [INT]) %1000, dateadd(year, cast(rmdmtj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rmdmtj_conv
	,cast(rmvdgj as [INT]) as rmvdgj
	,case when cast(rmvdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rmvdgj as [INT]) %1000, dateadd(year, cast(rmvdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rmvdgj_conv
	,cast(rmicu as [DECIMAL](38, 4)) as rmicu
	,cast(rmicut as [NVARCHAR](2)) as rmicut
	,cast(rmdicj as [INT]) as rmdicj
	,case when cast(rmdicj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rmdicj as [INT]) %1000, dateadd(year, cast(rmdicj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rmdicj_conv
	,cast(rmpaap as [DECIMAL](38, 4)) as rmpaap
	,cast(rmpaap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rmpaap_conv
	,rmcrcd as rmcrcd
	,ltrim(rtrim(rmcrcd)) as rmcrcd_conv
	,cast(rmcrrm as [NVARCHAR](1)) as rmcrrm
	,cast(rmam as [NVARCHAR](1)) as rmam
	,cast(rmvldt as [INT]) as rmvldt
	,case when cast(rmvldt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rmvldt as [INT]) %1000, dateadd(year, cast(rmvldt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rmvldt_conv
	,cast(rmpyin as [NVARCHAR](1)) as rmpyin
	,rmistp as rmistp
	,ltrim(rtrim(rmistp)) as rmistp_conv
	,rmcbnk as rmcbnk
	,ltrim(rtrim(rmcbnk)) as rmcbnk_conv
	,rmbktr as rmbktr
	,ltrim(rtrim(rmbktr)) as rmbktr_conv
	,rmtorg as rmtorg
	,ltrim(rtrim(rmtorg)) as rmtorg_conv
	,rmuser as rmuser
	,ltrim(rtrim(rmuser)) as rmuser_conv
	,rmpid as rmpid
	,ltrim(rtrim(rmpid)) as rmpid_conv
	,cast(rmupmj as [INT]) as rmupmj
	,case when cast(rmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rmupmj as [INT]) %1000, dateadd(year, cast(rmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rmupmj_conv
	,cast(rmupmt as [DECIMAL](38, 4)) as rmupmt
	,rmjobn as rmjobn
	,ltrim(rtrim(rmjobn)) as rmjobn_conv
	,rmmip as rmmip
	,ltrim(rtrim(rmmip)) as rmmip_conv
	,rmlrfl as rmlrfl
	,ltrim(rtrim(rmlrfl)) as rmlrfl_conv
	,rmprgf as rmprgf
	,ltrim(rtrim(rmprgf)) as rmprgf_conv
	,rmgfl7 as rmgfl7
	,ltrim(rtrim(rmgfl7)) as rmgfl7_conv
	,rmgfl8 as rmgfl8
	,ltrim(rtrim(rmgfl8)) as rmgfl8_conv
	,cast(rmgam3 as [DECIMAL](38, 4)) as rmgam3
	,cast(rmgam3 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rmgam3_conv
	,cast(rmgam4 as [DECIMAL](38, 4)) as rmgam4
	,cast(rmgam4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rmgam4_conv
	,rmgen6 as rmgen6
	,ltrim(rtrim(rmgen6)) as rmgen6_conv
	,rmgen7 as rmgen7
	,ltrim(rtrim(rmgen7)) as rmgen7_conv
	,cast(rmnettcid as [DECIMAL](38, 4)) as rmnettcid
	,cast(rmnettcid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rmnettcid_conv
	,cast(rmnetdoc as [DECIMAL](38, 4)) as rmnetdoc
	,cast(rmnetdoc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rmnetdoc_conv
	,cast(rmrcnd as [NVARCHAR](1)) as rmrcnd
	,rmr3 as rmr3
	,ltrim(rtrim(rmr3)) as rmr3_conv
	,cast(rmcntrtid as [DECIMAL](38, 4)) as rmcntrtid
	,cast(rmcntrtid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rmcntrtid_conv
	,rmcntrtcd as rmcntrtcd
	,ltrim(rtrim(rmcntrtcd)) as rmcntrtcd_conv
	,cast(rmwvid as [DECIMAL](38, 4)) as rmwvid
	,cast(rmwvid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rmwvid_conv
	,rmblscd2 as rmblscd2
	,ltrim(rtrim(rmblscd2)) as rmblscd2_conv
	,rmharper as rmharper
	,ltrim(rtrim(rmharper)) as rmharper_conv
	,rmharsfx as rmharsfx
	,ltrim(rtrim(rmharsfx)) as rmharsfx_conv
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
	,rmpyid
	,rmdctm
	,rmdocm
	,rmpye
	,rmglba
	,rmdmtj
	,rmvdgj
	,rmicu
	,rmicut
	,rmdicj
	,rmpaap
	,rmcrcd
	,rmcrrm
	,rmam
	,rmvldt
	,rmpyin
	,rmistp
	,rmcbnk
	,rmbktr
	,rmtorg
	,rmuser
	,rmpid
	,rmupmj
	,rmupmt
	,rmjobn
	,rmmip
	,rmlrfl
	,rmprgf
	,rmgfl7
	,rmgfl8
	,rmgam3
	,rmgam4
	,rmgen6
	,rmgen7
	,rmnettcid
	,rmnetdoc
	,rmrcnd
	,rmr3
	,rmcntrtid
	,rmcntrtcd
	,rmwvid
	,rmblscd2
	,rmharper
	,rmharsfx,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0413_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0413_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0413_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0413_cdc(sys_integration_id); 
