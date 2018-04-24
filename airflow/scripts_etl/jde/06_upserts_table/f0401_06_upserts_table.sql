-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0401_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0401_cdc


CREATE TABLE stg_jde.tmp_upsert_f0401_cdc
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
	,cast(a6an8 as [DECIMAL](38, 4)) as a6an8
	,a6apc as a6apc
	,ltrim(rtrim(a6apc)) as a6apc_conv
	,a6mcup as a6mcup
	,ltrim(rtrim(a6mcup)) as a6mcup_conv
	,a6obap as a6obap
	,ltrim(rtrim(a6obap)) as a6obap_conv
	,a6aidp as a6aidp
	,ltrim(rtrim(a6aidp)) as a6aidp_conv
	,a6kcop as a6kcop
	,ltrim(rtrim(a6kcop)) as a6kcop_conv
	,cast(a6dcap as [DECIMAL](38, 4)) as a6dcap
	,cast(a6dtap as [NVARCHAR](2)) as a6dtap
	,a6crrp as a6crrp
	,ltrim(rtrim(a6crrp)) as a6crrp_conv
	,a6txa2 as a6txa2
	,ltrim(rtrim(a6txa2)) as a6txa2_conv
	,cast(a6exr2 as [NVARCHAR](2)) as a6exr2
	,cast(a6hdpy as [NVARCHAR](1)) as a6hdpy
	,a6txa3 as a6txa3
	,ltrim(rtrim(a6txa3)) as a6txa3_conv
	,cast(a6exr3 as [NVARCHAR](2)) as a6exr3
	,cast(a6tawh as [DECIMAL](38, 4)) as a6tawh
	,cast(a6pcwh as [DECIMAL](38, 4)) as a6pcwh
	,a6trap as a6trap
	,ltrim(rtrim(a6trap)) as a6trap_conv
	,cast(a6sck as [NVARCHAR](1)) as a6sck
	,cast(a6pyin as [NVARCHAR](1)) as a6pyin
	,cast(a6snto as [DECIMAL](38, 4)) as a6snto
	,cast(a6ab1 as [NVARCHAR](1)) as a6ab1
	,cast(a6fld as [DECIMAL](38, 4)) as a6fld
	,cast(a6sqnl as [NVARCHAR](1)) as a6sqnl
	,a6crca as a6crca
	,ltrim(rtrim(a6crca)) as a6crca_conv
	,cast(a6aypd as [DECIMAL](38, 4)) as a6aypd
	,cast(a6aypd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as a6aypd_conv
	,cast(a6appd as [DECIMAL](38, 4)) as a6appd
	,cast(a6appd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as a6appd_conv
	,cast(a6abam as [DECIMAL](38, 4)) as a6abam
	,cast(a6abam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as a6abam_conv
	,cast(a6aba1 as [DECIMAL](38, 4)) as a6aba1
	,cast(a6aba1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as a6aba1_conv
	,cast(a6aprc as [DECIMAL](38, 4)) as a6aprc
	,cast(a6aprc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as a6aprc_conv
	,cast(a6mino as [DECIMAL](38, 4)) as a6mino
	,cast(a6mino as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as a6mino_conv
	,cast(a6maxo as [DECIMAL](38, 4)) as a6maxo
	,cast(a6maxo as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as a6maxo_conv
	,cast(a6an8r as [DECIMAL](38, 4)) as a6an8r
	,cast(a6badt as [NVARCHAR](1)) as a6badt
	,cast(a6cpgp as [NVARCHAR](8)) as a6cpgp
	,cast(a6ortp as [NVARCHAR](8)) as a6ortp
	,cast(a6inmg as [NVARCHAR](10)) as a6inmg
	,cast(a6hold as [NVARCHAR](2)) as a6hold
	,cast(a6rout as [NVARCHAR](3)) as a6rout
	,cast(a6stop as [NVARCHAR](3)) as a6stop
	,cast(a6zon as [NVARCHAR](3)) as a6zon
	,cast(a6ancr as [DECIMAL](38, 4)) as a6ancr
	,cast(a6cars as [DECIMAL](38, 4)) as a6cars
	,cast(a6cars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as a6cars_conv
	,a6del1 as a6del1
	,ltrim(rtrim(a6del1)) as a6del1_conv
	,a6del2 as a6del2
	,ltrim(rtrim(a6del2)) as a6del2_conv
	,cast(a6ltdt as [DECIMAL](38, 4)) as a6ltdt
	,cast(a6ltdt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as a6ltdt_conv
	,cast(a6frth as [NVARCHAR](3)) as a6frth
	,cast(a6invc as [DECIMAL](38, 4)) as a6invc
	,cast(a6invc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as a6invc_conv
	,a6plst as a6plst
	,ltrim(rtrim(a6plst)) as a6plst_conv
	,cast(a6wumd as [NVARCHAR](2)) as a6wumd
	,cast(a6vumd as [NVARCHAR](2)) as a6vumd
	,cast(a6prp5 as [NVARCHAR](3)) as a6prp5
	,cast(a6edpm as [NVARCHAR](1)) as a6edpm
	,cast(a6edci as [NVARCHAR](1)) as a6edci
	,cast(a6edii as [NVARCHAR](1)) as a6edii
	,cast(a6edqd as [DECIMAL](38, 4)) as a6edqd
	,cast(a6edad as [DECIMAL](38, 4)) as a6edad
	,a6edf1 as a6edf1
	,ltrim(rtrim(a6edf1)) as a6edf1_conv
	,cast(a6edf2 as [NVARCHAR](1)) as a6edf2
	,cast(a6vi01 as [NVARCHAR](1)) as a6vi01
	,cast(a6vi02 as [NVARCHAR](1)) as a6vi02
	,a6vi03 as a6vi03
	,ltrim(rtrim(a6vi03)) as a6vi03_conv
	,a6vi04 as a6vi04
	,ltrim(rtrim(a6vi04)) as a6vi04_conv
	,a6vi05 as a6vi05
	,ltrim(rtrim(a6vi05)) as a6vi05_conv
	,cast(a6mnsc as [NVARCHAR](1)) as a6mnsc
	,cast(a6ato as [NVARCHAR](1)) as a6ato
	,a6rvnt as a6rvnt
	,ltrim(rtrim(a6rvnt)) as a6rvnt_conv
	,a6urcd as a6urcd
	,ltrim(rtrim(a6urcd)) as a6urcd_conv
	,cast(a6urdt as [INT]) as a6urdt
	,case when cast(a6urdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(a6urdt as [INT]) %1000, dateadd(year, cast(a6urdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as a6urdt_conv
	,cast(a6urat as [DECIMAL](38, 4)) as a6urat
	,cast(a6urat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as a6urat_conv
	,cast(a6urab as [DECIMAL](38, 4)) as a6urab
	,cast(a6urab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as a6urab_conv
	,a6urrf as a6urrf
	,ltrim(rtrim(a6urrf)) as a6urrf_conv
	,a6user as a6user
	,ltrim(rtrim(a6user)) as a6user_conv
	,a6pid as a6pid
	,ltrim(rtrim(a6pid)) as a6pid_conv
	,a6jobn as a6jobn
	,ltrim(rtrim(a6jobn)) as a6jobn_conv
	,cast(a6upmj as [INT]) as a6upmj
	,case when cast(a6upmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(a6upmj as [INT]) %1000, dateadd(year, cast(a6upmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as a6upmj_conv
	,cast(a6upmt as [DECIMAL](38, 4)) as a6upmt
	,cast(a6asn as [NVARCHAR](8)) as a6asn
	,cast(a6crmd as [NVARCHAR](1)) as a6crmd
	,cast(a6avch as [NVARCHAR](1)) as a6avch
	,a6atrl as a6atrl
	,ltrim(rtrim(a6atrl)) as a6atrl_conv
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
	,a6an8
	,a6apc
	,a6mcup
	,a6obap
	,a6aidp
	,a6kcop
	,a6dcap
	,a6dtap
	,a6crrp
	,a6txa2
	,a6exr2
	,a6hdpy
	,a6txa3
	,a6exr3
	,a6tawh
	,a6pcwh
	,a6trap
	,a6sck
	,a6pyin
	,a6snto
	,a6ab1
	,a6fld
	,a6sqnl
	,a6crca
	,a6aypd
	,a6appd
	,a6abam
	,a6aba1
	,a6aprc
	,a6mino
	,a6maxo
	,a6an8r
	,a6badt
	,a6cpgp
	,a6ortp
	,a6inmg
	,a6hold
	,a6rout
	,a6stop
	,a6zon
	,a6ancr
	,a6cars
	,a6del1
	,a6del2
	,a6ltdt
	,a6frth
	,a6invc
	,a6plst
	,a6wumd
	,a6vumd
	,a6prp5
	,a6edpm
	,a6edci
	,a6edii
	,a6edqd
	,a6edad
	,a6edf1
	,a6edf2
	,a6vi01
	,a6vi02
	,a6vi03
	,a6vi04
	,a6vi05
	,a6mnsc
	,a6ato
	,a6rvnt
	,a6urcd
	,a6urdt
	,a6urat
	,a6urab
	,a6urrf
	,a6user
	,a6pid
	,a6jobn
	,a6upmj
	,a6upmt
	,a6asn
	,a6crmd
	,a6avch
	,a6atrl,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0401_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0401_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0401_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0401_cdc(sys_integration_id); 
