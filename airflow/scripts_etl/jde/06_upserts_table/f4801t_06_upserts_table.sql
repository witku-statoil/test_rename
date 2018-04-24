-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4801t_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4801t_cdc


CREATE TABLE stg_jde.tmp_upsert_f4801t_cdc
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
	,cast(wadoco as [DECIMAL](38, 4)) as wadoco
	,waline as waline
	,ltrim(rtrim(waline)) as waline_conv
	,cast(wamwdh as [NVARCHAR](1)) as wamwdh
	,cast(wascsp as [DECIMAL](38, 4)) as wascsp
	,cast(washft as [NVARCHAR](1)) as washft
	,cast(wasrcn as [NVARCHAR](1)) as wasrcn
	,cast(waledg as [NVARCHAR](2)) as waledg
	,cast(wacts4 as [DECIMAL](38, 4)) as wacts4
	,cast(wacts4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wacts4_conv
	,cast(wacts7 as [DECIMAL](38, 4)) as wacts7
	,cast(wacts7 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wacts7_conv
	,cast(wacts8 as [DECIMAL](38, 4)) as wacts8
	,cast(wacts8 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wacts8_conv
	,waaid as waaid
	,ltrim(rtrim(waaid)) as waaid_conv
	,waalse as waalse
	,ltrim(rtrim(waalse)) as waalse_conv
	,waasid as waasid
	,ltrim(rtrim(waasid)) as waasid_conv
	,cast(waatst as [INT]) as waatst
	,case when cast(waatst as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(waatst as [INT]) %1000, dateadd(year, cast(waatst as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as waatst_conv
	,cast(wabseq as [DECIMAL](38, 4)) as wabseq
	,cast(wabseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wabseq_conv
	,wachpr as wachpr
	,ltrim(rtrim(wachpr)) as wachpr_conv
	,wacrcd as wacrcd
	,ltrim(rtrim(wacrcd)) as wacrcd_conv
	,wacrce as wacrce
	,ltrim(rtrim(wacrce)) as wacrce_conv
	,wacrcf as wacrcf
	,ltrim(rtrim(wacrcf)) as wacrcf_conv
	,cast(wad5j as [INT]) as wad5j
	,case when cast(wad5j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wad5j as [INT]) %1000, dateadd(year, cast(wad5j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wad5j_conv
	,cast(wad6j as [INT]) as wad6j
	,case when cast(wad6j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wad6j as [INT]) %1000, dateadd(year, cast(wad6j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wad6j_conv
	,wadraw as wadraw
	,ltrim(rtrim(wadraw)) as wadraw_conv
	,wadual as wadual
	,ltrim(rtrim(wadual)) as wadual_conv
	,cast(wampce as [NVARCHAR](1)) as wampce
	,cast(wamprc as [NVARCHAR](1)) as wamprc
	,waobj as waobj
	,ltrim(rtrim(waobj)) as waobj_conv
	,cast(waotam as [DECIMAL](38, 4)) as waotam
	,cast(waotam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waotam_conv
	,cast(waprjm as [DECIMAL](38, 4)) as waprjm
	,cast(waprjm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as waprjm_conv
	,waprrp as waprrp
	,ltrim(rtrim(waprrp)) as waprrp_conv
	,cast(washpp as [NVARCHAR](1)) as washpp
	,cast(wasqor as [DECIMAL](38, 4)) as wasqor
	,cast(wasqor as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wasqor_conv
	,cast(wasrkf as [NVARCHAR](1)) as wasrkf
	,cast(wasrnk as [DECIMAL](38, 4)) as wasrnk
	,cast(wasrnk as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wasrnk_conv
	,cast(wassoq as [DECIMAL](38, 4)) as wassoq
	,cast(wassoq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wassoq_conv
	,cast(watraf as [NVARCHAR](1)) as watraf
	,cast(wauom2 as [NVARCHAR](2)) as wauom2
	,cast(wawr11 as [NVARCHAR](3)) as wawr11
	,cast(wawr12 as [NVARCHAR](3)) as wawr12
	,cast(wawr13 as [NVARCHAR](3)) as wawr13
	,cast(wawr14 as [NVARCHAR](3)) as wawr14
	,cast(wawr15 as [NVARCHAR](3)) as wawr15
	,cast(wawr16 as [NVARCHAR](3)) as wawr16
	,cast(wawr17 as [NVARCHAR](3)) as wawr17
	,cast(wawr18 as [NVARCHAR](3)) as wawr18
	,cast(wawr19 as [NVARCHAR](3)) as wawr19
	,cast(wawr20 as [NVARCHAR](3)) as wawr20
	,cast(wajbcd as [NVARCHAR](6)) as wajbcd
	,wavfwo as wavfwo
	,ltrim(rtrim(wavfwo)) as wavfwo_conv
	,wapmtn as wapmtn
	,ltrim(rtrim(wapmtn)) as wapmtn_conv
	,waissue as waissue
	,ltrim(rtrim(waissue)) as waissue_conv
	,cast(waprodm as [NVARCHAR](8)) as waprodm
	,wawho2 as wawho2
	,ltrim(rtrim(wawho2)) as wawho2_conv
	,waar1 as waar1
	,ltrim(rtrim(waar1)) as waar1_conv
	,waphn1 as waphn1
	,ltrim(rtrim(waphn1)) as waphn1_conv
	,cast(watmco as [DECIMAL](38, 4)) as watmco
	,cast(wamthpr as [NVARCHAR](1)) as wamthpr
	,cast(waentck as [NVARCHAR](3)) as waentck
	,cast(waryin as [NVARCHAR](1)) as waryin
	,cast(warstm as [DECIMAL](38, 4)) as warstm
	,cast(warstm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as warstm_conv
	,cast(wactr as [NVARCHAR](3)) as wactr
	,cast(waregion as [NVARCHAR](3)) as waregion
	,watxa1 as watxa1
	,ltrim(rtrim(watxa1)) as watxa1_conv
	,cast(waexr1 as [NVARCHAR](2)) as waexr1
	,cast(walngp as [NVARCHAR](2)) as walngp
	,cast(waglccv as [NVARCHAR](4)) as waglccv
	,cast(waglcnc as [NVARCHAR](4)) as waglcnc
	,cast(wacovgr as [NVARCHAR](8)) as wacovgr
	,cast(waasn4 as [NVARCHAR](8)) as waasn4
	,cast(waasn2 as [NVARCHAR](8)) as waasn2
	,cast(wasest as [DECIMAL](38, 4)) as wasest
	,cast(waseet as [DECIMAL](38, 4)) as waseet
	,wadsavname as wadsavname
	,ltrim(rtrim(wadsavname)) as wadsavname_conv
	,cast(watimezones as [NVARCHAR](2)) as watimezones
	,cast(waprodf as [NVARCHAR](8)) as waprodf
	,cast(wacslprt as [DECIMAL](38, 4)) as wacslprt
	,cast(wacslprt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wacslprt_conv
	,wamcucsl as wamcucsl
	,ltrim(rtrim(wamcucsl)) as wamcucsl_conv
	,warlot as warlot
	,ltrim(rtrim(warlot)) as warlot_conv
	,cast(wafailcd as [NVARCHAR](8)) as wafailcd
	,cast(wafaildt as [INT]) as wafaildt
	,case when cast(wafaildt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wafaildt as [INT]) %1000, dateadd(year, cast(wafaildt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wafaildt_conv
	,cast(wafailtm as [DECIMAL](38, 4)) as wafailtm
	,cast(wafailtm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wafailtm_conv
	,cast(warepdt as [INT]) as warepdt
	,case when cast(warepdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(warepdt as [INT]) %1000, dateadd(year, cast(warepdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as warepdt_conv
	,cast(wareptm as [DECIMAL](38, 4)) as wareptm
	,cast(wareptm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wareptm_conv
	,cast(wavend as [DECIMAL](38, 4)) as wavend
	,cast(wavend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wavend_conv
	,cast(waan8as as [DECIMAL](38, 4)) as waan8as
	,cast(waan8as as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as waan8as_conv
	,cast(waan8srm as [DECIMAL](38, 4)) as waan8srm
	,cast(waan8srm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as waan8srm_conv
	,cast(wasryn as [NVARCHAR](1)) as wasryn
	,waentcks as waentcks
	,ltrim(rtrim(waentcks)) as waentcks_conv
	,cast(wacurbalm1 as [DECIMAL](38, 4)) as wacurbalm1
	,cast(wacurbalm1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wacurbalm1_conv
	,cast(wacurbalm2 as [DECIMAL](38, 4)) as wacurbalm2
	,cast(wacurbalm2 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wacurbalm2_conv
	,cast(wacurbalm3 as [DECIMAL](38, 4)) as wacurbalm3
	,cast(wacurbalm3 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wacurbalm3_conv
	,wacrdc as wacrdc
	,ltrim(rtrim(wacrdc)) as wacrdc_conv
	,cast(wacrrm as [NVARCHAR](1)) as wacrrm
	,cast(wacrr as [DECIMAL](38, 4)) as wacrr
	,cast(wavmrs31 as [NVARCHAR](2)) as wavmrs31
	,cast(wavmrs32 as [NVARCHAR](8)) as wavmrs32
	,cast(waseqn as [DECIMAL](38, 4)) as waseqn
	,cast(waplmr as [DECIMAL](38, 4)) as waplmr
	,cast(waplmr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waplmr_conv
	,cast(wapllb as [DECIMAL](38, 4)) as wapllb
	,cast(wapllb as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wapllb_conv
	,cast(waplos as [DECIMAL](38, 4)) as waplos
	,cast(waplos as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waplos_conv
	,cast(wabgtc as [DECIMAL](38, 4)) as wabgtc
	,cast(wabgtc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wabgtc_conv
	,cast(watoem as [DECIMAL](38, 4)) as watoem
	,cast(watoem as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as watoem_conv
	,cast(watopl as [DECIMAL](38, 4)) as watopl
	,cast(watopl as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as watopl_conv
	,cast(waplsa as [DECIMAL](38, 4)) as waplsa
	,cast(waplsa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waplsa_conv
	,cast(waplsu as [DECIMAL](38, 4)) as waplsu
	,cast(waplsu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waplsu_conv
	,cast(waessa as [DECIMAL](38, 4)) as waessa
	,cast(waessa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waessa_conv
	,cast(waessu as [DECIMAL](38, 4)) as waessu
	,cast(waessu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waessu_conv
	,cast(waacsa as [DECIMAL](38, 4)) as waacsa
	,cast(waacsa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waacsa_conv
	,cast(waacsu as [DECIMAL](38, 4)) as waacsu
	,cast(waacsu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waacsu_conv
	,cast(waoacm as [DECIMAL](38, 4)) as waoacm
	,cast(waoacm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waoacm_conv
	,cast(waracm as [DECIMAL](38, 4)) as waracm
	,cast(waracm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waracm_conv
	,wahplf as wahplf
	,ltrim(rtrim(wahplf)) as wahplf_conv
	,cast(wavmrs33 as [NVARCHAR](10)) as wavmrs33
	,cast(wascall as [NVARCHAR](1)) as wascall
	,cast(waisno as [DECIMAL](38, 4)) as waisno
	,warmthd as warmthd
	,ltrim(rtrim(warmthd)) as warmthd_conv
	,cast(wadoc as [DECIMAL](38, 4)) as wadoc
	,cast(wadct as [NVARCHAR](2)) as wadct
	,wakco as wakco
	,ltrim(rtrim(wakco)) as wakco_conv
	,wacoch as wacoch
	,ltrim(rtrim(wacoch)) as wacoch_conv
	,cast(walnid as [DECIMAL](38, 4)) as walnid
	,cast(walnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as walnid_conv
	,wacrtu as wacrtu
	,ltrim(rtrim(wacrtu)) as wacrtu_conv
	,waxevt as waxevt
	,ltrim(rtrim(waxevt)) as waxevt_conv
	,wamcult as wamcult
	,ltrim(rtrim(wamcult)) as wamcult_conv
	,cast(wawschf as [NVARCHAR](1)) as wawschf
	,wadfmdp as wadfmdp
	,ltrim(rtrim(wadfmdp)) as wadfmdp_conv
	,wapmpn as wapmpn
	,ltrim(rtrim(wapmpn)) as wapmpn_conv
	,cast(wapns as [DECIMAL](38, 4)) as wapns
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
	,wadoco
	,waline
	,wamwdh
	,wascsp
	,washft
	,wasrcn
	,waledg
	,wacts4
	,wacts7
	,wacts8
	,waaid
	,waalse
	,waasid
	,waatst
	,wabseq
	,wachpr
	,wacrcd
	,wacrce
	,wacrcf
	,wad5j
	,wad6j
	,wadraw
	,wadual
	,wampce
	,wamprc
	,waobj
	,waotam
	,waprjm
	,waprrp
	,washpp
	,wasqor
	,wasrkf
	,wasrnk
	,wassoq
	,watraf
	,wauom2
	,wawr11
	,wawr12
	,wawr13
	,wawr14
	,wawr15
	,wawr16
	,wawr17
	,wawr18
	,wawr19
	,wawr20
	,wajbcd
	,wavfwo
	,wapmtn
	,waissue
	,waprodm
	,wawho2
	,waar1
	,waphn1
	,watmco
	,wamthpr
	,waentck
	,waryin
	,warstm
	,wactr
	,waregion
	,watxa1
	,waexr1
	,walngp
	,waglccv
	,waglcnc
	,wacovgr
	,waasn4
	,waasn2
	,wasest
	,waseet
	,wadsavname
	,watimezones
	,waprodf
	,wacslprt
	,wamcucsl
	,warlot
	,wafailcd
	,wafaildt
	,wafailtm
	,warepdt
	,wareptm
	,wavend
	,waan8as
	,waan8srm
	,wasryn
	,waentcks
	,wacurbalm1
	,wacurbalm2
	,wacurbalm3
	,wacrdc
	,wacrrm
	,wacrr
	,wavmrs31
	,wavmrs32
	,waseqn
	,waplmr
	,wapllb
	,waplos
	,wabgtc
	,watoem
	,watopl
	,waplsa
	,waplsu
	,waessa
	,waessu
	,waacsa
	,waacsu
	,waoacm
	,waracm
	,wahplf
	,wavmrs33
	,wascall
	,waisno
	,warmthd
	,wadoc
	,wadct
	,wakco
	,wacoch
	,walnid
	,wacrtu
	,waxevt
	,wamcult
	,wawschf
	,wadfmdp
	,wapmpn
	,wapns,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4801t_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4801t_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4801t_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4801t_cdc(sys_integration_id); 
