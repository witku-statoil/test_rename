-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f41511_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f41511_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f41511_cdc
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
	,cast(padoc as [DECIMAL](38, 4)) as padoc
	,cast(pajeln as [DECIMAL](38, 4)) as pajeln
	,cast(padct as [NVARCHAR](2)) as padct
	,pakco as pakco
	,ltrim(rtrim(pakco)) as pakco_conv
	,cast(padoco as [DECIMAL](38, 4)) as padoco
	,cast(padcto as [NVARCHAR](2)) as padcto
	,pakcoo as pakcoo
	,ltrim(rtrim(pakcoo)) as pakcoo_conv
	,cast(palnid as [DECIMAL](38, 4)) as palnid
	,cast(palnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as palnid_conv
	,cast(panlin as [DECIMAL](38, 4)) as panlin
	,cast(padgl as [INT]) as padgl
	,case when cast(padgl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(padgl as [INT]) %1000, dateadd(year, cast(padgl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as padgl_conv
	,cast(patrdj as [INT]) as patrdj
	,case when cast(patrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(patrdj as [INT]) %1000, dateadd(year, cast(patrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as patrdj_conv
	,cast(paicu as [DECIMAL](38, 4)) as paicu
	,patrcd as patrcd
	,ltrim(rtrim(patrcd)) as patrcd_conv
	,pamcu as pamcu
	,ltrim(rtrim(pamcu)) as pamcu_conv
	,patkid as patkid
	,ltrim(rtrim(patkid)) as patkid_conv
	,cast(paitm as [DECIMAL](38, 4)) as paitm
	,cast(patrno as [DECIMAL](38, 4)) as patrno
	,cast(patemp as [DECIMAL](38, 4)) as patemp
	,cast(patemp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as patemp_conv
	,cast(pastpu as [NVARCHAR](1)) as pastpu
	,cast(padnty as [DECIMAL](38, 4)) as padnty
	,cast(padnty as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as padnty_conv
	,cast(padntp as [NVARCHAR](1)) as padntp
	,cast(padetp as [DECIMAL](38, 4)) as padetp
	,cast(padetp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as padetp_conv
	,cast(padtpu as [NVARCHAR](1)) as padtpu
	,cast(paambr as [DECIMAL](38, 4)) as paambr
	,cast(paambr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as paambr_conv
	,cast(pabum3 as [NVARCHAR](2)) as pabum3
	,cast(paambi as [DECIMAL](38, 4)) as paambi
	,cast(paambi as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as paambi_conv
	,cast(paambu as [NVARCHAR](2)) as paambu
	,cast(pavcf as [DECIMAL](38, 4)) as pavcf
	,cast(pavcf as [DECIMAL](38, 5)) / cast(100000 as [DECIMAL](38, 5)) as pavcf_conv
	,cast(pastok as [DECIMAL](38, 4)) as pastok
	,cast(pastok as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pastok_conv
	,cast(pabum4 as [NVARCHAR](2)) as pabum4
	,cast(pawgtr as [DECIMAL](38, 4)) as pawgtr
	,cast(pawgtr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pawgtr_conv
	,cast(pabum5 as [NVARCHAR](2)) as pabum5
	,cast(pastum as [DECIMAL](38, 4)) as pastum
	,cast(pastum as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pastum_conv
	,cast(pabum6 as [NVARCHAR](2)) as pabum6
	,cast(parcd as [NVARCHAR](3)) as parcd
	,paupcf as paupcf
	,ltrim(rtrim(paupcf)) as paupcf_conv
	,palotn as palotn
	,ltrim(rtrim(palotn)) as palotn_conv
	,cast(palots as [NVARCHAR](1)) as palots
	,cast(pammej as [INT]) as pammej
	,case when cast(pammej as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pammej as [INT]) %1000, dateadd(year, cast(pammej as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pammej_conv
	,cast(pauncs as [DECIMAL](38, 4)) as pauncs
	,cast(pauncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pauncs_conv
	,cast(paecst as [DECIMAL](38, 4)) as paecst
	,cast(paecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as paecst_conv
	,patrex as patrex
	,ltrim(rtrim(patrex)) as patrex_conv
	,cast(paan8 as [DECIMAL](38, 4)) as paan8
	,padmct as padmct
	,ltrim(rtrim(padmct)) as padmct_conv
	,cast(padmcs as [DECIMAL](38, 4)) as padmcs
	,paacor as paacor
	,ltrim(rtrim(paacor)) as paacor_conv
	,pahcor as pahcor
	,ltrim(rtrim(pahcor)) as pahcor_conv
	,cast(padipt as [NVARCHAR](1)) as padipt
	,cast(pagdip as [DECIMAL](38, 4)) as pagdip
	,cast(pagdip as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pagdip_conv
	,cast(pawdip as [DECIMAL](38, 4)) as pawdip
	,cast(pawdip as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pawdip_conv
	,cast(pavapp as [DECIMAL](38, 4)) as pavapp
	,cast(pavapp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pavapp_conv
	,cast(papreu as [NVARCHAR](2)) as papreu
	,cast(palpgv as [DECIMAL](38, 4)) as palpgv
	,cast(palpgv as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as palpgv_conv
	,cast(patpu1 as [NVARCHAR](1)) as patpu1
	,cast(paslip as [NVARCHAR](1)) as paslip
	,cast(pavapw as [DECIMAL](38, 4)) as pavapw
	,cast(pavapw as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pavapw_conv
	,cast(pabum0 as [NVARCHAR](2)) as pabum0
	,cast(pavapv as [DECIMAL](38, 4)) as pavapv
	,cast(pavapv as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pavapv_conv
	,cast(pabum9 as [NVARCHAR](2)) as pabum9
	,cast(paliqw as [DECIMAL](38, 4)) as paliqw
	,cast(paliqw as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as paliqw_conv
	,cast(pabum7 as [NVARCHAR](2)) as pabum7
	,cast(paliqv as [DECIMAL](38, 4)) as paliqv
	,cast(paliqv as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as paliqv_conv
	,cast(pabum8 as [NVARCHAR](2)) as pabum8
	,cast(paovol as [DECIMAL](38, 4)) as paovol
	,cast(paovol as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as paovol_conv
	,cast(pabum2 as [NVARCHAR](2)) as pabum2
	,cast(pardtm as [DECIMAL](38, 4)) as pardtm
	,cast(padte as [INT]) as padte
	,case when cast(padte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(padte as [INT]) %1000, dateadd(year, cast(padte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as padte_conv
	,pametn as pametn
	,ltrim(rtrim(pametn)) as pametn_conv
	,cast(paopnr as [DECIMAL](38, 4)) as paopnr
	,cast(paopnr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as paopnr_conv
	,cast(papncr as [DECIMAL](38, 4)) as papncr
	,cast(papncr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as papncr_conv
	,pavehi as pavehi
	,ltrim(rtrim(pavehi)) as pavehi_conv
	,pavmcu as pavmcu
	,ltrim(rtrim(pavmcu)) as pavmcu_conv
	,cast(patrp as [DECIMAL](38, 4)) as patrp
	,cast(pacmpn as [DECIMAL](38, 4)) as pacmpn
	,cast(pacmpn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pacmpn_conv
	,cast(pabfwt as [DECIMAL](38, 4)) as pabfwt
	,cast(pabfwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pabfwt_conv
	,cast(pabwtu as [NVARCHAR](2)) as pabwtu
	,cast(paafwt as [DECIMAL](38, 4)) as paafwt
	,cast(paafwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as paafwt_conv
	,cast(paawtu as [NVARCHAR](2)) as paawtu
	,cast(pathdt as [INT]) as pathdt
	,case when cast(pathdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pathdt as [INT]) %1000, dateadd(year, cast(pathdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pathdt_conv
	,cast(paopdt as [INT]) as paopdt
	,case when cast(paopdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(paopdt as [INT]) %1000, dateadd(year, cast(paopdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as paopdt_conv
	,cast(palrst as [DECIMAL](38, 4)) as palrst
	,cast(parecs as [NVARCHAR](1)) as parecs
	,cast(papgms as [NVARCHAR](1)) as papgms
	,cast(paglrn as [DECIMAL](38, 4)) as paglrn
	,cast(paglrn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as paglrn_conv
	,cast(paptnr as [DECIMAL](38, 4)) as paptnr
	,paurcd as paurcd
	,ltrim(rtrim(paurcd)) as paurcd_conv
	,cast(paurat as [DECIMAL](38, 4)) as paurat
	,cast(paurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as paurat_conv
	,cast(paurab as [DECIMAL](38, 4)) as paurab
	,cast(paurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as paurab_conv
	,paurrf as paurrf
	,ltrim(rtrim(paurrf)) as paurrf_conv
	,cast(paurdt as [INT]) as paurdt
	,case when cast(paurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(paurdt as [INT]) %1000, dateadd(year, cast(paurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as paurdt_conv
	,pauser as pauser
	,ltrim(rtrim(pauser)) as pauser_conv
	,papid as papid
	,ltrim(rtrim(papid)) as papid_conv
	,pajobn as pajobn
	,ltrim(rtrim(pajobn)) as pajobn_conv
	,cast(paupmj as [INT]) as paupmj
	,case when cast(paupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(paupmj as [INT]) %1000, dateadd(year, cast(paupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as paupmj_conv
	,cast(patday as [DECIMAL](38, 4)) as patday
	,cast(paukid as [DECIMAL](38, 4)) as paukid
	,pabpas as pabpas
	,ltrim(rtrim(pabpas)) as pabpas_conv
	,palotc as palotc
	,ltrim(rtrim(palotc)) as palotc_conv
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
	,padoc
	,pajeln
	,padct
	,pakco
	,padoco
	,padcto
	,pakcoo
	,palnid
	,panlin
	,padgl
	,patrdj
	,paicu
	,patrcd
	,pamcu
	,patkid
	,paitm
	,patrno
	,patemp
	,pastpu
	,padnty
	,padntp
	,padetp
	,padtpu
	,paambr
	,pabum3
	,paambi
	,paambu
	,pavcf
	,pastok
	,pabum4
	,pawgtr
	,pabum5
	,pastum
	,pabum6
	,parcd
	,paupcf
	,palotn
	,palots
	,pammej
	,pauncs
	,paecst
	,patrex
	,paan8
	,padmct
	,padmcs
	,paacor
	,pahcor
	,padipt
	,pagdip
	,pawdip
	,pavapp
	,papreu
	,palpgv
	,patpu1
	,paslip
	,pavapw
	,pabum0
	,pavapv
	,pabum9
	,paliqw
	,pabum7
	,paliqv
	,pabum8
	,paovol
	,pabum2
	,pardtm
	,padte
	,pametn
	,paopnr
	,papncr
	,pavehi
	,pavmcu
	,patrp
	,pacmpn
	,pabfwt
	,pabwtu
	,paafwt
	,paawtu
	,pathdt
	,paopdt
	,palrst
	,parecs
	,papgms
	,paglrn
	,paptnr
	,paurcd
	,paurat
	,paurab
	,paurrf
	,paurdt
	,pauser
	,papid
	,pajobn
	,paupmj
	,patday
	,paukid
	,pabpas
	,palotc,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f41511_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f41511_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f41511_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f41511_cdc(sys_integration_id); 
