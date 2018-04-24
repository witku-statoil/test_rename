-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0101_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0101_cdc


CREATE TABLE stg_jde.tmp_upsert_f0101_cdc
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
	,cast(aban8 as [DECIMAL](38, 4)) as aban8
	,abalky as abalky
	,ltrim(rtrim(abalky)) as abalky_conv
	,abtax as abtax
	,ltrim(rtrim(abtax)) as abtax_conv
	,abalph as abalph
	,ltrim(rtrim(abalph)) as abalph_conv
	,abdc as abdc
	,ltrim(rtrim(abdc)) as abdc_conv
	,abmcu as abmcu
	,ltrim(rtrim(abmcu)) as abmcu_conv
	,cast(absic as [NVARCHAR](10)) as absic
	,cast(ablngp as [NVARCHAR](2)) as ablngp
	,cast(abat1 as [NVARCHAR](3)) as abat1
	,cast(abcm as [NVARCHAR](2)) as abcm
	,cast(abtaxc as [NVARCHAR](1)) as abtaxc
	,cast(abat2 as [NVARCHAR](1)) as abat2
	,cast(abat3 as [NVARCHAR](1)) as abat3
	,cast(abat4 as [NVARCHAR](1)) as abat4
	,cast(abat5 as [NVARCHAR](1)) as abat5
	,cast(abatp as [NVARCHAR](1)) as abatp
	,cast(abatr as [NVARCHAR](1)) as abatr
	,abatpr as abatpr
	,ltrim(rtrim(abatpr)) as abatpr_conv
	,cast(abab3 as [NVARCHAR](1)) as abab3
	,cast(abate as [NVARCHAR](1)) as abate
	,cast(absbli as [NVARCHAR](1)) as absbli
	,cast(abeftb as [INT]) as abeftb
	,case when cast(abeftb as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(abeftb as [INT]) %1000, dateadd(year, cast(abeftb as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as abeftb_conv
	,cast(aban81 as [DECIMAL](38, 4)) as aban81
	,cast(aban82 as [DECIMAL](38, 4)) as aban82
	,cast(aban83 as [DECIMAL](38, 4)) as aban83
	,cast(aban84 as [DECIMAL](38, 4)) as aban84
	,cast(aban86 as [DECIMAL](38, 4)) as aban86
	,cast(aban85 as [DECIMAL](38, 4)) as aban85
	,cast(abac01 as [NVARCHAR](3)) as abac01
	,cast(abac02 as [NVARCHAR](3)) as abac02
	,cast(abac03 as [NVARCHAR](3)) as abac03
	,cast(abac04 as [NVARCHAR](3)) as abac04
	,cast(abac05 as [NVARCHAR](3)) as abac05
	,cast(abac06 as [NVARCHAR](3)) as abac06
	,cast(abac07 as [NVARCHAR](3)) as abac07
	,cast(abac08 as [NVARCHAR](3)) as abac08
	,cast(abac09 as [NVARCHAR](3)) as abac09
	,cast(abac10 as [NVARCHAR](3)) as abac10
	,cast(abac11 as [NVARCHAR](3)) as abac11
	,cast(abac12 as [NVARCHAR](3)) as abac12
	,cast(abac13 as [NVARCHAR](3)) as abac13
	,cast(abac14 as [NVARCHAR](3)) as abac14
	,cast(abac15 as [NVARCHAR](3)) as abac15
	,cast(abac16 as [NVARCHAR](3)) as abac16
	,cast(abac17 as [NVARCHAR](3)) as abac17
	,cast(abac18 as [NVARCHAR](3)) as abac18
	,cast(abac19 as [NVARCHAR](3)) as abac19
	,cast(abac20 as [NVARCHAR](3)) as abac20
	,cast(abac21 as [NVARCHAR](3)) as abac21
	,cast(abac22 as [NVARCHAR](3)) as abac22
	,cast(abac23 as [NVARCHAR](3)) as abac23
	,cast(abac24 as [NVARCHAR](3)) as abac24
	,cast(abac25 as [NVARCHAR](3)) as abac25
	,cast(abac26 as [NVARCHAR](3)) as abac26
	,cast(abac27 as [NVARCHAR](3)) as abac27
	,cast(abac28 as [NVARCHAR](3)) as abac28
	,cast(abac29 as [NVARCHAR](3)) as abac29
	,cast(abac30 as [NVARCHAR](3)) as abac30
	,abglba as abglba
	,ltrim(rtrim(abglba)) as abglba_conv
	,cast(abpti as [DECIMAL](38, 4)) as abpti
	,cast(abpdi as [INT]) as abpdi
	,case when cast(abpdi as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(abpdi as [INT]) %1000, dateadd(year, cast(abpdi as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as abpdi_conv
	,cast(abmsga as [NVARCHAR](1)) as abmsga
	,abrmk as abrmk
	,ltrim(rtrim(abrmk)) as abrmk_conv
	,abtxct as abtxct
	,ltrim(rtrim(abtxct)) as abtxct_conv
	,abtx2 as abtx2
	,ltrim(rtrim(abtx2)) as abtx2_conv
	,abalp1 as abalp1
	,ltrim(rtrim(abalp1)) as abalp1_conv
	,aburcd as aburcd
	,ltrim(rtrim(aburcd)) as aburcd_conv
	,cast(aburdt as [INT]) as aburdt
	,case when cast(aburdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aburdt as [INT]) %1000, dateadd(year, cast(aburdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aburdt_conv
	,cast(aburat as [DECIMAL](38, 4)) as aburat
	,cast(aburat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aburat_conv
	,cast(aburab as [DECIMAL](38, 4)) as aburab
	,cast(aburab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aburab_conv
	,aburrf as aburrf
	,ltrim(rtrim(aburrf)) as aburrf_conv
	,abuser as abuser
	,ltrim(rtrim(abuser)) as abuser_conv
	,abpid as abpid
	,ltrim(rtrim(abpid)) as abpid_conv
	,cast(abupmj as [INT]) as abupmj
	,case when cast(abupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(abupmj as [INT]) %1000, dateadd(year, cast(abupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as abupmj_conv
	,abjobn as abjobn
	,ltrim(rtrim(abjobn)) as abjobn_conv
	,cast(abupmt as [DECIMAL](38, 4)) as abupmt
	,abprgf as abprgf
	,ltrim(rtrim(abprgf)) as abprgf_conv
	,cast(absccltp as [NVARCHAR](2)) as absccltp
	,abticker as abticker
	,ltrim(rtrim(abticker)) as abticker_conv
	,abexchg as abexchg
	,ltrim(rtrim(abexchg)) as abexchg_conv
	,abduns as abduns
	,ltrim(rtrim(abduns)) as abduns_conv
	,cast(abclass01 as [NVARCHAR](3)) as abclass01
	,cast(abclass02 as [NVARCHAR](3)) as abclass02
	,cast(abclass03 as [NVARCHAR](3)) as abclass03
	,cast(abclass04 as [NVARCHAR](3)) as abclass04
	,cast(abclass05 as [NVARCHAR](3)) as abclass05
	,cast(abnoe as [DECIMAL](38, 4)) as abnoe
	,cast(abnoe as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as abnoe_conv
	,cast(abgrowthr as [DECIMAL](38, 4)) as abgrowthr
	,cast(abgrowthr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as abgrowthr_conv
	,abyearstar as abyearstar
	,ltrim(rtrim(abyearstar)) as abyearstar_conv
	,cast(abaempgp as [NVARCHAR](5)) as abaempgp
	,abactin as abactin
	,ltrim(rtrim(abactin)) as abactin_conv
	,cast(abrevrng as [NVARCHAR](5)) as abrevrng
	,cast(absyncs as [DECIMAL](38, 4)) as absyncs
	,cast(abperrs as [DECIMAL](38, 4)) as abperrs
	,cast(abcaad as [DECIMAL](38, 4)) as abcaad
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
	,aban8
	,abalky
	,abtax
	,abalph
	,abdc
	,abmcu
	,absic
	,ablngp
	,abat1
	,abcm
	,abtaxc
	,abat2
	,abat3
	,abat4
	,abat5
	,abatp
	,abatr
	,abatpr
	,abab3
	,abate
	,absbli
	,abeftb
	,aban81
	,aban82
	,aban83
	,aban84
	,aban86
	,aban85
	,abac01
	,abac02
	,abac03
	,abac04
	,abac05
	,abac06
	,abac07
	,abac08
	,abac09
	,abac10
	,abac11
	,abac12
	,abac13
	,abac14
	,abac15
	,abac16
	,abac17
	,abac18
	,abac19
	,abac20
	,abac21
	,abac22
	,abac23
	,abac24
	,abac25
	,abac26
	,abac27
	,abac28
	,abac29
	,abac30
	,abglba
	,abpti
	,abpdi
	,abmsga
	,abrmk
	,abtxct
	,abtx2
	,abalp1
	,aburcd
	,aburdt
	,aburat
	,aburab
	,aburrf
	,abuser
	,abpid
	,abupmj
	,abjobn
	,abupmt
	,abprgf
	,absccltp
	,abticker
	,abexchg
	,abduns
	,abclass01
	,abclass02
	,abclass03
	,abclass04
	,abclass05
	,abnoe
	,abgrowthr
	,abyearstar
	,abaempgp
	,abactin
	,abrevrng
	,absyncs
	,abperrs
	,abcaad,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0101_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0101_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0101_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0101_cdc(sys_integration_id); 
