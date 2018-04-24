-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f554347_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f554347_cdc


CREATE TABLE stg_jde.tmp_upsert_f554347_cdc
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
	,cast(asaddj as [INT]) as asaddj
	,case when cast(asaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asaddj as [INT]) %1000, dateadd(year, cast(asaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asaddj_conv
	,cast(asan83 as [DECIMAL](38, 4)) as asan83
	,asedtg as asedtg
	,ltrim(rtrim(asedtg)) as asedtg_conv
	,cast(asukid as [DECIMAL](38, 4)) as asukid
	,asy55strg2 as asy55strg2
	,ltrim(rtrim(asy55strg2)) as asy55strg2_conv
	,cast(asitm as [DECIMAL](38, 4)) as asitm
	,aslitm as aslitm
	,ltrim(rtrim(aslitm)) as aslitm_conv
	,asaitm as asaitm
	,ltrim(rtrim(asaitm)) as asaitm_conv
	,asdsc1 as asdsc1
	,ltrim(rtrim(asdsc1)) as asdsc1_conv
	,cast(asmath80 as [DECIMAL](38, 4)) as asmath80
	,cast(asmath80 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asmath80_conv
	,cast(asan8 as [DECIMAL](38, 4)) as asan8
	,asaa04 as asaa04
	,ltrim(rtrim(asaa04)) as asaa04_conv
	,asdscrp3 as asdscrp3
	,ltrim(rtrim(asdscrp3)) as asdscrp3_conv
	,asdscrp4 as asdscrp4
	,ltrim(rtrim(asdscrp4)) as asdscrp4_conv
	,cast(asuncs as [DECIMAL](38, 4)) as asuncs
	,cast(asuncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asuncs_conv
	,cast(asuopn as [DECIMAL](38, 4)) as asuopn
	,cast(asuopn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asuopn_conv
	,cast(asuorg as [DECIMAL](38, 4)) as asuorg
	,cast(asuorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asuorg_conv
	,cast(asmath02 as [DECIMAL](38, 4)) as asmath02
	,cast(asmath02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asmath02_conv
	,cast(asmath03 as [DECIMAL](38, 4)) as asmath03
	,cast(asmath03 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asmath03_conv
	,cast(asaexp as [DECIMAL](38, 4)) as asaexp
	,cast(asaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asaexp_conv
	,asacatt1ds as asacatt1ds
	,ltrim(rtrim(asacatt1ds)) as asacatt1ds_conv
	,asacatt2ds as asacatt2ds
	,ltrim(rtrim(asacatt2ds)) as asacatt2ds_conv
	,cast(asacpr as [DECIMAL](38, 4)) as asacpr
	,cast(asacpr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asacpr_conv
	,cast(asfsrrc as [DECIMAL](38, 4)) as asfsrrc
	,cast(asfsrrc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asfsrrc_conv
	,cast(asacqow as [DECIMAL](38, 4)) as asacqow
	,cast(asacqow as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asacqow_conv
	,cast(asaarc as [DECIMAL](38, 4)) as asaarc
	,cast(asaarc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asaarc_conv
	,cast(asacrc as [DECIMAL](38, 4)) as asacrc
	,cast(asacrc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asacrc_conv
	,cast(asacrg as [DECIMAL](38, 4)) as asacrg
	,cast(asacrg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asacrg_conv
	,cast(asacrtb as [DECIMAL](38, 4)) as asacrtb
	,cast(asacrtb as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asacrtb_conv
	,cast(asaabr as [DECIMAL](38, 4)) as asaabr
	,cast(asaabr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asaabr_conv
	,cast(asactur as [DECIMAL](38, 4)) as asactur
	,cast(asactur as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asactur_conv
	,cast(asacvl as [DECIMAL](38, 4)) as asacvl
	,cast(asacvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asacvl_conv
	,asphrase as asphrase
	,ltrim(rtrim(asphrase)) as asphrase_conv
	,asmcdtl as asmcdtl
	,ltrim(rtrim(asmcdtl)) as asmcdtl_conv
	,assubtxt as assubtxt
	,ltrim(rtrim(assubtxt)) as assubtxt_conv
	,asdes1 as asdes1
	,ltrim(rtrim(asdes1)) as asdes1_conv
	,asdes3 as asdes3
	,ltrim(rtrim(asdes3)) as asdes3_conv
	,asprna1 as asprna1
	,ltrim(rtrim(asprna1)) as asprna1_conv
	,asdes2 as asdes2
	,ltrim(rtrim(asdes2)) as asdes2_conv
	,cast(asan82 as [DECIMAL](38, 4)) as asan82
	,asgflag2 as asgflag2
	,ltrim(rtrim(asgflag2)) as asgflag2_conv
	,asafe as asafe
	,ltrim(rtrim(asafe)) as asafe_conv
	,aslnty as aslnty
	,ltrim(rtrim(aslnty)) as aslnty_conv
	,asaa12 as asaa12
	,ltrim(rtrim(asaa12)) as asaa12_conv
	,asvr01 as asvr01
	,ltrim(rtrim(asvr01)) as asvr01_conv
	,asgfl8 as asgfl8
	,ltrim(rtrim(asgfl8)) as asgfl8_conv
	,cast(asmath10 as [DECIMAL](38, 4)) as asmath10
	,cast(asmath10 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asmath10_conv
	,cast(asuprc as [DECIMAL](38, 4)) as asuprc
	,cast(asuprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asuprc_conv
	,cast(asfrrc as [DECIMAL](38, 4)) as asfrrc
	,cast(asfrrc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asfrrc_conv
	,cast(asamrate as [DECIMAL](38, 4)) as asamrate
	,cast(asamrate as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asamrate_conv
	,cast(asxcpr as [DECIMAL](38, 4)) as asxcpr
	,cast(asxcpr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asxcpr_conv
	,cast(asacrd as [DECIMAL](38, 4)) as asacrd
	,cast(asacrd as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asacrd_conv
	,cast(asfup as [DECIMAL](38, 4)) as asfup
	,cast(asfup as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asfup_conv
	,cast(asofup as [DECIMAL](38, 4)) as asofup
	,cast(asofup as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asofup_conv
	,cast(asachg as [DECIMAL](38, 4)) as asachg
	,cast(asachg as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asachg_conv
	,assbl as assbl
	,ltrim(rtrim(assbl)) as assbl_conv
	,asaa02 as asaa02
	,ltrim(rtrim(asaa02)) as asaa02_conv
	,cast(asmath07 as [DECIMAL](38, 4)) as asmath07
	,cast(asmath07 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asmath07_conv
	,cast(asmath06 as [DECIMAL](38, 4)) as asmath06
	,cast(asmath06 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asmath06_conv
	,cast(asanby as [DECIMAL](38, 4)) as asanby
	,cast(asctime as [DECIMAL](38, 4)) as asctime
	,cast(asctime as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asctime_conv
	,cast(asbgtime as [DECIMAL](38, 4)) as asbgtime
	,cast(asapptime as [DECIMAL](38, 4)) as asapptime
	,cast(asacttime as [DECIMAL](38, 4)) as asacttime
	,cast(asacttime as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asacttime_conv
	,cast(asdkj as [INT]) as asdkj
	,case when cast(asdkj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdkj as [INT]) %1000, dateadd(year, cast(asdkj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdkj_conv
	,asgpf1 as asgpf1
	,ltrim(rtrim(asgpf1)) as asgpf1_conv
	,cast(asecst as [DECIMAL](38, 4)) as asecst
	,cast(asecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asecst_conv
	,cast(asaopn as [DECIMAL](38, 4)) as asaopn
	,cast(asaopn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asaopn_conv
	,cast(asdkc as [INT]) as asdkc
	,case when cast(asdkc as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdkc as [INT]) %1000, dateadd(year, cast(asdkc as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdkc_conv
	,cast(asaa as [DECIMAL](38, 4)) as asaa
	,cast(asaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asaa_conv
	,cast(aspn as [DECIMAL](38, 4)) as aspn
	,asdsc2 as asdsc2
	,ltrim(rtrim(asdsc2)) as asdsc2_conv
	,asaa09 as asaa09
	,ltrim(rtrim(asaa09)) as asaa09_conv
	,cast(asu as [DECIMAL](38, 4)) as asu
	,cast(asu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asu_conv
	,cast(asaa2 as [DECIMAL](38, 4)) as asaa2
	,cast(asaa2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asaa2_conv
	,cast(asaa3 as [DECIMAL](38, 4)) as asaa3
	,cast(asaa3 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asaa3_conv
	,aspost as aspost
	,ltrim(rtrim(aspost)) as aspost_conv
	,cast(asam as [NVARCHAR](1)) as asam
	,cast(assblt as [NVARCHAR](1)) as assblt
	,cast(asre as [NVARCHAR](1)) as asre
	,cast(ascrrm as [NVARCHAR](1)) as ascrrm
	,asprgf as asprgf
	,ltrim(rtrim(asprgf)) as asprgf_conv
	,cast(astx as [NVARCHAR](1)) as astx
	,cast(asdgj as [INT]) as asdgj
	,case when cast(asdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdgj as [INT]) %1000, dateadd(year, cast(asdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdgj_conv
	,cast(asdicj as [INT]) as asdicj
	,case when cast(asdicj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdicj as [INT]) %1000, dateadd(year, cast(asdicj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdicj_conv
	,cast(asdsyj as [INT]) as asdsyj
	,case when cast(asdsyj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdsyj as [INT]) %1000, dateadd(year, cast(asdsyj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdsyj_conv
	,cast(ascmtb as [INT]) as ascmtb
	,case when cast(ascmtb as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ascmtb as [INT]) %1000, dateadd(year, cast(ascmtb as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ascmtb_conv
	,cast(asdate01 as [INT]) as asdate01
	,case when cast(asdate01 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdate01 as [INT]) %1000, dateadd(year, cast(asdate01 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdate01_conv
	,cast(asdate02 as [INT]) as asdate02
	,case when cast(asdate02 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdate02 as [INT]) %1000, dateadd(year, cast(asdate02 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdate02_conv
	,cast(asdate03 as [INT]) as asdate03
	,case when cast(asdate03 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdate03 as [INT]) %1000, dateadd(year, cast(asdate03 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdate03_conv
	,cast(asdate04 as [INT]) as asdate04
	,case when cast(asdate04 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdate04 as [INT]) %1000, dateadd(year, cast(asdate04 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdate04_conv
	,cast(asdrqj as [INT]) as asdrqj
	,case when cast(asdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdrqj as [INT]) %1000, dateadd(year, cast(asdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdrqj_conv
	,cast(asdsvj as [INT]) as asdsvj
	,case when cast(asdsvj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdsvj as [INT]) %1000, dateadd(year, cast(asdsvj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdsvj_conv
	,cast(asdoc as [DECIMAL](38, 4)) as asdoc
	,cast(asjeln as [DECIMAL](38, 4)) as asjeln
	,cast(asicu as [DECIMAL](38, 4)) as asicu
	,cast(asticu as [DECIMAL](38, 4)) as asticu
	,cast(asabo1 as [DECIMAL](38, 4)) as asabo1
	,cast(asctry as [DECIMAL](38, 4)) as asctry
	,cast(asfy as [DECIMAL](38, 4)) as asfy
	,cast(asfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asfy_conv
	,cast(asodoc as [DECIMAL](38, 4)) as asodoc
	,cast(astxitm as [DECIMAL](38, 4)) as astxitm
	,cast(astxitm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as astxitm_conv
	,cast(asacr as [DECIMAL](38, 4)) as asacr
	,cast(asacr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asacr_conv
	,cast(asdlnid as [DECIMAL](38, 4)) as asdlnid
	,cast(asdlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as asdlnid_conv
	,cast(asancr as [DECIMAL](38, 4)) as asancr
	,cast(asrsht as [DECIMAL](38, 4)) as asrsht
	,cast(asrsht as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asrsht_conv
	,cast(astday as [DECIMAL](38, 4)) as astday
	,cast(asbct as [DECIMAL](38, 4)) as asbct
	,cast(asbct as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asbct_conv
	,cast(asurab as [DECIMAL](38, 4)) as asurab
	,cast(asurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asurab_conv
	,cast(asaa1 as [DECIMAL](38, 4)) as asaa1
	,cast(asaa1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asaa1_conv
	,cast(asfea as [DECIMAL](38, 4)) as asfea
	,cast(asfea as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asfea_conv
	,cast(asfec as [DECIMAL](38, 4)) as asfec
	,cast(asfec as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asfec_conv
	,cast(asfrec as [DECIMAL](38, 4)) as asfrec
	,cast(asfrec as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asfrec_conv
	,askco as askco
	,ltrim(rtrim(askco)) as askco_conv
	,cast(asdct as [NVARCHAR](2)) as asdct
	,asextl as asextl
	,ltrim(rtrim(asextl)) as asextl_conv
	,cast(asicut as [NVARCHAR](2)) as asicut
	,asco as asco
	,ltrim(rtrim(asco)) as asco_conv
	,asani as asani
	,ltrim(rtrim(asani)) as asani_conv
	,asaid as asaid
	,ltrim(rtrim(asaid)) as asaid_conv
	,asmcu as asmcu
	,ltrim(rtrim(asmcu)) as asmcu_conv
	,asobj as asobj
	,ltrim(rtrim(asobj)) as asobj_conv
	,assub as assub
	,ltrim(rtrim(assub)) as assub_conv
	,cast(aslt as [NVARCHAR](2)) as aslt
	,ascrcd as ascrcd
	,ltrim(rtrim(ascrcd)) as ascrcd_conv
	,cast(asum as [NVARCHAR](2)) as asum
	,asptc as asptc
	,ltrim(rtrim(asptc)) as asptc_conv
	,asglc as asglc
	,ltrim(rtrim(asglc)) as asglc_conv
	,asexa as asexa
	,ltrim(rtrim(asexa)) as asexa_conv
	,asexr as asexr
	,ltrim(rtrim(asexr)) as asexr_conv
	,asr1 as asr1
	,ltrim(rtrim(asr1)) as asr1_conv
	,asr2 as asr2
	,ltrim(rtrim(asr2)) as asr2_conv
	,asr3 as asr3
	,ltrim(rtrim(asr3)) as asr3_conv
	,cast(asodct as [NVARCHAR](2)) as asodct
	,asosfx as asosfx
	,ltrim(rtrim(asosfx)) as asosfx_conv
	,aspkco as aspkco
	,ltrim(rtrim(aspkco)) as aspkco_conv
	,asokco as asokco
	,ltrim(rtrim(asokco)) as asokco_conv
	,cast(aspdct as [NVARCHAR](2)) as aspdct
	,astorg as astorg
	,ltrim(rtrim(astorg)) as astorg_conv
	,asuser as asuser
	,ltrim(rtrim(asuser)) as asuser_conv
	,aspid as aspid
	,ltrim(rtrim(aspid)) as aspid_conv
	,cast(asupmt as [DECIMAL](38, 4)) as asupmt
	,asjobn as asjobn
	,ltrim(rtrim(asjobn)) as asjobn_conv
	,cast(asupmj as [INT]) as asupmj
	,case when cast(asupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asupmj as [INT]) %1000, dateadd(year, cast(asupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asupmj_conv
	,asbcrc as asbcrc
	,ltrim(rtrim(asbcrc)) as asbcrc_conv
	,astxa1 as astxa1
	,ltrim(rtrim(astxa1)) as astxa1_conv
	,cast(asexr1 as [NVARCHAR](2)) as asexr1
	,asacatt3ds as asacatt3ds
	,ltrim(rtrim(asacatt3ds)) as asacatt3ds_conv
	,asacatt4ds as asacatt4ds
	,ltrim(rtrim(asacatt4ds)) as asacatt4ds_conv
	,asacatt5ds as asacatt5ds
	,ltrim(rtrim(asacatt5ds)) as asacatt5ds_conv
	,asvr02 as asvr02
	,ltrim(rtrim(asvr02)) as asvr02_conv
	,asemcu as asemcu
	,ltrim(rtrim(asemcu)) as asemcu_conv
	,asahbu as asahbu
	,ltrim(rtrim(asahbu)) as asahbu_conv
	,asomcu as asomcu
	,ltrim(rtrim(asomcu)) as asomcu_conv
	,cast(asctr as [NVARCHAR](3)) as asctr
	,asurcd as asurcd
	,ltrim(rtrim(asurcd)) as asurcd_conv
	,asaad as asaad
	,ltrim(rtrim(asaad)) as asaad_conv
	,cast(ascord as [DECIMAL](38, 4)) as ascord
	,asgflag1 as asgflag1
	,ltrim(rtrim(asgflag1)) as asgflag1_conv
	,asflag as asflag
	,ltrim(rtrim(asflag)) as asflag_conv
	,asev01 as asev01
	,ltrim(rtrim(asev01)) as asev01_conv
	,asev02 as asev02
	,ltrim(rtrim(asev02)) as asev02_conv
	,asev03 as asev03
	,ltrim(rtrim(asev03)) as asev03_conv
	,asev04 as asev04
	,ltrim(rtrim(asev04)) as asev04_conv
	,asev05 as asev05
	,ltrim(rtrim(asev05)) as asev05_conv
	,asev06 as asev06
	,ltrim(rtrim(asev06)) as asev06_conv
	,asev07 as asev07
	,ltrim(rtrim(asev07)) as asev07_conv
	,asev08 as asev08
	,ltrim(rtrim(asev08)) as asev08_conv
	,asev09 as asev09
	,ltrim(rtrim(asev09)) as asev09_conv
	,asev10 as asev10
	,ltrim(rtrim(asev10)) as asev10_conv
	,asaa03 as asaa03
	,ltrim(rtrim(asaa03)) as asaa03_conv
	,asaa05 as asaa05
	,ltrim(rtrim(asaa05)) as asaa05_conv
	,asaa06 as asaa06
	,ltrim(rtrim(asaa06)) as asaa06_conv
	,asaa07 as asaa07
	,ltrim(rtrim(asaa07)) as asaa07_conv
	,asaa08 as asaa08
	,ltrim(rtrim(asaa08)) as asaa08_conv
	,asaa10 as asaa10
	,ltrim(rtrim(asaa10)) as asaa10_conv
	,cast(asada1 as [INT]) as asada1
	,case when cast(asada1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asada1 as [INT]) %1000, dateadd(year, cast(asada1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asada1_conv
	,cast(asada2 as [INT]) as asada2
	,case when cast(asada2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asada2 as [INT]) %1000, dateadd(year, cast(asada2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asada2_conv
	,cast(asada3 as [INT]) as asada3
	,case when cast(asada3 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asada3 as [INT]) %1000, dateadd(year, cast(asada3 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asada3_conv
	,cast(asdate05 as [INT]) as asdate05
	,case when cast(asdate05 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdate05 as [INT]) %1000, dateadd(year, cast(asdate05 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdate05_conv
	,cast(asdate1 as [INT]) as asdate1
	,case when cast(asdate1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdate1 as [INT]) %1000, dateadd(year, cast(asdate1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdate1_conv
	,cast(asdate2 as [INT]) as asdate2
	,case when cast(asdate2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdate2 as [INT]) %1000, dateadd(year, cast(asdate2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdate2_conv
	,cast(asdate3 as [INT]) as asdate3
	,case when cast(asdate3 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asdate3 as [INT]) %1000, dateadd(year, cast(asdate3 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asdate3_conv
	,cast(asmath01 as [DECIMAL](38, 4)) as asmath01
	,cast(asmath01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asmath01_conv
	,cast(asmath04 as [DECIMAL](38, 4)) as asmath04
	,cast(asmath04 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asmath04_conv
	,cast(asmath05 as [DECIMAL](38, 4)) as asmath05
	,cast(asmath05 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asmath05_conv
	,cast(asmath08 as [DECIMAL](38, 4)) as asmath08
	,cast(asmath08 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asmath08_conv
	,cast(asmath09 as [DECIMAL](38, 4)) as asmath09
	,cast(asmath09 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asmath09_conv
	,asy55strg1 as asy55strg1
	,ltrim(rtrim(asy55strg1)) as asy55strg1_conv
	,asy55strg3 as asy55strg3
	,ltrim(rtrim(asy55strg3)) as asy55strg3_conv
	,asy55strg4 as asy55strg4
	,ltrim(rtrim(asy55strg4)) as asy55strg4_conv
	,asy55strg5 as asy55strg5
	,ltrim(rtrim(asy55strg5)) as asy55strg5_conv
	,asy55strg6 as asy55strg6
	,ltrim(rtrim(asy55strg6)) as asy55strg6_conv
	,asy55strg7 as asy55strg7
	,ltrim(rtrim(asy55strg7)) as asy55strg7_conv
	,asy55strg8 as asy55strg8
	,ltrim(rtrim(asy55strg8)) as asy55strg8_conv
	,asy55strg9 as asy55strg9
	,ltrim(rtrim(asy55strg9)) as asy55strg9_conv
	,cast(asy55date1 as [INT]) as asy55date1
	,case when cast(asy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asy55date1 as [INT]) %1000, dateadd(year, cast(asy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asy55date1_conv
	,cast(asy55date2 as [INT]) as asy55date2
	,case when cast(asy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(asy55date2 as [INT]) %1000, dateadd(year, cast(asy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as asy55date2_conv
	,cast(asy55time1 as [DECIMAL](38, 4)) as asy55time1
	,cast(asy55time2 as [DECIMAL](38, 4)) as asy55time2
	,asy55char1 as asy55char1
	,ltrim(rtrim(asy55char1)) as asy55char1_conv
	,asy55char2 as asy55char2
	,ltrim(rtrim(asy55char2)) as asy55char2_conv
	,cast(asy55amnt1 as [DECIMAL](38, 4)) as asy55amnt1
	,cast(asy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asy55amnt1_conv
	,cast(asy55amnt2 as [DECIMAL](38, 4)) as asy55amnt2
	,cast(asy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asy55amnt2_conv
	,cast(asy55amnt3 as [DECIMAL](38, 4)) as asy55amnt3
	,cast(asy55amnt3 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asy55amnt3_conv
	,cast(asy55amnt4 as [DECIMAL](38, 4)) as asy55amnt4
	,cast(asy55amnt4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asy55amnt4_conv
	,cast(asunap as [DECIMAL](38, 4)) as asunap
	,cast(asunap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as asunap_conv
	,cast(asuntprcsr as [DECIMAL](38, 4)) as asuntprcsr
	,cast(asuntprcsr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as asuntprcsr_conv
	,cast(asgrwt as [DECIMAL](38, 4)) as asgrwt
	,cast(asgrwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asgrwt_conv
	,cast(ascrec as [DECIMAL](38, 4)) as ascrec
	,cast(ascrec as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ascrec_conv
	,cast(asamc3 as [DECIMAL](38, 4)) as asamc3
	,cast(asamc3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asamc3_conv
	,cast(asurlv as [DECIMAL](38, 4)) as asurlv
	,cast(asurlv as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asurlv_conv
	,cast(asuchg as [DECIMAL](38, 4)) as asuchg
	,cast(asuchg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asuchg_conv
	,cast(asprrc as [DECIMAL](38, 4)) as asprrc
	,cast(asprrc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as asprrc_conv
	,cast(asdoco as [DECIMAL](38, 4)) as asdoco
	,cast(asdcto as [NVARCHAR](2)) as asdcto
	,askcoo as askcoo
	,ltrim(rtrim(askcoo)) as askcoo_conv
	,cast(aslnid as [DECIMAL](38, 4)) as aslnid
	,cast(aslnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as aslnid_conv
	,asatcdname as asatcdname
	,ltrim(rtrim(asatcdname)) as asatcdname_conv
	,cast(asy55uncs as [DECIMAL](38, 4)) as asy55uncs
	,cast(asy55uncs as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as asy55uncs_conv
	,cast(asy55uncs3 as [DECIMAL](38, 4)) as asy55uncs3
	,cast(asy55uncs3 as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as asy55uncs3_conv
	,cast(asy55uncs4 as [DECIMAL](38, 4)) as asy55uncs4
	,cast(asy55uncs4 as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as asy55uncs4_conv
	,cast(asy55uncs5 as [DECIMAL](38, 4)) as asy55uncs5
	,cast(asy55uncs5 as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as asy55uncs5_conv
	,cast(asy55uncs6 as [DECIMAL](38, 4)) as asy55uncs6
	,cast(asy55uncs6 as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as asy55uncs6_conv
	,cast(asy55uncs7 as [DECIMAL](38, 4)) as asy55uncs7
	,cast(asy55uncs7 as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as asy55uncs7_conv
	,cast(asy55uncs8 as [DECIMAL](38, 4)) as asy55uncs8
	,cast(asy55uncs8 as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as asy55uncs8_conv
	,cast(asy55uncs9 as [DECIMAL](38, 4)) as asy55uncs9
	,cast(asy55uncs9 as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as asy55uncs9_conv
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
	,asaddj
	,asan83
	,asedtg
	,asukid
	,asy55strg2
	,asitm
	,aslitm
	,asaitm
	,asdsc1
	,asmath80
	,asan8
	,asaa04
	,asdscrp3
	,asdscrp4
	,asuncs
	,asuopn
	,asuorg
	,asmath02
	,asmath03
	,asaexp
	,asacatt1ds
	,asacatt2ds
	,asacpr
	,asfsrrc
	,asacqow
	,asaarc
	,asacrc
	,asacrg
	,asacrtb
	,asaabr
	,asactur
	,asacvl
	,asphrase
	,asmcdtl
	,assubtxt
	,asdes1
	,asdes3
	,asprna1
	,asdes2
	,asan82
	,asgflag2
	,asafe
	,aslnty
	,asaa12
	,asvr01
	,asgfl8
	,asmath10
	,asuprc
	,asfrrc
	,asamrate
	,asxcpr
	,asacrd
	,asfup
	,asofup
	,asachg
	,assbl
	,asaa02
	,asmath07
	,asmath06
	,asanby
	,asctime
	,asbgtime
	,asapptime
	,asacttime
	,asdkj
	,asgpf1
	,asecst
	,asaopn
	,asdkc
	,asaa
	,aspn
	,asdsc2
	,asaa09
	,asu
	,asaa2
	,asaa3
	,aspost
	,asam
	,assblt
	,asre
	,ascrrm
	,asprgf
	,astx
	,asdgj
	,asdicj
	,asdsyj
	,ascmtb
	,asdate01
	,asdate02
	,asdate03
	,asdate04
	,asdrqj
	,asdsvj
	,asdoc
	,asjeln
	,asicu
	,asticu
	,asabo1
	,asctry
	,asfy
	,asodoc
	,astxitm
	,asacr
	,asdlnid
	,asancr
	,asrsht
	,astday
	,asbct
	,asurab
	,asaa1
	,asfea
	,asfec
	,asfrec
	,askco
	,asdct
	,asextl
	,asicut
	,asco
	,asani
	,asaid
	,asmcu
	,asobj
	,assub
	,aslt
	,ascrcd
	,asum
	,asptc
	,asglc
	,asexa
	,asexr
	,asr1
	,asr2
	,asr3
	,asodct
	,asosfx
	,aspkco
	,asokco
	,aspdct
	,astorg
	,asuser
	,aspid
	,asupmt
	,asjobn
	,asupmj
	,asbcrc
	,astxa1
	,asexr1
	,asacatt3ds
	,asacatt4ds
	,asacatt5ds
	,asvr02
	,asemcu
	,asahbu
	,asomcu
	,asctr
	,asurcd
	,asaad
	,ascord
	,asgflag1
	,asflag
	,asev01
	,asev02
	,asev03
	,asev04
	,asev05
	,asev06
	,asev07
	,asev08
	,asev09
	,asev10
	,asaa03
	,asaa05
	,asaa06
	,asaa07
	,asaa08
	,asaa10
	,asada1
	,asada2
	,asada3
	,asdate05
	,asdate1
	,asdate2
	,asdate3
	,asmath01
	,asmath04
	,asmath05
	,asmath08
	,asmath09
	,asy55strg1
	,asy55strg3
	,asy55strg4
	,asy55strg5
	,asy55strg6
	,asy55strg7
	,asy55strg8
	,asy55strg9
	,asy55date1
	,asy55date2
	,asy55time1
	,asy55time2
	,asy55char1
	,asy55char2
	,asy55amnt1
	,asy55amnt2
	,asy55amnt3
	,asy55amnt4
	,asunap
	,asuntprcsr
	,asgrwt
	,ascrec
	,asamc3
	,asurlv
	,asuchg
	,asprrc
	,asdoco
	,asdcto
	,askcoo
	,aslnid
	,asatcdname
	,asy55uncs
	,asy55uncs3
	,asy55uncs4
	,asy55uncs5
	,asy55uncs6
	,asy55uncs7
	,asy55uncs8
	,asy55uncs9,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f554347_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f554347_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f554347_cdc_sys_integration_id ON stg_jde.tmp_upsert_f554347_cdc(sys_integration_id); 
