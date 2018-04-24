-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4211_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4211_cdc


CREATE TABLE stg_jde.tmp_upsert_f4211_cdc
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
	,sdkcoo as sdkcoo
	,ltrim(rtrim(sdkcoo)) as sdkcoo_conv
	,cast(sddoco as [DECIMAL](38, 4)) as sddoco
	,cast(sddcto as [NVARCHAR](2)) as sddcto
	,cast(sdlnid as [DECIMAL](38, 4)) as sdlnid
	,cast(sdlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdlnid_conv
	,sdsfxo as sdsfxo
	,ltrim(rtrim(sdsfxo)) as sdsfxo_conv
	,sdmcu as sdmcu
	,ltrim(rtrim(sdmcu)) as sdmcu_conv
	,sdco as sdco
	,ltrim(rtrim(sdco)) as sdco_conv
	,sdokco as sdokco
	,ltrim(rtrim(sdokco)) as sdokco_conv
	,sdoorn as sdoorn
	,ltrim(rtrim(sdoorn)) as sdoorn_conv
	,cast(sdocto as [NVARCHAR](2)) as sdocto
	,cast(sdogno as [DECIMAL](38, 4)) as sdogno
	,cast(sdogno as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdogno_conv
	,sdrkco as sdrkco
	,ltrim(rtrim(sdrkco)) as sdrkco_conv
	,sdrorn as sdrorn
	,ltrim(rtrim(sdrorn)) as sdrorn_conv
	,cast(sdrcto as [NVARCHAR](2)) as sdrcto
	,cast(sdrlln as [DECIMAL](38, 4)) as sdrlln
	,cast(sdrlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdrlln_conv
	,sddmct as sddmct
	,ltrim(rtrim(sddmct)) as sddmct_conv
	,cast(sddmcs as [DECIMAL](38, 4)) as sddmcs
	,cast(sdan8 as [DECIMAL](38, 4)) as sdan8
	,cast(sdshan as [DECIMAL](38, 4)) as sdshan
	,cast(sdpa8 as [DECIMAL](38, 4)) as sdpa8
	,cast(sddrqj as [INT]) as sddrqj
	,case when cast(sddrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sddrqj as [INT]) %1000, dateadd(year, cast(sddrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sddrqj_conv
	,cast(sdtrdj as [INT]) as sdtrdj
	,case when cast(sdtrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdtrdj as [INT]) %1000, dateadd(year, cast(sdtrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdtrdj_conv
	,cast(sdpddj as [INT]) as sdpddj
	,case when cast(sdpddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdpddj as [INT]) %1000, dateadd(year, cast(sdpddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdpddj_conv
	,cast(sdaddj as [INT]) as sdaddj
	,case when cast(sdaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdaddj as [INT]) %1000, dateadd(year, cast(sdaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdaddj_conv
	,cast(sdivd as [INT]) as sdivd
	,case when cast(sdivd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdivd as [INT]) %1000, dateadd(year, cast(sdivd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdivd_conv
	,cast(sdcndj as [INT]) as sdcndj
	,case when cast(sdcndj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdcndj as [INT]) %1000, dateadd(year, cast(sdcndj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdcndj_conv
	,cast(sddgl as [INT]) as sddgl
	,case when cast(sddgl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sddgl as [INT]) %1000, dateadd(year, cast(sddgl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sddgl_conv
	,cast(sdrsdj as [INT]) as sdrsdj
	,case when cast(sdrsdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdrsdj as [INT]) %1000, dateadd(year, cast(sdrsdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdrsdj_conv
	,cast(sdpefj as [INT]) as sdpefj
	,case when cast(sdpefj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdpefj as [INT]) %1000, dateadd(year, cast(sdpefj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdpefj_conv
	,cast(sdppdj as [INT]) as sdppdj
	,case when cast(sdppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdppdj as [INT]) %1000, dateadd(year, cast(sdppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdppdj_conv
	,sdvr01 as sdvr01
	,ltrim(rtrim(sdvr01)) as sdvr01_conv
	,sdvr02 as sdvr02
	,ltrim(rtrim(sdvr02)) as sdvr02_conv
	,cast(sditm as [DECIMAL](38, 4)) as sditm
	,sdlitm as sdlitm
	,ltrim(rtrim(sdlitm)) as sdlitm_conv
	,sdaitm as sdaitm
	,ltrim(rtrim(sdaitm)) as sdaitm_conv
	,sdlocn as sdlocn
	,ltrim(rtrim(sdlocn)) as sdlocn_conv
	,sdlotn as sdlotn
	,ltrim(rtrim(sdlotn)) as sdlotn_conv
	,cast(sdfrgd as [NVARCHAR](3)) as sdfrgd
	,cast(sdthgd as [NVARCHAR](3)) as sdthgd
	,cast(sdfrmp as [DECIMAL](38, 4)) as sdfrmp
	,cast(sdfrmp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdfrmp_conv
	,cast(sdthrp as [DECIMAL](38, 4)) as sdthrp
	,cast(sdthrp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdthrp_conv
	,cast(sdexdp as [DECIMAL](38, 4)) as sdexdp
	,cast(sdexdp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdexdp_conv
	,sddsc1 as sddsc1
	,ltrim(rtrim(sddsc1)) as sddsc1_conv
	,sddsc2 as sddsc2
	,ltrim(rtrim(sddsc2)) as sddsc2_conv
	,sdlnty as sdlnty
	,ltrim(rtrim(sdlnty)) as sdlnty_conv
	,cast(sdnxtr as [NVARCHAR](3)) as sdnxtr
	,cast(sdlttr as [NVARCHAR](3)) as sdlttr
	,sdemcu as sdemcu
	,ltrim(rtrim(sdemcu)) as sdemcu_conv
	,sdrlit as sdrlit
	,ltrim(rtrim(sdrlit)) as sdrlit_conv
	,cast(sdktln as [DECIMAL](38, 4)) as sdktln
	,cast(sdktln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdktln_conv
	,cast(sdcpnt as [DECIMAL](38, 4)) as sdcpnt
	,cast(sdrkit as [DECIMAL](38, 4)) as sdrkit
	,cast(sdrkit as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdrkit_conv
	,cast(sdktp as [DECIMAL](38, 4)) as sdktp
	,cast(sdktp as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdktp_conv
	,cast(sdsrp1 as [NVARCHAR](3)) as sdsrp1
	,cast(sdsrp2 as [NVARCHAR](3)) as sdsrp2
	,cast(sdsrp3 as [NVARCHAR](3)) as sdsrp3
	,cast(sdsrp4 as [NVARCHAR](3)) as sdsrp4
	,cast(sdsrp5 as [NVARCHAR](3)) as sdsrp5
	,cast(sdprp1 as [NVARCHAR](3)) as sdprp1
	,cast(sdprp2 as [NVARCHAR](3)) as sdprp2
	,cast(sdprp3 as [NVARCHAR](3)) as sdprp3
	,cast(sdprp4 as [NVARCHAR](3)) as sdprp4
	,cast(sdprp5 as [NVARCHAR](3)) as sdprp5
	,cast(sduom as [NVARCHAR](2)) as sduom
	,cast(sduorg as [DECIMAL](38, 4)) as sduorg
	,cast(sduorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sduorg_conv
	,cast(sdsoqs as [DECIMAL](38, 4)) as sdsoqs
	,cast(sdsoqs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdsoqs_conv
	,cast(sdsobk as [DECIMAL](38, 4)) as sdsobk
	,cast(sdsobk as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdsobk_conv
	,cast(sdsocn as [DECIMAL](38, 4)) as sdsocn
	,cast(sdsocn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdsocn_conv
	,cast(sdsone as [DECIMAL](38, 4)) as sdsone
	,cast(sdsone as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdsone_conv
	,cast(sduopn as [DECIMAL](38, 4)) as sduopn
	,cast(sduopn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sduopn_conv
	,cast(sdqtyt as [DECIMAL](38, 4)) as sdqtyt
	,cast(sdqtyt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdqtyt_conv
	,cast(sdqrlv as [DECIMAL](38, 4)) as sdqrlv
	,cast(sdqrlv as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdqrlv_conv
	,cast(sdcomm as [NVARCHAR](1)) as sdcomm
	,cast(sdotqy as [NVARCHAR](1)) as sdotqy
	,cast(sduprc as [DECIMAL](38, 4)) as sduprc
	,cast(sduprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sduprc_conv
	,cast(sdaexp as [DECIMAL](38, 4)) as sdaexp
	,cast(sdaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sdaexp_conv
	,cast(sdaopn as [DECIMAL](38, 4)) as sdaopn
	,cast(sdaopn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sdaopn_conv
	,sdprov as sdprov
	,ltrim(rtrim(sdprov)) as sdprov_conv
	,sdtpc as sdtpc
	,ltrim(rtrim(sdtpc)) as sdtpc_conv
	,cast(sdapum as [NVARCHAR](2)) as sdapum
	,cast(sdlprc as [DECIMAL](38, 4)) as sdlprc
	,cast(sdlprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdlprc_conv
	,cast(sduncs as [DECIMAL](38, 4)) as sduncs
	,cast(sduncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sduncs_conv
	,cast(sdecst as [DECIMAL](38, 4)) as sdecst
	,cast(sdecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sdecst_conv
	,sdcsto as sdcsto
	,ltrim(rtrim(sdcsto)) as sdcsto_conv
	,cast(sdtcst as [DECIMAL](38, 4)) as sdtcst
	,cast(sdtcst as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdtcst_conv
	,cast(sdinmg as [NVARCHAR](10)) as sdinmg
	,sdptc as sdptc
	,ltrim(rtrim(sdptc)) as sdptc_conv
	,cast(sdryin as [NVARCHAR](1)) as sdryin
	,cast(sddtbs as [NVARCHAR](1)) as sddtbs
	,cast(sdtrdc as [DECIMAL](38, 4)) as sdtrdc
	,cast(sdtrdc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdtrdc_conv
	,cast(sdfun2 as [DECIMAL](38, 4)) as sdfun2
	,cast(sdfun2 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdfun2_conv
	,cast(sdasn as [NVARCHAR](8)) as sdasn
	,cast(sdprgr as [NVARCHAR](8)) as sdprgr
	,sdclvl as sdclvl
	,ltrim(rtrim(sdclvl)) as sdclvl_conv
	,cast(sdcadc as [DECIMAL](38, 4)) as sdcadc
	,cast(sdcadc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdcadc_conv
	,sdkco as sdkco
	,ltrim(rtrim(sdkco)) as sdkco_conv
	,cast(sddoc as [DECIMAL](38, 4)) as sddoc
	,cast(sddct as [NVARCHAR](2)) as sddct
	,cast(sdodoc as [DECIMAL](38, 4)) as sdodoc
	,cast(sdodct as [NVARCHAR](2)) as sdodct
	,sdokc as sdokc
	,ltrim(rtrim(sdokc)) as sdokc_conv
	,cast(sdpsn as [DECIMAL](38, 4)) as sdpsn
	,cast(sddeln as [DECIMAL](38, 4)) as sddeln
	,cast(sdtax1 as [NVARCHAR](1)) as sdtax1
	,sdtxa1 as sdtxa1
	,ltrim(rtrim(sdtxa1)) as sdtxa1_conv
	,cast(sdexr1 as [NVARCHAR](2)) as sdexr1
	,sdatxt as sdatxt
	,ltrim(rtrim(sdatxt)) as sdatxt_conv
	,cast(sdprio as [NVARCHAR](1)) as sdprio
	,sdresl as sdresl
	,ltrim(rtrim(sdresl)) as sdresl_conv
	,sdback as sdback
	,ltrim(rtrim(sdback)) as sdback_conv
	,sdsbal as sdsbal
	,ltrim(rtrim(sdsbal)) as sdsbal_conv
	,sdapts as sdapts
	,ltrim(rtrim(sdapts)) as sdapts_conv
	,cast(sdlob as [NVARCHAR](3)) as sdlob
	,cast(sdeuse as [NVARCHAR](3)) as sdeuse
	,cast(sddtys as [NVARCHAR](2)) as sddtys
	,cast(sdntr as [NVARCHAR](2)) as sdntr
	,cast(sdvend as [DECIMAL](38, 4)) as sdvend
	,cast(sdvend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdvend_conv
	,cast(sdcars as [DECIMAL](38, 4)) as sdcars
	,cast(sdcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdcars_conv
	,cast(sdmot as [NVARCHAR](3)) as sdmot
	,cast(sdrout as [NVARCHAR](3)) as sdrout
	,cast(sdstop as [NVARCHAR](3)) as sdstop
	,cast(sdzon as [NVARCHAR](3)) as sdzon
	,sdcnid as sdcnid
	,ltrim(rtrim(sdcnid)) as sdcnid_conv
	,cast(sdfrth as [NVARCHAR](3)) as sdfrth
	,cast(sdshcm as [NVARCHAR](3)) as sdshcm
	,cast(sdshcn as [NVARCHAR](3)) as sdshcn
	,sdsern as sdsern
	,ltrim(rtrim(sdsern)) as sdsern_conv
	,cast(sduom1 as [NVARCHAR](2)) as sduom1
	,cast(sdpqor as [DECIMAL](38, 4)) as sdpqor
	,cast(sdpqor as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdpqor_conv
	,cast(sduom2 as [NVARCHAR](2)) as sduom2
	,cast(sdsqor as [DECIMAL](38, 4)) as sdsqor
	,cast(sdsqor as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdsqor_conv
	,cast(sduom4 as [NVARCHAR](2)) as sduom4
	,cast(sditwt as [DECIMAL](38, 4)) as sditwt
	,cast(sditwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sditwt_conv
	,cast(sdwtum as [NVARCHAR](2)) as sdwtum
	,cast(sditvl as [DECIMAL](38, 4)) as sditvl
	,cast(sditvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sditvl_conv
	,cast(sdvlum as [NVARCHAR](2)) as sdvlum
	,cast(sdrprc as [NVARCHAR](8)) as sdrprc
	,cast(sdorpr as [NVARCHAR](8)) as sdorpr
	,sdorp as sdorp
	,ltrim(rtrim(sdorp)) as sdorp_conv
	,sdcmgp as sdcmgp
	,ltrim(rtrim(sdcmgp)) as sdcmgp_conv
	,sdglc as sdglc
	,ltrim(rtrim(sdglc)) as sdglc_conv
	,cast(sdctry as [DECIMAL](38, 4)) as sdctry
	,cast(sdfy as [DECIMAL](38, 4)) as sdfy
	,cast(sdfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdfy_conv
	,sdso01 as sdso01
	,ltrim(rtrim(sdso01)) as sdso01_conv
	,sdso02 as sdso02
	,ltrim(rtrim(sdso02)) as sdso02_conv
	,sdso03 as sdso03
	,ltrim(rtrim(sdso03)) as sdso03_conv
	,sdso04 as sdso04
	,ltrim(rtrim(sdso04)) as sdso04_conv
	,sdso05 as sdso05
	,ltrim(rtrim(sdso05)) as sdso05_conv
	,sdso06 as sdso06
	,ltrim(rtrim(sdso06)) as sdso06_conv
	,sdso07 as sdso07
	,ltrim(rtrim(sdso07)) as sdso07_conv
	,sdso08 as sdso08
	,ltrim(rtrim(sdso08)) as sdso08_conv
	,sdso09 as sdso09
	,ltrim(rtrim(sdso09)) as sdso09_conv
	,sdso10 as sdso10
	,ltrim(rtrim(sdso10)) as sdso10_conv
	,sdso11 as sdso11
	,ltrim(rtrim(sdso11)) as sdso11_conv
	,sdso12 as sdso12
	,ltrim(rtrim(sdso12)) as sdso12_conv
	,sdso13 as sdso13
	,ltrim(rtrim(sdso13)) as sdso13_conv
	,sdso14 as sdso14
	,ltrim(rtrim(sdso14)) as sdso14_conv
	,sdso15 as sdso15
	,ltrim(rtrim(sdso15)) as sdso15_conv
	,sdacom as sdacom
	,ltrim(rtrim(sdacom)) as sdacom_conv
	,sdcmcg as sdcmcg
	,ltrim(rtrim(sdcmcg)) as sdcmcg_conv
	,cast(sdrcd as [NVARCHAR](3)) as sdrcd
	,cast(sdgrwt as [DECIMAL](38, 4)) as sdgrwt
	,cast(sdgrwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdgrwt_conv
	,cast(sdgwum as [NVARCHAR](2)) as sdgwum
	,sdsbl as sdsbl
	,ltrim(rtrim(sdsbl)) as sdsbl_conv
	,cast(sdsblt as [NVARCHAR](1)) as sdsblt
	,cast(sdlcod as [NVARCHAR](2)) as sdlcod
	,cast(sdupc1 as [NVARCHAR](2)) as sdupc1
	,cast(sdupc2 as [NVARCHAR](2)) as sdupc2
	,cast(sdupc3 as [NVARCHAR](2)) as sdupc3
	,sdswms as sdswms
	,ltrim(rtrim(sdswms)) as sdswms_conv
	,cast(sduncd as [NVARCHAR](1)) as sduncd
	,cast(sdcrmd as [NVARCHAR](1)) as sdcrmd
	,sdcrcd as sdcrcd
	,ltrim(rtrim(sdcrcd)) as sdcrcd_conv
	,cast(sdcrr as [DECIMAL](38, 4)) as sdcrr
	,cast(sdfprc as [DECIMAL](38, 4)) as sdfprc
	,cast(sdfprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdfprc_conv
	,cast(sdfup as [DECIMAL](38, 4)) as sdfup
	,cast(sdfup as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdfup_conv
	,cast(sdfea as [DECIMAL](38, 4)) as sdfea
	,cast(sdfea as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sdfea_conv
	,cast(sdfuc as [DECIMAL](38, 4)) as sdfuc
	,cast(sdfuc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sdfuc_conv
	,cast(sdfec as [DECIMAL](38, 4)) as sdfec
	,cast(sdfec as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sdfec_conv
	,sdurcd as sdurcd
	,ltrim(rtrim(sdurcd)) as sdurcd_conv
	,cast(sdurdt as [INT]) as sdurdt
	,case when cast(sdurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdurdt as [INT]) %1000, dateadd(year, cast(sdurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdurdt_conv
	,cast(sdurat as [DECIMAL](38, 4)) as sdurat
	,cast(sdurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sdurat_conv
	,cast(sdurab as [DECIMAL](38, 4)) as sdurab
	,cast(sdurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdurab_conv
	,sdurrf as sdurrf
	,ltrim(rtrim(sdurrf)) as sdurrf_conv
	,sdtorg as sdtorg
	,ltrim(rtrim(sdtorg)) as sdtorg_conv
	,sduser as sduser
	,ltrim(rtrim(sduser)) as sduser_conv
	,sdpid as sdpid
	,ltrim(rtrim(sdpid)) as sdpid_conv
	,sdjobn as sdjobn
	,ltrim(rtrim(sdjobn)) as sdjobn_conv
	,cast(sdupmj as [INT]) as sdupmj
	,case when cast(sdupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdupmj as [INT]) %1000, dateadd(year, cast(sdupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdupmj_conv
	,cast(sdtday as [DECIMAL](38, 4)) as sdtday
	,sdso16 as sdso16
	,ltrim(rtrim(sdso16)) as sdso16_conv
	,sdso17 as sdso17
	,ltrim(rtrim(sdso17)) as sdso17_conv
	,sdso18 as sdso18
	,ltrim(rtrim(sdso18)) as sdso18_conv
	,sdso19 as sdso19
	,ltrim(rtrim(sdso19)) as sdso19_conv
	,sdso20 as sdso20
	,ltrim(rtrim(sdso20)) as sdso20_conv
	,sdir01 as sdir01
	,ltrim(rtrim(sdir01)) as sdir01_conv
	,sdir02 as sdir02
	,ltrim(rtrim(sdir02)) as sdir02_conv
	,sdir03 as sdir03
	,ltrim(rtrim(sdir03)) as sdir03_conv
	,sdir04 as sdir04
	,ltrim(rtrim(sdir04)) as sdir04_conv
	,sdir05 as sdir05
	,ltrim(rtrim(sdir05)) as sdir05_conv
	,cast(sdsoor as [INT]) as sdsoor
	,cast(sdsoor as [INT]) / cast(1 as [INT]) as sdsoor_conv
	,sdvr03 as sdvr03
	,ltrim(rtrim(sdvr03)) as sdvr03_conv
	,cast(sddeid as [DECIMAL](38, 4)) as sddeid
	,cast(sddeid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sddeid_conv
	,sdpsig as sdpsig
	,ltrim(rtrim(sdpsig)) as sdpsig_conv
	,sdrlnu as sdrlnu
	,ltrim(rtrim(sdrlnu)) as sdrlnu_conv
	,cast(sdpmdt as [DECIMAL](38, 4)) as sdpmdt
	,cast(sdrltm as [DECIMAL](38, 4)) as sdrltm
	,cast(sdrldj as [INT]) as sdrldj
	,case when cast(sdrldj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdrldj as [INT]) %1000, dateadd(year, cast(sdrldj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdrldj_conv
	,cast(sddrqt as [DECIMAL](38, 4)) as sddrqt
	,cast(sdadtm as [DECIMAL](38, 4)) as sdadtm
	,cast(sdoptt as [DECIMAL](38, 4)) as sdoptt
	,cast(sdpdtt as [DECIMAL](38, 4)) as sdpdtt
	,cast(sdpstm as [DECIMAL](38, 4)) as sdpstm
	,sdxdck as sdxdck
	,ltrim(rtrim(sdxdck)) as sdxdck_conv
	,cast(sdxpty as [DECIMAL](38, 4)) as sdxpty
	,cast(sdxpty as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdxpty_conv
	,sddual as sddual
	,ltrim(rtrim(sddual)) as sddual_conv
	,cast(sdbsc as [NVARCHAR](10)) as sdbsc
	,cast(sdcbsc as [NVARCHAR](10)) as sdcbsc
	,cast(sdcord as [DECIMAL](38, 4)) as sdcord
	,cast(sddvan as [DECIMAL](38, 4)) as sddvan
	,cast(sddvan as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sddvan_conv
	,sdpend as sdpend
	,ltrim(rtrim(sdpend)) as sdpend_conv
	,cast(sdrfrv as [NVARCHAR](3)) as sdrfrv
	,cast(sdmcln as [DECIMAL](38, 4)) as sdmcln
	,cast(sdmcln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdmcln_conv
	,cast(sdshpn as [DECIMAL](38, 4)) as sdshpn
	,cast(sdrsdt as [DECIMAL](38, 4)) as sdrsdt
	,cast(sdprjm as [DECIMAL](38, 4)) as sdprjm
	,cast(sdprjm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdprjm_conv
	,cast(sdoseq as [DECIMAL](38, 4)) as sdoseq
	,cast(sdoseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdoseq_conv
	,sdmerl as sdmerl
	,ltrim(rtrim(sdmerl)) as sdmerl_conv
	,cast(sdhold as [NVARCHAR](2)) as sdhold
	,sdhdbu as sdhdbu
	,ltrim(rtrim(sdhdbu)) as sdhdbu_conv
	,sddmbu as sddmbu
	,ltrim(rtrim(sddmbu)) as sddmbu_conv
	,sdbcrc as sdbcrc
	,ltrim(rtrim(sdbcrc)) as sdbcrc_conv
	,cast(sdodln as [DECIMAL](38, 4)) as sdodln
	,cast(sdodln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdodln_conv
	,cast(sdopdj as [INT]) as sdopdj
	,case when cast(sdopdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sdopdj as [INT]) %1000, dateadd(year, cast(sdopdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sdopdj_conv
	,sdxkco as sdxkco
	,ltrim(rtrim(sdxkco)) as sdxkco_conv
	,cast(sdxorn as [DECIMAL](38, 4)) as sdxorn
	,cast(sdxcto as [NVARCHAR](2)) as sdxcto
	,cast(sdxlln as [DECIMAL](38, 4)) as sdxlln
	,cast(sdxlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdxlln_conv
	,sdxsfx as sdxsfx
	,ltrim(rtrim(sdxsfx)) as sdxsfx_conv
	,cast(sdpoe as [NVARCHAR](6)) as sdpoe
	,sdpmto as sdpmto
	,ltrim(rtrim(sdpmto)) as sdpmto_conv
	,cast(sdanby as [DECIMAL](38, 4)) as sdanby
	,sdpmtn as sdpmtn
	,ltrim(rtrim(sdpmtn)) as sdpmtn_conv
	,cast(sdnumb as [DECIMAL](38, 4)) as sdnumb
	,cast(sdaaid as [DECIMAL](38, 4)) as sdaaid
	,sdspattn as sdspattn
	,ltrim(rtrim(sdspattn)) as sdspattn_conv
	,cast(sdpran8 as [DECIMAL](38, 4)) as sdpran8
	,cast(sdpran8 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdpran8_conv
	,cast(sdprcidln as [DECIMAL](38, 4)) as sdprcidln
	,cast(sdprcidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdprcidln_conv
	,cast(sdccidln as [DECIMAL](38, 4)) as sdccidln
	,cast(sdccidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdccidln_conv
	,cast(sdshccidln as [DECIMAL](38, 4)) as sdshccidln
	,cast(sdshccidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdshccidln_conv
	,cast(sdoppid as [DECIMAL](38, 4)) as sdoppid
	,cast(sdoppid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdoppid_conv
	,cast(sdostp as [NVARCHAR](3)) as sdostp
	,cast(sdukid as [DECIMAL](38, 4)) as sdukid
	,sdcatnm as sdcatnm
	,ltrim(rtrim(sdcatnm)) as sdcatnm_conv
	,sdalloc as sdalloc
	,ltrim(rtrim(sdalloc)) as sdalloc_conv
	,cast(sdfulpid as [INT]) as sdfulpid
	,cast(sdfulpid as [INT]) / cast(1 as [INT]) as sdfulpid_conv
	,cast(sdallsts as [NVARCHAR](30)) as sdallsts
	,cast(sdoscore as [DECIMAL](38, 4)) as sdoscore
	,cast(sdoscore as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sdoscore_conv
	,sdoscoreo as sdoscoreo
	,ltrim(rtrim(sdoscoreo)) as sdoscoreo_conv
	,sdcmco as sdcmco
	,ltrim(rtrim(sdcmco)) as sdcmco_conv
	,cast(sdkitid as [DECIMAL](38, 4)) as sdkitid
	,cast(sdkitid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdkitid_conv
	,cast(sdkitamtdom as [DECIMAL](38, 4)) as sdkitamtdom
	,cast(sdkitamtdom as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sdkitamtdom_conv
	,cast(sdkitamtfor as [DECIMAL](38, 4)) as sdkitamtfor
	,cast(sdkitamtfor as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sdkitamtfor_conv
	,sdkitdirty as sdkitdirty
	,ltrim(rtrim(sdkitdirty)) as sdkitdirty_conv
	,sdocitt as sdocitt
	,ltrim(rtrim(sdocitt)) as sdocitt_conv
	,cast(sdoccardno as [DECIMAL](38, 4)) as sdoccardno
	,cast(sdoccardno as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sdoccardno_conv
	,sdpmpn as sdpmpn
	,ltrim(rtrim(sdpmpn)) as sdpmpn_conv
	,cast(sdpns as [DECIMAL](38, 4)) as sdpns
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
	,sdkcoo
	,sddoco
	,sddcto
	,sdlnid
	,sdsfxo
	,sdmcu
	,sdco
	,sdokco
	,sdoorn
	,sdocto
	,sdogno
	,sdrkco
	,sdrorn
	,sdrcto
	,sdrlln
	,sddmct
	,sddmcs
	,sdan8
	,sdshan
	,sdpa8
	,sddrqj
	,sdtrdj
	,sdpddj
	,sdaddj
	,sdivd
	,sdcndj
	,sddgl
	,sdrsdj
	,sdpefj
	,sdppdj
	,sdvr01
	,sdvr02
	,sditm
	,sdlitm
	,sdaitm
	,sdlocn
	,sdlotn
	,sdfrgd
	,sdthgd
	,sdfrmp
	,sdthrp
	,sdexdp
	,sddsc1
	,sddsc2
	,sdlnty
	,sdnxtr
	,sdlttr
	,sdemcu
	,sdrlit
	,sdktln
	,sdcpnt
	,sdrkit
	,sdktp
	,sdsrp1
	,sdsrp2
	,sdsrp3
	,sdsrp4
	,sdsrp5
	,sdprp1
	,sdprp2
	,sdprp3
	,sdprp4
	,sdprp5
	,sduom
	,sduorg
	,sdsoqs
	,sdsobk
	,sdsocn
	,sdsone
	,sduopn
	,sdqtyt
	,sdqrlv
	,sdcomm
	,sdotqy
	,sduprc
	,sdaexp
	,sdaopn
	,sdprov
	,sdtpc
	,sdapum
	,sdlprc
	,sduncs
	,sdecst
	,sdcsto
	,sdtcst
	,sdinmg
	,sdptc
	,sdryin
	,sddtbs
	,sdtrdc
	,sdfun2
	,sdasn
	,sdprgr
	,sdclvl
	,sdcadc
	,sdkco
	,sddoc
	,sddct
	,sdodoc
	,sdodct
	,sdokc
	,sdpsn
	,sddeln
	,sdtax1
	,sdtxa1
	,sdexr1
	,sdatxt
	,sdprio
	,sdresl
	,sdback
	,sdsbal
	,sdapts
	,sdlob
	,sdeuse
	,sddtys
	,sdntr
	,sdvend
	,sdcars
	,sdmot
	,sdrout
	,sdstop
	,sdzon
	,sdcnid
	,sdfrth
	,sdshcm
	,sdshcn
	,sdsern
	,sduom1
	,sdpqor
	,sduom2
	,sdsqor
	,sduom4
	,sditwt
	,sdwtum
	,sditvl
	,sdvlum
	,sdrprc
	,sdorpr
	,sdorp
	,sdcmgp
	,sdglc
	,sdctry
	,sdfy
	,sdso01
	,sdso02
	,sdso03
	,sdso04
	,sdso05
	,sdso06
	,sdso07
	,sdso08
	,sdso09
	,sdso10
	,sdso11
	,sdso12
	,sdso13
	,sdso14
	,sdso15
	,sdacom
	,sdcmcg
	,sdrcd
	,sdgrwt
	,sdgwum
	,sdsbl
	,sdsblt
	,sdlcod
	,sdupc1
	,sdupc2
	,sdupc3
	,sdswms
	,sduncd
	,sdcrmd
	,sdcrcd
	,sdcrr
	,sdfprc
	,sdfup
	,sdfea
	,sdfuc
	,sdfec
	,sdurcd
	,sdurdt
	,sdurat
	,sdurab
	,sdurrf
	,sdtorg
	,sduser
	,sdpid
	,sdjobn
	,sdupmj
	,sdtday
	,sdso16
	,sdso17
	,sdso18
	,sdso19
	,sdso20
	,sdir01
	,sdir02
	,sdir03
	,sdir04
	,sdir05
	,sdsoor
	,sdvr03
	,sddeid
	,sdpsig
	,sdrlnu
	,sdpmdt
	,sdrltm
	,sdrldj
	,sddrqt
	,sdadtm
	,sdoptt
	,sdpdtt
	,sdpstm
	,sdxdck
	,sdxpty
	,sddual
	,sdbsc
	,sdcbsc
	,sdcord
	,sddvan
	,sdpend
	,sdrfrv
	,sdmcln
	,sdshpn
	,sdrsdt
	,sdprjm
	,sdoseq
	,sdmerl
	,sdhold
	,sdhdbu
	,sddmbu
	,sdbcrc
	,sdodln
	,sdopdj
	,sdxkco
	,sdxorn
	,sdxcto
	,sdxlln
	,sdxsfx
	,sdpoe
	,sdpmto
	,sdanby
	,sdpmtn
	,sdnumb
	,sdaaid
	,sdspattn
	,sdpran8
	,sdprcidln
	,sdccidln
	,sdshccidln
	,sdoppid
	,sdostp
	,sdukid
	,sdcatnm
	,sdalloc
	,sdfulpid
	,sdallsts
	,sdoscore
	,sdoscoreo
	,sdcmco
	,sdkitid
	,sdkitamtdom
	,sdkitamtfor
	,sdkitdirty
	,sdocitt
	,sdoccardno
	,sdpmpn
	,sdpns,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4211_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4211_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4211_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4211_cdc(sys_integration_id); 
