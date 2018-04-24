-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f03012_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f03012_new


CREATE TABLE rep_jde_qat.f03012_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
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
	,cast(aian8 as [DECIMAL](38, 4)) as aian8
	,aico as aico
	,ltrim(rtrim(aico)) as aico_conv
	,aiarc as aiarc
	,ltrim(rtrim(aiarc)) as aiarc_conv
	,aimcur as aimcur
	,ltrim(rtrim(aimcur)) as aimcur_conv
	,aiobar as aiobar
	,ltrim(rtrim(aiobar)) as aiobar_conv
	,aiaidr as aiaidr
	,ltrim(rtrim(aiaidr)) as aiaidr_conv
	,aikcor as aikcor
	,ltrim(rtrim(aikcor)) as aikcor_conv
	,cast(aidcar as [DECIMAL](38, 4)) as aidcar
	,cast(aidtar as [NVARCHAR](2)) as aidtar
	,aicrcd as aicrcd
	,ltrim(rtrim(aicrcd)) as aicrcd_conv
	,aitxa1 as aitxa1
	,ltrim(rtrim(aitxa1)) as aitxa1_conv
	,cast(aiexr1 as [NVARCHAR](2)) as aiexr1
	,cast(aiacl as [DECIMAL](38, 4)) as aiacl
	,cast(aihdar as [NVARCHAR](1)) as aihdar
	,aitrar as aitrar
	,ltrim(rtrim(aitrar)) as aitrar_conv
	,cast(aistto as [NVARCHAR](1)) as aistto
	,cast(airyin as [NVARCHAR](1)) as airyin
	,cast(aistmt as [NVARCHAR](1)) as aistmt
	,cast(aiarpy as [DECIMAL](38, 4)) as aiarpy
	,aiatcs as aiatcs
	,ltrim(rtrim(aiatcs)) as aiatcs_conv
	,cast(aisito as [NVARCHAR](1)) as aisito
	,cast(aisqnl as [NVARCHAR](1)) as aisqnl
	,cast(aialgm as [NVARCHAR](2)) as aialgm
	,aicycn as aicycn
	,ltrim(rtrim(aicycn)) as aicycn_conv
	,cast(aibo as [NVARCHAR](1)) as aibo
	,cast(aitsta as [NVARCHAR](2)) as aitsta
	,aickhc as aickhc
	,ltrim(rtrim(aickhc)) as aickhc_conv
	,cast(aidlc as [INT]) as aidlc
	,case when cast(aidlc as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aidlc as [INT]) %1000, dateadd(year, cast(aidlc as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aidlc_conv
	,aidnlt as aidnlt
	,ltrim(rtrim(aidnlt)) as aidnlt_conv
	,aiplcr as aiplcr
	,ltrim(rtrim(aiplcr)) as aiplcr_conv
	,cast(airvdj as [INT]) as airvdj
	,case when cast(airvdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(airvdj as [INT]) %1000, dateadd(year, cast(airvdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as airvdj_conv
	,cast(aidso as [DECIMAL](38, 4)) as aidso
	,cast(aicmgr as [NVARCHAR](10)) as aicmgr
	,cast(aiclmg as [NVARCHAR](10)) as aiclmg
	,cast(aidlqt as [DECIMAL](38, 4)) as aidlqt
	,cast(aidlqj as [INT]) as aidlqj
	,case when cast(aidlqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aidlqj as [INT]) %1000, dateadd(year, cast(aidlqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aidlqj_conv
	,cast(ainbrr as [NVARCHAR](1)) as ainbrr
	,cast(aicoll as [NVARCHAR](1)) as aicoll
	,cast(ainbr1 as [DECIMAL](38, 4)) as ainbr1
	,cast(ainbr2 as [DECIMAL](38, 4)) as ainbr2
	,cast(ainbr3 as [DECIMAL](38, 4)) as ainbr3
	,cast(ainbcl as [DECIMAL](38, 4)) as ainbcl
	,cast(aiafc as [NVARCHAR](1)) as aiafc
	,cast(aifd as [DECIMAL](38, 4)) as aifd
	,cast(aifp as [DECIMAL](38, 4)) as aifp
	,cast(aifp as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as aifp_conv
	,aicfce as aicfce
	,ltrim(rtrim(aicfce)) as aicfce_conv
	,aiab2 as aiab2
	,ltrim(rtrim(aiab2)) as aiab2_conv
	,cast(aidt1j as [INT]) as aidt1j
	,case when cast(aidt1j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aidt1j as [INT]) %1000, dateadd(year, cast(aidt1j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aidt1j_conv
	,cast(aidfij as [INT]) as aidfij
	,case when cast(aidfij as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aidfij as [INT]) %1000, dateadd(year, cast(aidfij as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aidfij_conv
	,cast(aidlij as [INT]) as aidlij
	,case when cast(aidlij as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aidlij as [INT]) %1000, dateadd(year, cast(aidlij as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aidlij_conv
	,cast(aiabc1 as [NVARCHAR](1)) as aiabc1
	,cast(aiabc2 as [NVARCHAR](1)) as aiabc2
	,cast(aiabc3 as [NVARCHAR](1)) as aiabc3
	,cast(aifndj as [INT]) as aifndj
	,case when cast(aifndj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aifndj as [INT]) %1000, dateadd(year, cast(aifndj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aifndj_conv
	,cast(aidlp as [INT]) as aidlp
	,case when cast(aidlp as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aidlp as [INT]) %1000, dateadd(year, cast(aidlp as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aidlp_conv
	,cast(aidb as [NVARCHAR](3)) as aidb
	,cast(aidnbj as [INT]) as aidnbj
	,case when cast(aidnbj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aidnbj as [INT]) %1000, dateadd(year, cast(aidnbj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aidnbj_conv
	,cast(aitrw as [NVARCHAR](3)) as aitrw
	,cast(aitwdj as [INT]) as aitwdj
	,case when cast(aitwdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aitwdj as [INT]) %1000, dateadd(year, cast(aitwdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aitwdj_conv
	,cast(aiavd as [DECIMAL](38, 4)) as aiavd
	,aicrca as aicrca
	,ltrim(rtrim(aicrca)) as aicrca_conv
	,cast(aiad as [DECIMAL](38, 4)) as aiad
	,cast(aiad as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiad_conv
	,cast(aiafcp as [DECIMAL](38, 4)) as aiafcp
	,cast(aiafcp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiafcp_conv
	,cast(aiafcy as [DECIMAL](38, 4)) as aiafcy
	,cast(aiafcy as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiafcy_conv
	,cast(aiasty as [DECIMAL](38, 4)) as aiasty
	,cast(aiasty as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiasty_conv
	,cast(aispye as [DECIMAL](38, 4)) as aispye
	,cast(aispye as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aispye_conv
	,cast(aiahb as [DECIMAL](38, 4)) as aiahb
	,cast(aiahb as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiahb_conv
	,cast(aialp as [DECIMAL](38, 4)) as aialp
	,cast(aialp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aialp_conv
	,cast(aiabam as [DECIMAL](38, 4)) as aiabam
	,cast(aiabam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiabam_conv
	,cast(aiaba1 as [DECIMAL](38, 4)) as aiaba1
	,cast(aiaba1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiaba1_conv
	,cast(aiaprc as [DECIMAL](38, 4)) as aiaprc
	,cast(aiaprc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiaprc_conv
	,cast(aimaxo as [DECIMAL](38, 4)) as aimaxo
	,cast(aimaxo as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aimaxo_conv
	,cast(aimino as [DECIMAL](38, 4)) as aimino
	,cast(aimino as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aimino_conv
	,cast(aioytd as [DECIMAL](38, 4)) as aioytd
	,cast(aiopy as [DECIMAL](38, 4)) as aiopy
	,aipopn as aipopn
	,ltrim(rtrim(aipopn)) as aipopn_conv
	,cast(aidaoj as [INT]) as aidaoj
	,case when cast(aidaoj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aidaoj as [INT]) %1000, dateadd(year, cast(aidaoj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aidaoj_conv
	,cast(aian8r as [DECIMAL](38, 4)) as aian8r
	,cast(aibadt as [NVARCHAR](1)) as aibadt
	,cast(aicpgp as [NVARCHAR](8)) as aicpgp
	,cast(aiortp as [NVARCHAR](8)) as aiortp
	,cast(aitrdc as [DECIMAL](38, 4)) as aitrdc
	,cast(aitrdc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as aitrdc_conv
	,cast(aiinmg as [NVARCHAR](10)) as aiinmg
	,aiexhd as aiexhd
	,ltrim(rtrim(aiexhd)) as aiexhd_conv
	,cast(aihold as [NVARCHAR](2)) as aihold
	,cast(airout as [NVARCHAR](3)) as airout
	,cast(aistop as [NVARCHAR](3)) as aistop
	,cast(aizon as [NVARCHAR](3)) as aizon
	,cast(aicars as [DECIMAL](38, 4)) as aicars
	,cast(aicars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aicars_conv
	,aidel1 as aidel1
	,ltrim(rtrim(aidel1)) as aidel1_conv
	,aidel2 as aidel2
	,ltrim(rtrim(aidel2)) as aidel2_conv
	,cast(ailtdt as [DECIMAL](38, 4)) as ailtdt
	,cast(ailtdt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ailtdt_conv
	,cast(aifrth as [NVARCHAR](3)) as aifrth
	,aiaft as aiaft
	,ltrim(rtrim(aiaft)) as aiaft_conv
	,aiapts as aiapts
	,ltrim(rtrim(aiapts)) as aiapts_conv
	,aisbal as aisbal
	,ltrim(rtrim(aisbal)) as aisbal_conv
	,aiback as aiback
	,ltrim(rtrim(aiback)) as aiback_conv
	,aiporq as aiporq
	,ltrim(rtrim(aiporq)) as aiporq_conv
	,cast(aiprio as [NVARCHAR](1)) as aiprio
	,cast(aiarto as [NVARCHAR](1)) as aiarto
	,cast(aiinvc as [DECIMAL](38, 4)) as aiinvc
	,cast(aiinvc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aiinvc_conv
	,aiicon as aiicon
	,ltrim(rtrim(aiicon)) as aiicon_conv
	,cast(aiblfr as [NVARCHAR](1)) as aiblfr
	,cast(ainivd as [INT]) as ainivd
	,case when cast(ainivd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ainivd as [INT]) %1000, dateadd(year, cast(ainivd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ainivd_conv
	,cast(ailedj as [INT]) as ailedj
	,case when cast(ailedj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ailedj as [INT]) %1000, dateadd(year, cast(ailedj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ailedj_conv
	,aiplst as aiplst
	,ltrim(rtrim(aiplst)) as aiplst_conv
	,aimord as aimord
	,ltrim(rtrim(aimord)) as aimord_conv
	,cast(aicmc1 as [DECIMAL](38, 4)) as aicmc1
	,cast(aicmc1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aicmc1_conv
	,cast(aicmr1 as [DECIMAL](38, 4)) as aicmr1
	,cast(aicmr1 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as aicmr1_conv
	,cast(aicmc2 as [DECIMAL](38, 4)) as aicmc2
	,cast(aicmc2 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aicmc2_conv
	,cast(aicmr2 as [DECIMAL](38, 4)) as aicmr2
	,cast(aicmr2 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as aicmr2_conv
	,aipalc as aipalc
	,ltrim(rtrim(aipalc)) as aipalc_conv
	,cast(aivumd as [NVARCHAR](2)) as aivumd
	,cast(aiwumd as [NVARCHAR](2)) as aiwumd
	,cast(aiedpm as [NVARCHAR](1)) as aiedpm
	,cast(aiedii as [NVARCHAR](1)) as aiedii
	,cast(aiedci as [NVARCHAR](1)) as aiedci
	,cast(aiedqd as [DECIMAL](38, 4)) as aiedqd
	,cast(aiedad as [DECIMAL](38, 4)) as aiedad
	,aiedf1 as aiedf1
	,ltrim(rtrim(aiedf1)) as aiedf1_conv
	,cast(aiedf2 as [NVARCHAR](1)) as aiedf2
	,aisi01 as aisi01
	,ltrim(rtrim(aisi01)) as aisi01_conv
	,aisi02 as aisi02
	,ltrim(rtrim(aisi02)) as aisi02_conv
	,aisi03 as aisi03
	,ltrim(rtrim(aisi03)) as aisi03_conv
	,aisi04 as aisi04
	,ltrim(rtrim(aisi04)) as aisi04_conv
	,aisi05 as aisi05
	,ltrim(rtrim(aisi05)) as aisi05_conv
	,aiurcd as aiurcd
	,ltrim(rtrim(aiurcd)) as aiurcd_conv
	,cast(aiurat as [DECIMAL](38, 4)) as aiurat
	,cast(aiurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aiurat_conv
	,cast(aiurab as [DECIMAL](38, 4)) as aiurab
	,cast(aiurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aiurab_conv
	,cast(aiurdt as [INT]) as aiurdt
	,case when cast(aiurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aiurdt as [INT]) %1000, dateadd(year, cast(aiurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aiurdt_conv
	,aiurrf as aiurrf
	,ltrim(rtrim(aiurrf)) as aiurrf_conv
	,cast(aicp01 as [NVARCHAR](1)) as aicp01
	,cast(aiasn as [NVARCHAR](8)) as aiasn
	,aidspa as aidspa
	,ltrim(rtrim(aidspa)) as aidspa_conv
	,cast(aicrmd as [NVARCHAR](1)) as aicrmd
	,cast(aiply as [DECIMAL](38, 4)) as aiply
	,cast(aiman8 as [DECIMAL](38, 4)) as aiman8
	,aiarl as aiarl
	,ltrim(rtrim(aiarl)) as aiarl_conv
	,cast(aiamcr as [DECIMAL](38, 4)) as aiamcr
	,cast(aiac01 as [NVARCHAR](3)) as aiac01
	,cast(aiac02 as [NVARCHAR](3)) as aiac02
	,cast(aiac03 as [NVARCHAR](3)) as aiac03
	,cast(aiac04 as [NVARCHAR](3)) as aiac04
	,cast(aiac05 as [NVARCHAR](3)) as aiac05
	,cast(aiac06 as [NVARCHAR](3)) as aiac06
	,cast(aiac07 as [NVARCHAR](3)) as aiac07
	,cast(aiac08 as [NVARCHAR](3)) as aiac08
	,cast(aiac09 as [NVARCHAR](3)) as aiac09
	,cast(aiac10 as [NVARCHAR](3)) as aiac10
	,cast(aiac11 as [NVARCHAR](3)) as aiac11
	,cast(aiac12 as [NVARCHAR](3)) as aiac12
	,cast(aiac13 as [NVARCHAR](3)) as aiac13
	,cast(aiac14 as [NVARCHAR](3)) as aiac14
	,cast(aiac15 as [NVARCHAR](3)) as aiac15
	,cast(aiac16 as [NVARCHAR](3)) as aiac16
	,cast(aiac17 as [NVARCHAR](3)) as aiac17
	,cast(aiac18 as [NVARCHAR](3)) as aiac18
	,cast(aiac19 as [NVARCHAR](3)) as aiac19
	,cast(aiac20 as [NVARCHAR](3)) as aiac20
	,cast(aiac21 as [NVARCHAR](3)) as aiac21
	,cast(aiac22 as [NVARCHAR](3)) as aiac22
	,cast(aiac23 as [NVARCHAR](3)) as aiac23
	,cast(aiac24 as [NVARCHAR](3)) as aiac24
	,cast(aiac25 as [NVARCHAR](3)) as aiac25
	,cast(aiac26 as [NVARCHAR](3)) as aiac26
	,cast(aiac27 as [NVARCHAR](3)) as aiac27
	,cast(aiac28 as [NVARCHAR](3)) as aiac28
	,cast(aiac29 as [NVARCHAR](3)) as aiac29
	,cast(aiac30 as [NVARCHAR](3)) as aiac30
	,aislpg as aislpg
	,ltrim(rtrim(aislpg)) as aislpg_conv
	,aisldw as aisldw
	,ltrim(rtrim(aisldw)) as aisldw_conv
	,aicfpp as aicfpp
	,ltrim(rtrim(aicfpp)) as aicfpp_conv
	,aicfsp as aicfsp
	,ltrim(rtrim(aicfsp)) as aicfsp_conv
	,aicfdf as aicfdf
	,ltrim(rtrim(aicfdf)) as aicfdf_conv
	,airq01 as airq01
	,ltrim(rtrim(airq01)) as airq01_conv
	,airq02 as airq02
	,ltrim(rtrim(airq02)) as airq02_conv
	,cast(aidr03 as [NVARCHAR](2)) as aidr03
	,cast(aidr04 as [NVARCHAR](2)) as aidr04
	,airq03 as airq03
	,ltrim(rtrim(airq03)) as airq03_conv
	,airq04 as airq04
	,ltrim(rtrim(airq04)) as airq04_conv
	,airq05 as airq05
	,ltrim(rtrim(airq05)) as airq05_conv
	,airq06 as airq06
	,ltrim(rtrim(airq06)) as airq06_conv
	,airq07 as airq07
	,ltrim(rtrim(airq07)) as airq07_conv
	,airq08 as airq08
	,ltrim(rtrim(airq08)) as airq08_conv
	,cast(aidr08 as [NVARCHAR](2)) as aidr08
	,cast(aidr09 as [NVARCHAR](2)) as aidr09
	,airq09 as airq09
	,ltrim(rtrim(airq09)) as airq09_conv
	,aiuser as aiuser
	,ltrim(rtrim(aiuser)) as aiuser_conv
	,aipid as aipid
	,ltrim(rtrim(aipid)) as aipid_conv
	,cast(aiupmj as [INT]) as aiupmj
	,case when cast(aiupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aiupmj as [INT]) %1000, dateadd(year, cast(aiupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aiupmj_conv
	,cast(aiupmt as [DECIMAL](38, 4)) as aiupmt
	,aijobn as aijobn
	,ltrim(rtrim(aijobn)) as aijobn_conv
	,aiprgf as aiprgf
	,ltrim(rtrim(aiprgf)) as aiprgf_conv
	,aibyal as aibyal
	,ltrim(rtrim(aibyal)) as aibyal_conv
	,cast(aibsc as [NVARCHAR](10)) as aibsc
	,aiashl as aiashl
	,ltrim(rtrim(aiashl)) as aiashl_conv
	,aiprsn as aiprsn
	,ltrim(rtrim(aiprsn)) as aiprsn_conv
	,cast(aiopbo as [NVARCHAR](30)) as aiopbo
	,aiapsb as aiapsb
	,ltrim(rtrim(aiapsb)) as aiapsb_conv
	,cast(aitier1 as [NVARCHAR](5)) as aitier1
	,cast(aipwpcp as [DECIMAL](38, 4)) as aipwpcp
	,cast(aipwpcp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aipwpcp_conv
	,cast(aicusts as [NVARCHAR](1)) as aicusts
	,aistof as aistof
	,ltrim(rtrim(aistof)) as aistof_conv
	,cast(aiterrid as [DECIMAL](38, 4)) as aiterrid
	,cast(aiterrid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aiterrid_conv
	,cast(aicig as [DECIMAL](38, 4)) as aicig
	,cast(aicig as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aicig_conv
	,aitorg as aitorg
	,ltrim(rtrim(aitorg)) as aitorg_conv
	,aidtee as aidtee
	,aidtee as aidtee_conv
	,cast(aisyncs as [DECIMAL](38, 4)) as aisyncs
	,cast(aicaad as [DECIMAL](38, 4)) as aicaad
	,cast(aigopasf as [NVARCHAR](1)) as aigopasf
FROM 
    stg_jde_qat.tmp_f03012_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f03012_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f03012_sys_integration_id ON rep_jde_qat.f03012_new(sys_integration_id); 
