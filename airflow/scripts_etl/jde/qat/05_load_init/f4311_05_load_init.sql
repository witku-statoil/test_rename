-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4311_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4311_new


CREATE TABLE rep_jde_qat.f4311_new
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
	,pdkcoo as pdkcoo
	,ltrim(rtrim(pdkcoo)) as pdkcoo_conv
	,cast(pddoco as [DECIMAL](38, 4)) as pddoco
	,cast(pddcto as [NVARCHAR](2)) as pddcto
	,pdsfxo as pdsfxo
	,ltrim(rtrim(pdsfxo)) as pdsfxo_conv
	,cast(pdlnid as [DECIMAL](38, 4)) as pdlnid
	,cast(pdlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pdlnid_conv
	,pdmcu as pdmcu
	,ltrim(rtrim(pdmcu)) as pdmcu_conv
	,pdco as pdco
	,ltrim(rtrim(pdco)) as pdco_conv
	,pdokco as pdokco
	,ltrim(rtrim(pdokco)) as pdokco_conv
	,pdoorn as pdoorn
	,ltrim(rtrim(pdoorn)) as pdoorn_conv
	,cast(pdocto as [NVARCHAR](2)) as pdocto
	,cast(pdogno as [DECIMAL](38, 4)) as pdogno
	,cast(pdogno as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pdogno_conv
	,pdrkco as pdrkco
	,ltrim(rtrim(pdrkco)) as pdrkco_conv
	,pdrorn as pdrorn
	,ltrim(rtrim(pdrorn)) as pdrorn_conv
	,cast(pdrcto as [NVARCHAR](2)) as pdrcto
	,cast(pdrlln as [DECIMAL](38, 4)) as pdrlln
	,cast(pdrlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pdrlln_conv
	,pddmct as pddmct
	,ltrim(rtrim(pddmct)) as pddmct_conv
	,cast(pddmcs as [DECIMAL](38, 4)) as pddmcs
	,pdbalu as pdbalu
	,ltrim(rtrim(pdbalu)) as pdbalu_conv
	,cast(pdan8 as [DECIMAL](38, 4)) as pdan8
	,cast(pdshan as [DECIMAL](38, 4)) as pdshan
	,cast(pddrqj as [INT]) as pddrqj
	,case when cast(pddrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pddrqj as [INT]) %1000, dateadd(year, cast(pddrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pddrqj_conv
	,cast(pdtrdj as [INT]) as pdtrdj
	,case when cast(pdtrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdtrdj as [INT]) %1000, dateadd(year, cast(pdtrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdtrdj_conv
	,cast(pdpddj as [INT]) as pdpddj
	,case when cast(pdpddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdpddj as [INT]) %1000, dateadd(year, cast(pdpddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdpddj_conv
	,cast(pdopdj as [INT]) as pdopdj
	,case when cast(pdopdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdopdj as [INT]) %1000, dateadd(year, cast(pdopdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdopdj_conv
	,cast(pdaddj as [INT]) as pdaddj
	,case when cast(pdaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdaddj as [INT]) %1000, dateadd(year, cast(pdaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdaddj_conv
	,cast(pdcndj as [INT]) as pdcndj
	,case when cast(pdcndj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdcndj as [INT]) %1000, dateadd(year, cast(pdcndj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdcndj_conv
	,cast(pdpefj as [INT]) as pdpefj
	,case when cast(pdpefj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdpefj as [INT]) %1000, dateadd(year, cast(pdpefj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdpefj_conv
	,cast(pdppdj as [INT]) as pdppdj
	,case when cast(pdppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdppdj as [INT]) %1000, dateadd(year, cast(pdppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdppdj_conv
	,cast(pdpsdj as [INT]) as pdpsdj
	,case when cast(pdpsdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdpsdj as [INT]) %1000, dateadd(year, cast(pdpsdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdpsdj_conv
	,cast(pddsvj as [INT]) as pddsvj
	,case when cast(pddsvj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pddsvj as [INT]) %1000, dateadd(year, cast(pddsvj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pddsvj_conv
	,cast(pddgl as [INT]) as pddgl
	,case when cast(pddgl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pddgl as [INT]) %1000, dateadd(year, cast(pddgl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pddgl_conv
	,cast(pdpn as [DECIMAL](38, 4)) as pdpn
	,pdvr01 as pdvr01
	,ltrim(rtrim(pdvr01)) as pdvr01_conv
	,pdvr02 as pdvr02
	,ltrim(rtrim(pdvr02)) as pdvr02_conv
	,cast(pditm as [DECIMAL](38, 4)) as pditm
	,pdlitm as pdlitm
	,ltrim(rtrim(pdlitm)) as pdlitm_conv
	,pdaitm as pdaitm
	,ltrim(rtrim(pdaitm)) as pdaitm_conv
	,pdlocn as pdlocn
	,ltrim(rtrim(pdlocn)) as pdlocn_conv
	,pdlotn as pdlotn
	,ltrim(rtrim(pdlotn)) as pdlotn_conv
	,cast(pdfrgd as [NVARCHAR](3)) as pdfrgd
	,cast(pdthgd as [NVARCHAR](3)) as pdthgd
	,cast(pdfrmp as [DECIMAL](38, 4)) as pdfrmp
	,cast(pdfrmp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pdfrmp_conv
	,cast(pdthrp as [DECIMAL](38, 4)) as pdthrp
	,cast(pdthrp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pdthrp_conv
	,pddsc1 as pddsc1
	,ltrim(rtrim(pddsc1)) as pddsc1_conv
	,pddsc2 as pddsc2
	,ltrim(rtrim(pddsc2)) as pddsc2_conv
	,pdlnty as pdlnty
	,ltrim(rtrim(pdlnty)) as pdlnty_conv
	,cast(pdnxtr as [NVARCHAR](3)) as pdnxtr
	,cast(pdlttr as [NVARCHAR](3)) as pdlttr
	,pdrlit as pdrlit
	,ltrim(rtrim(pdrlit)) as pdrlit_conv
	,pdpds1 as pdpds1
	,ltrim(rtrim(pdpds1)) as pdpds1_conv
	,pdpds2 as pdpds2
	,ltrim(rtrim(pdpds2)) as pdpds2_conv
	,pdpds3 as pdpds3
	,ltrim(rtrim(pdpds3)) as pdpds3_conv
	,pdpds4 as pdpds4
	,ltrim(rtrim(pdpds4)) as pdpds4_conv
	,pdpds5 as pdpds5
	,ltrim(rtrim(pdpds5)) as pdpds5_conv
	,cast(pdpdp1 as [NVARCHAR](3)) as pdpdp1
	,cast(pdpdp2 as [NVARCHAR](3)) as pdpdp2
	,cast(pdpdp3 as [NVARCHAR](3)) as pdpdp3
	,cast(pdpdp4 as [NVARCHAR](3)) as pdpdp4
	,cast(pdpdp5 as [NVARCHAR](3)) as pdpdp5
	,cast(pduom as [NVARCHAR](2)) as pduom
	,cast(pduorg as [DECIMAL](38, 4)) as pduorg
	,cast(pduorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pduorg_conv
	,cast(pduchg as [DECIMAL](38, 4)) as pduchg
	,cast(pduchg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pduchg_conv
	,cast(pduopn as [DECIMAL](38, 4)) as pduopn
	,cast(pduopn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pduopn_conv
	,cast(pdurec as [DECIMAL](38, 4)) as pdurec
	,cast(pdurec as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdurec_conv
	,cast(pdcrec as [DECIMAL](38, 4)) as pdcrec
	,cast(pdcrec as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdcrec_conv
	,cast(pdurlv as [DECIMAL](38, 4)) as pdurlv
	,cast(pdurlv as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdurlv_conv
	,cast(pdotqy as [NVARCHAR](1)) as pdotqy
	,cast(pdprrc as [DECIMAL](38, 4)) as pdprrc
	,cast(pdprrc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdprrc_conv
	,cast(pdaexp as [DECIMAL](38, 4)) as pdaexp
	,cast(pdaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdaexp_conv
	,cast(pdachg as [DECIMAL](38, 4)) as pdachg
	,cast(pdachg as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdachg_conv
	,cast(pdaopn as [DECIMAL](38, 4)) as pdaopn
	,cast(pdaopn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdaopn_conv
	,cast(pdarec as [DECIMAL](38, 4)) as pdarec
	,cast(pdarec as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdarec_conv
	,cast(pdarlv as [DECIMAL](38, 4)) as pdarlv
	,cast(pdarlv as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdarlv_conv
	,cast(pdftn1 as [DECIMAL](38, 4)) as pdftn1
	,cast(pdftn1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdftn1_conv
	,cast(pdtrlv as [DECIMAL](38, 4)) as pdtrlv
	,cast(pdtrlv as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdtrlv_conv
	,pdprov as pdprov
	,ltrim(rtrim(pdprov)) as pdprov_conv
	,cast(pdamc3 as [DECIMAL](38, 4)) as pdamc3
	,cast(pdamc3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdamc3_conv
	,cast(pdecst as [DECIMAL](38, 4)) as pdecst
	,cast(pdecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdecst_conv
	,pdcsto as pdcsto
	,ltrim(rtrim(pdcsto)) as pdcsto_conv
	,pdcsmp as pdcsmp
	,ltrim(rtrim(pdcsmp)) as pdcsmp_conv
	,cast(pdinmg as [NVARCHAR](10)) as pdinmg
	,cast(pdasn as [NVARCHAR](8)) as pdasn
	,cast(pdprgr as [NVARCHAR](8)) as pdprgr
	,pdclvl as pdclvl
	,ltrim(rtrim(pdclvl)) as pdclvl_conv
	,cast(pdcatn as [NVARCHAR](8)) as pdcatn
	,cast(pddspr as [DECIMAL](38, 4)) as pddspr
	,cast(pddspr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pddspr_conv
	,pdptc as pdptc
	,ltrim(rtrim(pdptc)) as pdptc_conv
	,cast(pdtx as [NVARCHAR](1)) as pdtx
	,cast(pdexr1 as [NVARCHAR](2)) as pdexr1
	,pdtxa1 as pdtxa1
	,ltrim(rtrim(pdtxa1)) as pdtxa1_conv
	,pdatxt as pdatxt
	,ltrim(rtrim(pdatxt)) as pdatxt_conv
	,pdcnid as pdcnid
	,ltrim(rtrim(pdcnid)) as pdcnid_conv
	,pdcdcd as pdcdcd
	,ltrim(rtrim(pdcdcd)) as pdcdcd_conv
	,cast(pdntr as [NVARCHAR](2)) as pdntr
	,cast(pdfrth as [NVARCHAR](3)) as pdfrth
	,pdfrtc as pdfrtc
	,ltrim(rtrim(pdfrtc)) as pdfrtc_conv
	,cast(pdzon as [NVARCHAR](3)) as pdzon
	,cast(pdfrat as [NVARCHAR](10)) as pdfrat
	,pdratt as pdratt
	,ltrim(rtrim(pdratt)) as pdratt_conv
	,cast(pdanby as [DECIMAL](38, 4)) as pdanby
	,cast(pdancr as [DECIMAL](38, 4)) as pdancr
	,cast(pdmot as [NVARCHAR](3)) as pdmot
	,cast(pdcot as [NVARCHAR](3)) as pdcot
	,cast(pdshcm as [NVARCHAR](3)) as pdshcm
	,cast(pdshcn as [NVARCHAR](3)) as pdshcn
	,cast(pduom1 as [NVARCHAR](2)) as pduom1
	,cast(pdpqor as [DECIMAL](38, 4)) as pdpqor
	,cast(pdpqor as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdpqor_conv
	,cast(pduom2 as [NVARCHAR](2)) as pduom2
	,cast(pdsqor as [DECIMAL](38, 4)) as pdsqor
	,cast(pdsqor as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdsqor_conv
	,cast(pduom3 as [NVARCHAR](2)) as pduom3
	,cast(pditwt as [DECIMAL](38, 4)) as pditwt
	,cast(pditwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pditwt_conv
	,cast(pdwtum as [NVARCHAR](2)) as pdwtum
	,cast(pditvl as [DECIMAL](38, 4)) as pditvl
	,cast(pditvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pditvl_conv
	,cast(pdvlum as [NVARCHAR](2)) as pdvlum
	,pdglc as pdglc
	,ltrim(rtrim(pdglc)) as pdglc_conv
	,cast(pdctry as [DECIMAL](38, 4)) as pdctry
	,cast(pdfy as [DECIMAL](38, 4)) as pdfy
	,cast(pdfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pdfy_conv
	,pdstts as pdstts
	,ltrim(rtrim(pdstts)) as pdstts_conv
	,cast(pdrcd as [NVARCHAR](3)) as pdrcd
	,cast(pdfuf1 as [NVARCHAR](1)) as pdfuf1
	,pdfuf2 as pdfuf2
	,ltrim(rtrim(pdfuf2)) as pdfuf2_conv
	,cast(pdgrwt as [DECIMAL](38, 4)) as pdgrwt
	,cast(pdgrwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdgrwt_conv
	,cast(pdgwum as [NVARCHAR](2)) as pdgwum
	,cast(pdlt as [NVARCHAR](2)) as pdlt
	,pdani as pdani
	,ltrim(rtrim(pdani)) as pdani_conv
	,pdaid as pdaid
	,ltrim(rtrim(pdaid)) as pdaid_conv
	,pdomcu as pdomcu
	,ltrim(rtrim(pdomcu)) as pdomcu_conv
	,pdobj as pdobj
	,ltrim(rtrim(pdobj)) as pdobj_conv
	,pdsub as pdsub
	,ltrim(rtrim(pdsub)) as pdsub_conv
	,cast(pdsblt as [NVARCHAR](1)) as pdsblt
	,pdsbl as pdsbl
	,ltrim(rtrim(pdsbl)) as pdsbl_conv
	,pdasid as pdasid
	,ltrim(rtrim(pdasid)) as pdasid_conv
	,cast(pdccmp as [DECIMAL](38, 4)) as pdccmp
	,cast(pdccmp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pdccmp_conv
	,pdtag as pdtag
	,ltrim(rtrim(pdtag)) as pdtag_conv
	,cast(pdwr01 as [NVARCHAR](4)) as pdwr01
	,pdpl as pdpl
	,ltrim(rtrim(pdpl)) as pdpl_conv
	,pdelev as pdelev
	,ltrim(rtrim(pdelev)) as pdelev_conv
	,cast(pdr001 as [NVARCHAR](3)) as pdr001
	,pdrtnr as pdrtnr
	,ltrim(rtrim(pdrtnr)) as pdrtnr_conv
	,cast(pdlcod as [NVARCHAR](2)) as pdlcod
	,pdpurg as pdpurg
	,ltrim(rtrim(pdpurg)) as pdpurg_conv
	,cast(pdprom as [NVARCHAR](1)) as pdprom
	,pdfnlp as pdfnlp
	,ltrim(rtrim(pdfnlp)) as pdfnlp_conv
	,cast(pdavch as [NVARCHAR](1)) as pdavch
	,pdprpy as pdprpy
	,ltrim(rtrim(pdprpy)) as pdprpy_conv
	,cast(pduncd as [NVARCHAR](1)) as pduncd
	,cast(pdmaty as [NVARCHAR](1)) as pdmaty
	,pdrtgc as pdrtgc
	,ltrim(rtrim(pdrtgc)) as pdrtgc_conv
	,pdrcpf as pdrcpf
	,ltrim(rtrim(pdrcpf)) as pdrcpf_conv
	,pdps01 as pdps01
	,ltrim(rtrim(pdps01)) as pdps01_conv
	,pdps02 as pdps02
	,ltrim(rtrim(pdps02)) as pdps02_conv
	,pdps03 as pdps03
	,ltrim(rtrim(pdps03)) as pdps03_conv
	,pdps04 as pdps04
	,ltrim(rtrim(pdps04)) as pdps04_conv
	,pdps05 as pdps05
	,ltrim(rtrim(pdps05)) as pdps05_conv
	,pdps06 as pdps06
	,ltrim(rtrim(pdps06)) as pdps06_conv
	,pdps07 as pdps07
	,ltrim(rtrim(pdps07)) as pdps07_conv
	,pdps08 as pdps08
	,ltrim(rtrim(pdps08)) as pdps08_conv
	,pdps09 as pdps09
	,ltrim(rtrim(pdps09)) as pdps09_conv
	,pdps10 as pdps10
	,ltrim(rtrim(pdps10)) as pdps10_conv
	,cast(pdcrmd as [NVARCHAR](1)) as pdcrmd
	,pdartg as pdartg
	,ltrim(rtrim(pdartg)) as pdartg_conv
	,cast(pdcord as [DECIMAL](38, 4)) as pdcord
	,cast(pdchdt as [NVARCHAR](2)) as pdchdt
	,cast(pddocc as [DECIMAL](38, 4)) as pddocc
	,cast(pdchln as [DECIMAL](38, 4)) as pdchln
	,cast(pdchln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pdchln_conv
	,pdcrcd as pdcrcd
	,ltrim(rtrim(pdcrcd)) as pdcrcd_conv
	,cast(pdcrr as [DECIMAL](38, 4)) as pdcrr
	,cast(pdfrrc as [DECIMAL](38, 4)) as pdfrrc
	,cast(pdfrrc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdfrrc_conv
	,cast(pdfea as [DECIMAL](38, 4)) as pdfea
	,cast(pdfea as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdfea_conv
	,cast(pdfuc as [DECIMAL](38, 4)) as pdfuc
	,cast(pdfuc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdfuc_conv
	,cast(pdfec as [DECIMAL](38, 4)) as pdfec
	,cast(pdfec as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdfec_conv
	,cast(pdfchg as [DECIMAL](38, 4)) as pdfchg
	,cast(pdfchg as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdfchg_conv
	,cast(pdfap as [DECIMAL](38, 4)) as pdfap
	,cast(pdfap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdfap_conv
	,cast(pdfrec as [DECIMAL](38, 4)) as pdfrec
	,cast(pdfrec as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdfrec_conv
	,pdurcd as pdurcd
	,ltrim(rtrim(pdurcd)) as pdurcd_conv
	,cast(pdurdt as [INT]) as pdurdt
	,case when cast(pdurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdurdt as [INT]) %1000, dateadd(year, cast(pdurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdurdt_conv
	,cast(pdurat as [DECIMAL](38, 4)) as pdurat
	,cast(pdurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdurat_conv
	,cast(pdurab as [DECIMAL](38, 4)) as pdurab
	,cast(pdurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pdurab_conv
	,pdurrf as pdurrf
	,ltrim(rtrim(pdurrf)) as pdurrf_conv
	,pdtorg as pdtorg
	,ltrim(rtrim(pdtorg)) as pdtorg_conv
	,pduser as pduser
	,ltrim(rtrim(pduser)) as pduser_conv
	,pdpid as pdpid
	,ltrim(rtrim(pdpid)) as pdpid_conv
	,pdjobn as pdjobn
	,ltrim(rtrim(pdjobn)) as pdjobn_conv
	,cast(pdupmj as [INT]) as pdupmj
	,case when cast(pdupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdupmj as [INT]) %1000, dateadd(year, cast(pdupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdupmj_conv
	,cast(pdtday as [DECIMAL](38, 4)) as pdtday
	,pdvr05 as pdvr05
	,ltrim(rtrim(pdvr05)) as pdvr05_conv
	,pdvr04 as pdvr04
	,ltrim(rtrim(pdvr04)) as pdvr04_conv
	,cast(pdshpn as [DECIMAL](38, 4)) as pdshpn
	,cast(pdrsht as [DECIMAL](38, 4)) as pdrsht
	,cast(pdrsht as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pdrsht_conv
	,cast(pdprjm as [DECIMAL](38, 4)) as pdprjm
	,cast(pdprjm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pdprjm_conv
	,pdosfx as pdosfx
	,ltrim(rtrim(pdosfx)) as pdosfx_conv
	,pdmerl as pdmerl
	,ltrim(rtrim(pdmerl)) as pdmerl_conv
	,cast(pdmcln as [DECIMAL](38, 4)) as pdmcln
	,cast(pdmcln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pdmcln_conv
	,cast(pdmact as [NVARCHAR](1)) as pdmact
	,cast(pdktln as [DECIMAL](38, 4)) as pdktln
	,cast(pdktln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pdktln_conv
	,cast(pdftrl as [DECIMAL](38, 4)) as pdftrl
	,cast(pdftrl as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdftrl_conv
	,pddual as pddual
	,ltrim(rtrim(pddual)) as pddual_conv
	,cast(pddrqt as [DECIMAL](38, 4)) as pddrqt
	,cast(pddlej as [INT]) as pddlej
	,case when cast(pddlej as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pddlej as [INT]) %1000, dateadd(year, cast(pddlej as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pddlej_conv
	,cast(pdctam as [DECIMAL](38, 4)) as pdctam
	,cast(pdctam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pdctam_conv
	,cast(pdcpnt as [DECIMAL](38, 4)) as pdcpnt
	,cast(pdcht as [DECIMAL](38, 4)) as pdcht
	,cast(pdchrt as [DECIMAL](38, 4)) as pdchrt
	,cast(pdchrs as [NVARCHAR](1)) as pdchrs
	,cast(pdchmj as [INT]) as pdchmj
	,case when cast(pdchmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pdchmj as [INT]) %1000, dateadd(year, cast(pdchmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pdchmj_conv
	,pdbcrc as pdbcrc
	,ltrim(rtrim(pdbcrc)) as pdbcrc_conv
	,pdvr03 as pdvr03
	,ltrim(rtrim(pdvr03)) as pdvr03_conv
	,cast(pdldnm as [DECIMAL](38, 4)) as pdldnm
	,cast(pdmkfr as [DECIMAL](38, 4)) as pdmkfr
	,pdpmtn as pdpmtn
	,ltrim(rtrim(pdpmtn)) as pdpmtn_conv
	,cast(pdukid as [DECIMAL](38, 4)) as pdukid
	,pdunspsc as pdunspsc
	,ltrim(rtrim(pdunspsc)) as pdunspsc_conv
	,pdcmdcde as pdcmdcde
	,ltrim(rtrim(pdcmdcde)) as pdcmdcde_conv
	,pdrsfx as pdrsfx
	,ltrim(rtrim(pdrsfx)) as pdrsfx_conv
	,cast(pdwvid as [DECIMAL](38, 4)) as pdwvid
	,cast(pdwvid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pdwvid_conv
	,cast(pdcntrtid as [DECIMAL](38, 4)) as pdcntrtid
	,cast(pdcntrtid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pdcntrtid_conv
	,cast(pdcntrtdid as [DECIMAL](38, 4)) as pdcntrtdid
	,pdmoadj as pdmoadj
	,ltrim(rtrim(pdmoadj)) as pdmoadj_conv
	,cast(pdpodc01 as [NVARCHAR](3)) as pdpodc01
	,cast(pdpodc02 as [NVARCHAR](3)) as pdpodc02
	,cast(pdpodc03 as [NVARCHAR](10)) as pdpodc03
	,cast(pdpodc04 as [NVARCHAR](10)) as pdpodc04
	,cast(pdjbcd as [NVARCHAR](6)) as pdjbcd
	,cast(pdsrqty as [DECIMAL](38, 4)) as pdsrqty
	,cast(pdsrqty as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pdsrqty_conv
	,cast(pdsruom as [NVARCHAR](2)) as pdsruom
	,pdcfgfl as pdcfgfl
	,ltrim(rtrim(pdcfgfl)) as pdcfgfl_conv
	,pdpmpn as pdpmpn
	,ltrim(rtrim(pdpmpn)) as pdpmpn_conv
	,cast(pdpns as [DECIMAL](38, 4)) as pdpns
FROM 
    stg_jde_qat.tmp_f4311_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4311_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4311_sys_integration_id ON rep_jde_qat.f4311_new(sys_integration_id); 
