-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f42199_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f42199_new


CREATE TABLE rep_jde_qat.f42199_new
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
	,slkcoo as slkcoo
	,ltrim(rtrim(slkcoo)) as slkcoo_conv
	,cast(sldoco as [DECIMAL](38, 4)) as sldoco
	,cast(sldcto as [NVARCHAR](2)) as sldcto
	,cast(sllnid as [DECIMAL](38, 4)) as sllnid
	,cast(sllnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sllnid_conv
	,slsfxo as slsfxo
	,ltrim(rtrim(slsfxo)) as slsfxo_conv
	,slmcu as slmcu
	,ltrim(rtrim(slmcu)) as slmcu_conv
	,slco as slco
	,ltrim(rtrim(slco)) as slco_conv
	,slokco as slokco
	,ltrim(rtrim(slokco)) as slokco_conv
	,sloorn as sloorn
	,ltrim(rtrim(sloorn)) as sloorn_conv
	,cast(slocto as [NVARCHAR](2)) as slocto
	,cast(slogno as [DECIMAL](38, 4)) as slogno
	,cast(slogno as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slogno_conv
	,slrkco as slrkco
	,ltrim(rtrim(slrkco)) as slrkco_conv
	,slrorn as slrorn
	,ltrim(rtrim(slrorn)) as slrorn_conv
	,cast(slrcto as [NVARCHAR](2)) as slrcto
	,cast(slrlln as [DECIMAL](38, 4)) as slrlln
	,cast(slrlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slrlln_conv
	,sldmct as sldmct
	,ltrim(rtrim(sldmct)) as sldmct_conv
	,cast(sldmcs as [DECIMAL](38, 4)) as sldmcs
	,cast(slan8 as [DECIMAL](38, 4)) as slan8
	,cast(slshan as [DECIMAL](38, 4)) as slshan
	,cast(slpa8 as [DECIMAL](38, 4)) as slpa8
	,cast(sldrqj as [INT]) as sldrqj
	,case when cast(sldrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sldrqj as [INT]) %1000, dateadd(year, cast(sldrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sldrqj_conv
	,cast(sltrdj as [INT]) as sltrdj
	,case when cast(sltrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sltrdj as [INT]) %1000, dateadd(year, cast(sltrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sltrdj_conv
	,cast(slpddj as [INT]) as slpddj
	,case when cast(slpddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slpddj as [INT]) %1000, dateadd(year, cast(slpddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slpddj_conv
	,cast(sladdj as [INT]) as sladdj
	,case when cast(sladdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sladdj as [INT]) %1000, dateadd(year, cast(sladdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sladdj_conv
	,cast(slivd as [INT]) as slivd
	,case when cast(slivd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slivd as [INT]) %1000, dateadd(year, cast(slivd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slivd_conv
	,cast(slcndj as [INT]) as slcndj
	,case when cast(slcndj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slcndj as [INT]) %1000, dateadd(year, cast(slcndj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slcndj_conv
	,cast(sldgl as [INT]) as sldgl
	,case when cast(sldgl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sldgl as [INT]) %1000, dateadd(year, cast(sldgl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sldgl_conv
	,cast(slrsdj as [INT]) as slrsdj
	,case when cast(slrsdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slrsdj as [INT]) %1000, dateadd(year, cast(slrsdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slrsdj_conv
	,cast(slpefj as [INT]) as slpefj
	,case when cast(slpefj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slpefj as [INT]) %1000, dateadd(year, cast(slpefj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slpefj_conv
	,cast(slppdj as [INT]) as slppdj
	,case when cast(slppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slppdj as [INT]) %1000, dateadd(year, cast(slppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slppdj_conv
	,slvr01 as slvr01
	,ltrim(rtrim(slvr01)) as slvr01_conv
	,slvr02 as slvr02
	,ltrim(rtrim(slvr02)) as slvr02_conv
	,cast(slitm as [DECIMAL](38, 4)) as slitm
	,sllitm as sllitm
	,ltrim(rtrim(sllitm)) as sllitm_conv
	,slaitm as slaitm
	,ltrim(rtrim(slaitm)) as slaitm_conv
	,sllocn as sllocn
	,ltrim(rtrim(sllocn)) as sllocn_conv
	,sllotn as sllotn
	,ltrim(rtrim(sllotn)) as sllotn_conv
	,cast(slfrgd as [NVARCHAR](3)) as slfrgd
	,cast(slthgd as [NVARCHAR](3)) as slthgd
	,cast(slfrmp as [DECIMAL](38, 4)) as slfrmp
	,cast(slfrmp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slfrmp_conv
	,cast(slthrp as [DECIMAL](38, 4)) as slthrp
	,cast(slthrp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slthrp_conv
	,cast(slexdp as [DECIMAL](38, 4)) as slexdp
	,cast(slexdp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slexdp_conv
	,sldsc1 as sldsc1
	,ltrim(rtrim(sldsc1)) as sldsc1_conv
	,sldsc2 as sldsc2
	,ltrim(rtrim(sldsc2)) as sldsc2_conv
	,sllnty as sllnty
	,ltrim(rtrim(sllnty)) as sllnty_conv
	,cast(slnxtr as [NVARCHAR](3)) as slnxtr
	,cast(sllttr as [NVARCHAR](3)) as sllttr
	,slemcu as slemcu
	,ltrim(rtrim(slemcu)) as slemcu_conv
	,slrlit as slrlit
	,ltrim(rtrim(slrlit)) as slrlit_conv
	,cast(slktln as [DECIMAL](38, 4)) as slktln
	,cast(slktln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slktln_conv
	,cast(slcpnt as [DECIMAL](38, 4)) as slcpnt
	,cast(slrkit as [DECIMAL](38, 4)) as slrkit
	,cast(slrkit as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slrkit_conv
	,cast(slktp as [DECIMAL](38, 4)) as slktp
	,cast(slktp as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slktp_conv
	,cast(slsrp1 as [NVARCHAR](3)) as slsrp1
	,cast(slsrp2 as [NVARCHAR](3)) as slsrp2
	,cast(slsrp3 as [NVARCHAR](3)) as slsrp3
	,cast(slsrp4 as [NVARCHAR](3)) as slsrp4
	,cast(slsrp5 as [NVARCHAR](3)) as slsrp5
	,cast(slprp1 as [NVARCHAR](3)) as slprp1
	,cast(slprp2 as [NVARCHAR](3)) as slprp2
	,cast(slprp3 as [NVARCHAR](3)) as slprp3
	,cast(slprp4 as [NVARCHAR](3)) as slprp4
	,cast(slprp5 as [NVARCHAR](3)) as slprp5
	,cast(sluom as [NVARCHAR](2)) as sluom
	,cast(sluorg as [DECIMAL](38, 4)) as sluorg
	,cast(sluorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sluorg_conv
	,cast(slsoqs as [DECIMAL](38, 4)) as slsoqs
	,cast(slsoqs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slsoqs_conv
	,cast(slsobk as [DECIMAL](38, 4)) as slsobk
	,cast(slsobk as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slsobk_conv
	,cast(slsocn as [DECIMAL](38, 4)) as slsocn
	,cast(slsocn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slsocn_conv
	,cast(slsone as [DECIMAL](38, 4)) as slsone
	,cast(slsone as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slsone_conv
	,cast(sluopn as [DECIMAL](38, 4)) as sluopn
	,cast(sluopn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sluopn_conv
	,cast(slqtyt as [DECIMAL](38, 4)) as slqtyt
	,cast(slqtyt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slqtyt_conv
	,cast(slqrlv as [DECIMAL](38, 4)) as slqrlv
	,cast(slqrlv as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slqrlv_conv
	,cast(slcomm as [NVARCHAR](1)) as slcomm
	,cast(slotqy as [NVARCHAR](1)) as slotqy
	,cast(sluprc as [DECIMAL](38, 4)) as sluprc
	,cast(sluprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sluprc_conv
	,cast(slaexp as [DECIMAL](38, 4)) as slaexp
	,cast(slaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as slaexp_conv
	,cast(slaopn as [DECIMAL](38, 4)) as slaopn
	,cast(slaopn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as slaopn_conv
	,slprov as slprov
	,ltrim(rtrim(slprov)) as slprov_conv
	,sltpc as sltpc
	,ltrim(rtrim(sltpc)) as sltpc_conv
	,cast(slapum as [NVARCHAR](2)) as slapum
	,cast(sllprc as [DECIMAL](38, 4)) as sllprc
	,cast(sllprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sllprc_conv
	,cast(sluncs as [DECIMAL](38, 4)) as sluncs
	,cast(sluncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sluncs_conv
	,cast(slecst as [DECIMAL](38, 4)) as slecst
	,cast(slecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as slecst_conv
	,slcsto as slcsto
	,ltrim(rtrim(slcsto)) as slcsto_conv
	,cast(sltcst as [DECIMAL](38, 4)) as sltcst
	,cast(sltcst as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sltcst_conv
	,cast(slinmg as [NVARCHAR](10)) as slinmg
	,slptc as slptc
	,ltrim(rtrim(slptc)) as slptc_conv
	,cast(slryin as [NVARCHAR](1)) as slryin
	,cast(sldtbs as [NVARCHAR](1)) as sldtbs
	,cast(sltrdc as [DECIMAL](38, 4)) as sltrdc
	,cast(sltrdc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sltrdc_conv
	,cast(slfun2 as [DECIMAL](38, 4)) as slfun2
	,cast(slfun2 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slfun2_conv
	,cast(slasn as [NVARCHAR](8)) as slasn
	,cast(slprgr as [NVARCHAR](8)) as slprgr
	,slclvl as slclvl
	,ltrim(rtrim(slclvl)) as slclvl_conv
	,cast(slcadc as [DECIMAL](38, 4)) as slcadc
	,cast(slcadc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slcadc_conv
	,slkco as slkco
	,ltrim(rtrim(slkco)) as slkco_conv
	,cast(sldoc as [DECIMAL](38, 4)) as sldoc
	,cast(sldct as [NVARCHAR](2)) as sldct
	,cast(slodoc as [DECIMAL](38, 4)) as slodoc
	,cast(slodct as [NVARCHAR](2)) as slodct
	,slokc as slokc
	,ltrim(rtrim(slokc)) as slokc_conv
	,cast(slpsn as [DECIMAL](38, 4)) as slpsn
	,cast(sldeln as [DECIMAL](38, 4)) as sldeln
	,cast(sltax1 as [NVARCHAR](1)) as sltax1
	,sltxa1 as sltxa1
	,ltrim(rtrim(sltxa1)) as sltxa1_conv
	,cast(slexr1 as [NVARCHAR](2)) as slexr1
	,slatxt as slatxt
	,ltrim(rtrim(slatxt)) as slatxt_conv
	,cast(slprio as [NVARCHAR](1)) as slprio
	,slresl as slresl
	,ltrim(rtrim(slresl)) as slresl_conv
	,slback as slback
	,ltrim(rtrim(slback)) as slback_conv
	,slsbal as slsbal
	,ltrim(rtrim(slsbal)) as slsbal_conv
	,slapts as slapts
	,ltrim(rtrim(slapts)) as slapts_conv
	,cast(sllob as [NVARCHAR](3)) as sllob
	,cast(sleuse as [NVARCHAR](3)) as sleuse
	,cast(sldtys as [NVARCHAR](2)) as sldtys
	,cast(slntr as [NVARCHAR](2)) as slntr
	,cast(slvend as [DECIMAL](38, 4)) as slvend
	,cast(slvend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slvend_conv
	,cast(slcars as [DECIMAL](38, 4)) as slcars
	,cast(slcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slcars_conv
	,cast(slmot as [NVARCHAR](3)) as slmot
	,cast(slrout as [NVARCHAR](3)) as slrout
	,cast(slstop as [NVARCHAR](3)) as slstop
	,cast(slzon as [NVARCHAR](3)) as slzon
	,slcnid as slcnid
	,ltrim(rtrim(slcnid)) as slcnid_conv
	,cast(slfrth as [NVARCHAR](3)) as slfrth
	,cast(slshcm as [NVARCHAR](3)) as slshcm
	,cast(slshcn as [NVARCHAR](3)) as slshcn
	,slsern as slsern
	,ltrim(rtrim(slsern)) as slsern_conv
	,cast(sluom1 as [NVARCHAR](2)) as sluom1
	,cast(slpqor as [DECIMAL](38, 4)) as slpqor
	,cast(slpqor as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slpqor_conv
	,cast(sluom2 as [NVARCHAR](2)) as sluom2
	,cast(slsqor as [DECIMAL](38, 4)) as slsqor
	,cast(slsqor as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slsqor_conv
	,cast(sluom4 as [NVARCHAR](2)) as sluom4
	,cast(slitwt as [DECIMAL](38, 4)) as slitwt
	,cast(slitwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slitwt_conv
	,cast(slwtum as [NVARCHAR](2)) as slwtum
	,cast(slitvl as [DECIMAL](38, 4)) as slitvl
	,cast(slitvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slitvl_conv
	,cast(slvlum as [NVARCHAR](2)) as slvlum
	,cast(slrprc as [NVARCHAR](8)) as slrprc
	,cast(slorpr as [NVARCHAR](8)) as slorpr
	,slorp as slorp
	,ltrim(rtrim(slorp)) as slorp_conv
	,slcmgp as slcmgp
	,ltrim(rtrim(slcmgp)) as slcmgp_conv
	,slglc as slglc
	,ltrim(rtrim(slglc)) as slglc_conv
	,cast(slctry as [DECIMAL](38, 4)) as slctry
	,cast(slfy as [DECIMAL](38, 4)) as slfy
	,cast(slfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slfy_conv
	,slso01 as slso01
	,ltrim(rtrim(slso01)) as slso01_conv
	,slso02 as slso02
	,ltrim(rtrim(slso02)) as slso02_conv
	,slso03 as slso03
	,ltrim(rtrim(slso03)) as slso03_conv
	,slso04 as slso04
	,ltrim(rtrim(slso04)) as slso04_conv
	,slso05 as slso05
	,ltrim(rtrim(slso05)) as slso05_conv
	,slso06 as slso06
	,ltrim(rtrim(slso06)) as slso06_conv
	,slso07 as slso07
	,ltrim(rtrim(slso07)) as slso07_conv
	,slso08 as slso08
	,ltrim(rtrim(slso08)) as slso08_conv
	,slso09 as slso09
	,ltrim(rtrim(slso09)) as slso09_conv
	,slso10 as slso10
	,ltrim(rtrim(slso10)) as slso10_conv
	,slso11 as slso11
	,ltrim(rtrim(slso11)) as slso11_conv
	,slso12 as slso12
	,ltrim(rtrim(slso12)) as slso12_conv
	,slso13 as slso13
	,ltrim(rtrim(slso13)) as slso13_conv
	,slso14 as slso14
	,ltrim(rtrim(slso14)) as slso14_conv
	,slso15 as slso15
	,ltrim(rtrim(slso15)) as slso15_conv
	,slacom as slacom
	,ltrim(rtrim(slacom)) as slacom_conv
	,slcmcg as slcmcg
	,ltrim(rtrim(slcmcg)) as slcmcg_conv
	,cast(slrcd as [NVARCHAR](3)) as slrcd
	,cast(slgrwt as [DECIMAL](38, 4)) as slgrwt
	,cast(slgrwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slgrwt_conv
	,cast(slgwum as [NVARCHAR](2)) as slgwum
	,slsbl as slsbl
	,ltrim(rtrim(slsbl)) as slsbl_conv
	,cast(slsblt as [NVARCHAR](1)) as slsblt
	,cast(sllcod as [NVARCHAR](2)) as sllcod
	,cast(slupc1 as [NVARCHAR](2)) as slupc1
	,cast(slupc2 as [NVARCHAR](2)) as slupc2
	,cast(slupc3 as [NVARCHAR](2)) as slupc3
	,slswms as slswms
	,ltrim(rtrim(slswms)) as slswms_conv
	,cast(sluncd as [NVARCHAR](1)) as sluncd
	,cast(slcrmd as [NVARCHAR](1)) as slcrmd
	,slcrcd as slcrcd
	,ltrim(rtrim(slcrcd)) as slcrcd_conv
	,cast(slcrr as [DECIMAL](38, 4)) as slcrr
	,cast(slfprc as [DECIMAL](38, 4)) as slfprc
	,cast(slfprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slfprc_conv
	,cast(slfup as [DECIMAL](38, 4)) as slfup
	,cast(slfup as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slfup_conv
	,cast(slfea as [DECIMAL](38, 4)) as slfea
	,cast(slfea as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as slfea_conv
	,cast(slfuc as [DECIMAL](38, 4)) as slfuc
	,cast(slfuc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as slfuc_conv
	,cast(slfec as [DECIMAL](38, 4)) as slfec
	,cast(slfec as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as slfec_conv
	,slurcd as slurcd
	,ltrim(rtrim(slurcd)) as slurcd_conv
	,cast(slurdt as [INT]) as slurdt
	,case when cast(slurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slurdt as [INT]) %1000, dateadd(year, cast(slurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slurdt_conv
	,cast(slurat as [DECIMAL](38, 4)) as slurat
	,cast(slurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as slurat_conv
	,cast(slurab as [DECIMAL](38, 4)) as slurab
	,cast(slurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slurab_conv
	,slurrf as slurrf
	,ltrim(rtrim(slurrf)) as slurrf_conv
	,sltorg as sltorg
	,ltrim(rtrim(sltorg)) as sltorg_conv
	,sluser as sluser
	,ltrim(rtrim(sluser)) as sluser_conv
	,slpid as slpid
	,ltrim(rtrim(slpid)) as slpid_conv
	,sljobn as sljobn
	,ltrim(rtrim(sljobn)) as sljobn_conv
	,cast(slupmj as [INT]) as slupmj
	,case when cast(slupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slupmj as [INT]) %1000, dateadd(year, cast(slupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slupmj_conv
	,cast(sltday as [DECIMAL](38, 4)) as sltday
	,slso16 as slso16
	,ltrim(rtrim(slso16)) as slso16_conv
	,slso17 as slso17
	,ltrim(rtrim(slso17)) as slso17_conv
	,slso18 as slso18
	,ltrim(rtrim(slso18)) as slso18_conv
	,slso19 as slso19
	,ltrim(rtrim(slso19)) as slso19_conv
	,slso20 as slso20
	,ltrim(rtrim(slso20)) as slso20_conv
	,slir01 as slir01
	,ltrim(rtrim(slir01)) as slir01_conv
	,slir02 as slir02
	,ltrim(rtrim(slir02)) as slir02_conv
	,slir03 as slir03
	,ltrim(rtrim(slir03)) as slir03_conv
	,slir04 as slir04
	,ltrim(rtrim(slir04)) as slir04_conv
	,slir05 as slir05
	,ltrim(rtrim(slir05)) as slir05_conv
	,cast(slsoor as [INT]) as slsoor
	,cast(slsoor as [INT]) / cast(1 as [INT]) as slsoor_conv
	,slvr03 as slvr03
	,ltrim(rtrim(slvr03)) as slvr03_conv
	,cast(sldeid as [DECIMAL](38, 4)) as sldeid
	,cast(sldeid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sldeid_conv
	,slpsig as slpsig
	,ltrim(rtrim(slpsig)) as slpsig_conv
	,slrlnu as slrlnu
	,ltrim(rtrim(slrlnu)) as slrlnu_conv
	,cast(slpmdt as [DECIMAL](38, 4)) as slpmdt
	,cast(slrltm as [DECIMAL](38, 4)) as slrltm
	,cast(slrldj as [INT]) as slrldj
	,case when cast(slrldj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slrldj as [INT]) %1000, dateadd(year, cast(slrldj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slrldj_conv
	,cast(sldrqt as [DECIMAL](38, 4)) as sldrqt
	,cast(sladtm as [DECIMAL](38, 4)) as sladtm
	,cast(sloptt as [DECIMAL](38, 4)) as sloptt
	,cast(slpdtt as [DECIMAL](38, 4)) as slpdtt
	,cast(slpstm as [DECIMAL](38, 4)) as slpstm
	,slxdck as slxdck
	,ltrim(rtrim(slxdck)) as slxdck_conv
	,cast(slxpty as [DECIMAL](38, 4)) as slxpty
	,cast(slxpty as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slxpty_conv
	,sldual as sldual
	,ltrim(rtrim(sldual)) as sldual_conv
	,cast(slbsc as [NVARCHAR](10)) as slbsc
	,cast(slcbsc as [NVARCHAR](10)) as slcbsc
	,cast(slcord as [DECIMAL](38, 4)) as slcord
	,cast(sldvan as [DECIMAL](38, 4)) as sldvan
	,cast(sldvan as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sldvan_conv
	,slpend as slpend
	,ltrim(rtrim(slpend)) as slpend_conv
	,cast(slrfrv as [NVARCHAR](3)) as slrfrv
	,cast(slmcln as [DECIMAL](38, 4)) as slmcln
	,cast(slmcln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slmcln_conv
	,cast(slshpn as [DECIMAL](38, 4)) as slshpn
	,cast(slrsdt as [DECIMAL](38, 4)) as slrsdt
	,cast(slprjm as [DECIMAL](38, 4)) as slprjm
	,cast(slprjm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slprjm_conv
	,cast(sloseq as [DECIMAL](38, 4)) as sloseq
	,cast(sloseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sloseq_conv
	,slmerl as slmerl
	,ltrim(rtrim(slmerl)) as slmerl_conv
	,cast(slhold as [NVARCHAR](2)) as slhold
	,slhdbu as slhdbu
	,ltrim(rtrim(slhdbu)) as slhdbu_conv
	,sldmbu as sldmbu
	,ltrim(rtrim(sldmbu)) as sldmbu_conv
	,slbcrc as slbcrc
	,ltrim(rtrim(slbcrc)) as slbcrc_conv
	,cast(slodln as [DECIMAL](38, 4)) as slodln
	,cast(slodln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slodln_conv
	,cast(slopdj as [INT]) as slopdj
	,case when cast(slopdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(slopdj as [INT]) %1000, dateadd(year, cast(slopdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as slopdj_conv
	,slxkco as slxkco
	,ltrim(rtrim(slxkco)) as slxkco_conv
	,cast(slxorn as [DECIMAL](38, 4)) as slxorn
	,cast(slxcto as [NVARCHAR](2)) as slxcto
	,cast(slxlln as [DECIMAL](38, 4)) as slxlln
	,cast(slxlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as slxlln_conv
	,slxsfx as slxsfx
	,ltrim(rtrim(slxsfx)) as slxsfx_conv
	,cast(slpoe as [NVARCHAR](6)) as slpoe
	,slpmto as slpmto
	,ltrim(rtrim(slpmto)) as slpmto_conv
	,cast(slanby as [DECIMAL](38, 4)) as slanby
	,slpmtn as slpmtn
	,ltrim(rtrim(slpmtn)) as slpmtn_conv
	,cast(slnumb as [DECIMAL](38, 4)) as slnumb
	,cast(slaaid as [DECIMAL](38, 4)) as slaaid
	,cast(slpran8 as [DECIMAL](38, 4)) as slpran8
	,cast(slpran8 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slpran8_conv
	,slspattn as slspattn
	,ltrim(rtrim(slspattn)) as slspattn_conv
	,cast(slprcidln as [DECIMAL](38, 4)) as slprcidln
	,cast(slprcidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slprcidln_conv
	,cast(slccidln as [DECIMAL](38, 4)) as slccidln
	,cast(slccidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slccidln_conv
	,cast(slshccidln as [DECIMAL](38, 4)) as slshccidln
	,cast(slshccidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slshccidln_conv
	,cast(sloppid as [DECIMAL](38, 4)) as sloppid
	,cast(sloppid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sloppid_conv
	,cast(slostp as [NVARCHAR](3)) as slostp
	,cast(slukid as [DECIMAL](38, 4)) as slukid
	,slcatnm as slcatnm
	,ltrim(rtrim(slcatnm)) as slcatnm_conv
	,slalloc as slalloc
	,ltrim(rtrim(slalloc)) as slalloc_conv
	,cast(slfulpid as [INT]) as slfulpid
	,cast(slfulpid as [INT]) / cast(1 as [INT]) as slfulpid_conv
	,cast(slallsts as [NVARCHAR](30)) as slallsts
	,cast(sloscore as [DECIMAL](38, 4)) as sloscore
	,cast(sloscore as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sloscore_conv
	,sloscoreo as sloscoreo
	,ltrim(rtrim(sloscoreo)) as sloscoreo_conv
	,slcmco as slcmco
	,ltrim(rtrim(slcmco)) as slcmco_conv
	,cast(slkitid as [DECIMAL](38, 4)) as slkitid
	,cast(slkitid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as slkitid_conv
	,cast(slkitamtdom as [DECIMAL](38, 4)) as slkitamtdom
	,cast(slkitamtdom as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as slkitamtdom_conv
	,cast(slkitamtfor as [DECIMAL](38, 4)) as slkitamtfor
	,cast(slkitamtfor as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as slkitamtfor_conv
	,slkitdirty as slkitdirty
	,ltrim(rtrim(slkitdirty)) as slkitdirty_conv
	,slocitt as slocitt
	,ltrim(rtrim(slocitt)) as slocitt_conv
	,cast(sloccardno as [DECIMAL](38, 4)) as sloccardno
	,cast(sloccardno as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as sloccardno_conv
	,slpmpn as slpmpn
	,ltrim(rtrim(slpmpn)) as slpmpn_conv
	,cast(slpns as [DECIMAL](38, 4)) as slpns
FROM 
    stg_jde_qat.tmp_f42199_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f42199_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f42199_sys_integration_id ON rep_jde_qat.f42199_new(sys_integration_id); 
