-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f060116_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f060116_cdc


CREATE TABLE stg_jde.tmp_upsert_f060116_cdc
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
	,cast(yaan8 as [DECIMAL](38, 4)) as yaan8
	,yaalph as yaalph
	,ltrim(rtrim(yaalph)) as yaalph_conv
	,yassn as yassn
	,ltrim(rtrim(yassn)) as yassn_conv
	,yaoemp as yaoemp
	,ltrim(rtrim(yaoemp)) as yaoemp_conv
	,cast(yasex as [NVARCHAR](1)) as yasex
	,cast(yamstx as [NVARCHAR](1)) as yamstx
	,cast(yamsti as [NVARCHAR](1)) as yamsti
	,cast(yawsps as [NVARCHAR](1)) as yawsps
	,cast(yaeic as [NVARCHAR](1)) as yaeic
	,cast(yandep as [DECIMAL](38, 4)) as yandep
	,cast(yaest as [NVARCHAR](1)) as yaest
	,cast(yaecnt as [NVARCHAR](1)) as yaecnt
	,yatarr as yatarr
	,ltrim(rtrim(yatarr)) as yatarr_conv
	,yatara as yatara
	,ltrim(rtrim(yatara)) as yatara_conv
	,cast(yascdc as [DECIMAL](38, 4)) as yascdc
	,cast(yahmst as [NVARCHAR](3)) as yahmst
	,cast(yawkse as [NVARCHAR](3)) as yawkse
	,cast(yahmlc as [NVARCHAR](3)) as yahmlc
	,cast(yalwk1 as [NVARCHAR](3)) as yalwk1
	,cast(yalwk2 as [NVARCHAR](3)) as yalwk2
	,yahmco as yahmco
	,ltrim(rtrim(yahmco)) as yahmco_conv
	,yamcu as yamcu
	,ltrim(rtrim(yamcu)) as yamcu_conv
	,yahmcu as yahmcu
	,ltrim(rtrim(yahmcu)) as yahmcu_conv
	,yasg as yasg
	,ltrim(rtrim(yasg)) as yasg_conv
	,cast(yamail as [NVARCHAR](10)) as yamail
	,cast(yapast as [NVARCHAR](1)) as yapast
	,cast(yapfrq as [NVARCHAR](1)) as yapfrq
	,cast(yasaly as [NVARCHAR](1)) as yasaly
	,cast(yapycb as [DECIMAL](38, 4)) as yapycb
	,cast(yabcb as [DECIMAL](38, 4)) as yabcb
	,cast(yaun as [NVARCHAR](6)) as yaun
	,cast(yajbcd as [NVARCHAR](6)) as yajbcd
	,cast(yajbst as [NVARCHAR](4)) as yajbst
	,cast(yaeeoj as [NVARCHAR](3)) as yaeeoj
	,cast(yaeeom as [NVARCHAR](2)) as yaeeom
	,cast(yatrs as [NVARCHAR](3)) as yatrs
	,cast(yawcmp as [NVARCHAR](4)) as yawcmp
	,cast(yaflsa as [NVARCHAR](1)) as yaflsa
	,cast(yaws as [NVARCHAR](1)) as yaws
	,cast(yashft as [NVARCHAR](1)) as yashft
	,cast(yalmth as [NVARCHAR](1)) as yalmth
	,cast(yapbrt as [DECIMAL](38, 4)) as yapbrt
	,cast(yapbrt as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yapbrt_conv
	,cast(yalf as [DECIMAL](38, 4)) as yalf
	,cast(yalf as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as yalf_conv
	,cast(yasal as [DECIMAL](38, 4)) as yasal
	,cast(yasal as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yasal_conv
	,cast(yaphrt as [DECIMAL](38, 4)) as yaphrt
	,cast(yaphrt as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yaphrt_conv
	,cast(yapprt as [DECIMAL](38, 4)) as yapprt
	,cast(yapprt as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yapprt_conv
	,cast(yapwrn as [DECIMAL](38, 4)) as yapwrn
	,cast(yapwrn as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yapwrn_conv
	,cast(yahrtn as [DECIMAL](38, 4)) as yahrtn
	,cast(yahrtn as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yahrtn_conv
	,cast(yabrtn as [DECIMAL](38, 4)) as yabrtn
	,cast(yabrtn as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yabrtn_conv
	,cast(yasaln as [DECIMAL](38, 4)) as yasaln
	,cast(yasaln as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yasaln_conv
	,cast(yastdh as [DECIMAL](38, 4)) as yastdh
	,cast(yastdh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yastdh_conv
	,cast(yastdd as [DECIMAL](38, 4)) as yastdd
	,cast(yastdd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yastdd_conv
	,cast(yasdyy as [DECIMAL](38, 4)) as yasdyy
	,cast(yasdyy as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yasdyy_conv
	,yausr as yausr
	,ltrim(rtrim(yausr)) as yausr_conv
	,yauflg as yauflg
	,ltrim(rtrim(yauflg)) as yauflg_conv
	,cast(yans as [NVARCHAR](1)) as yans
	,yaifn as yaifn
	,ltrim(rtrim(yaifn)) as yaifn_conv
	,yaimn as yaimn
	,ltrim(rtrim(yaimn)) as yaimn_conv
	,cast(yadob as [INT]) as yadob
	,case when cast(yadob as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yadob as [INT]) %1000, dateadd(year, cast(yadob as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yadob_conv
	,cast(yadsi as [INT]) as yadsi
	,case when cast(yadsi as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yadsi as [INT]) %1000, dateadd(year, cast(yadsi as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yadsi_conv
	,cast(yadt as [INT]) as yadt
	,case when cast(yadt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yadt as [INT]) %1000, dateadd(year, cast(yadt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yadt_conv
	,cast(yadst as [INT]) as yadst
	,case when cast(yadst as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yadst as [INT]) %1000, dateadd(year, cast(yadst as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yadst_conv
	,cast(yapsdt as [INT]) as yapsdt
	,case when cast(yapsdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yapsdt as [INT]) %1000, dateadd(year, cast(yapsdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yapsdt_conv
	,cast(yaptdt as [INT]) as yaptdt
	,case when cast(yaptdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaptdt as [INT]) %1000, dateadd(year, cast(yaptdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaptdt_conv
	,cast(yanrdt as [INT]) as yanrdt
	,case when cast(yanrdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yanrdt as [INT]) %1000, dateadd(year, cast(yanrdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yanrdt_conv
	,cast(yanbdt as [INT]) as yanbdt
	,case when cast(yanbdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yanbdt as [INT]) %1000, dateadd(year, cast(yanbdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yanbdt_conv
	,cast(yanpdt as [INT]) as yanpdt
	,case when cast(yanpdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yanpdt as [INT]) %1000, dateadd(year, cast(yanpdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yanpdt_conv
	,cast(yalcdt as [INT]) as yalcdt
	,case when cast(yalcdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yalcdt as [INT]) %1000, dateadd(year, cast(yalcdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yalcdt_conv
	,cast(yadr as [INT]) as yadr
	,case when cast(yadr as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yadr as [INT]) %1000, dateadd(year, cast(yadr as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yadr_conv
	,cast(yactdt as [INT]) as yactdt
	,case when cast(yactdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yactdt as [INT]) %1000, dateadd(year, cast(yactdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yactdt_conv
	,cast(yaladt as [INT]) as yaladt
	,case when cast(yaladt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaladt as [INT]) %1000, dateadd(year, cast(yaladt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaladt_conv
	,cast(yabsdt as [INT]) as yabsdt
	,case when cast(yabsdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yabsdt as [INT]) %1000, dateadd(year, cast(yabsdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yabsdt_conv
	,cast(yacpdt as [INT]) as yacpdt
	,case when cast(yacpdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yacpdt as [INT]) %1000, dateadd(year, cast(yacpdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yacpdt_conv
	,cast(yaficm as [NVARCHAR](1)) as yaficm
	,cast(yap001 as [NVARCHAR](3)) as yap001
	,cast(yap002 as [NVARCHAR](3)) as yap002
	,cast(yap003 as [NVARCHAR](3)) as yap003
	,cast(yap004 as [NVARCHAR](3)) as yap004
	,cast(yap005 as [NVARCHAR](3)) as yap005
	,cast(yap006 as [NVARCHAR](3)) as yap006
	,cast(yap007 as [NVARCHAR](3)) as yap007
	,cast(yap008 as [NVARCHAR](3)) as yap008
	,cast(yap009 as [NVARCHAR](3)) as yap009
	,cast(yap010 as [NVARCHAR](3)) as yap010
	,cast(yap011 as [NVARCHAR](3)) as yap011
	,cast(yap012 as [NVARCHAR](3)) as yap012
	,cast(yap013 as [NVARCHAR](3)) as yap013
	,cast(yap014 as [NVARCHAR](3)) as yap014
	,cast(yap015 as [NVARCHAR](3)) as yap015
	,cast(yap016 as [NVARCHAR](3)) as yap016
	,cast(yap017 as [NVARCHAR](3)) as yap017
	,cast(yap018 as [NVARCHAR](3)) as yap018
	,cast(yap019 as [NVARCHAR](3)) as yap019
	,cast(yap020 as [NVARCHAR](3)) as yap020
	,cast(yae001 as [NVARCHAR](1)) as yae001
	,cast(yae002 as [NVARCHAR](1)) as yae002
	,cast(yae003 as [NVARCHAR](1)) as yae003
	,cast(yae004 as [NVARCHAR](1)) as yae004
	,cast(yae005 as [NVARCHAR](1)) as yae005
	,cast(yae006 as [NVARCHAR](1)) as yae006
	,cast(yae007 as [NVARCHAR](1)) as yae007
	,cast(yae008 as [NVARCHAR](1)) as yae008
	,cast(yae009 as [NVARCHAR](1)) as yae009
	,cast(yae010 as [NVARCHAR](1)) as yae010
	,cast(yaicc as [NVARCHAR](1)) as yaicc
	,cast(yanmax as [DECIMAL](38, 4)) as yanmax
	,cast(yanmax as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yanmax_conv
	,cast(yasocc as [NVARCHAR](4)) as yasocc
	,yaiusr as yaiusr
	,ltrim(rtrim(yaiusr)) as yaiusr_conv
	,yaitrm as yaitrm
	,ltrim(rtrim(yaitrm)) as yaitrm_conv
	,yainbt as yainbt
	,ltrim(rtrim(yainbt)) as yainbt_conv
	,cast(yaregn as [NVARCHAR](3)) as yaregn
	,yapstp as yapstp
	,ltrim(rtrim(yapstp)) as yapstp_conv
	,cast(yaih as [DECIMAL](38, 4)) as yaih
	,cast(yaih as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yaih_conv
	,cast(yaccpr as [NVARCHAR](3)) as yaccpr
	,cast(yaadpn as [NVARCHAR](1)) as yaadpn
	,yatcnf as yatcnf
	,ltrim(rtrim(yatcnf)) as yatcnf_conv
	,yaraf as yaraf
	,ltrim(rtrim(yaraf)) as yaraf_conv
	,cast(yatipe as [NVARCHAR](1)) as yatipe
	,cast(yarccd as [NVARCHAR](1)) as yarccd
	,cast(yabdrt as [DECIMAL](38, 4)) as yabdrt
	,cast(yabdrt as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yabdrt_conv
	,cast(yaborn as [DECIMAL](38, 4)) as yaborn
	,cast(yaborn as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yaborn_conv
	,cast(yanbdr as [INT]) as yanbdr
	,case when cast(yanbdr as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yanbdr as [INT]) %1000, dateadd(year, cast(yanbdr as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yanbdr_conv
	,cast(yawet as [NVARCHAR](1)) as yawet
	,yaaflg as yaaflg
	,ltrim(rtrim(yaaflg)) as yaaflg_conv
	,cast(yarmst as [NVARCHAR](1)) as yarmst
	,cast(yalmst as [NVARCHAR](1)) as yalmst
	,cast(yasmkr as [NVARCHAR](2)) as yasmkr
	,cast(yacbac as [DECIMAL](38, 4)) as yacbac
	,cast(yacbac as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yacbac_conv
	,yacbaf as yacbaf
	,ltrim(rtrim(yacbaf)) as yacbaf_conv
	,cast(yapccd as [NVARCHAR](5)) as yapccd
	,cast(yarcdt as [INT]) as yarcdt
	,case when cast(yarcdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yarcdt as [INT]) %1000, dateadd(year, cast(yarcdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yarcdt_conv
	,cast(yalsdt as [INT]) as yalsdt
	,case when cast(yalsdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yalsdt as [INT]) %1000, dateadd(year, cast(yalsdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yalsdt_conv
	,cast(yapadt as [INT]) as yapadt
	,case when cast(yapadt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yapadt as [INT]) %1000, dateadd(year, cast(yapadt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yapadt_conv
	,yapymh as yapymh
	,ltrim(rtrim(yapymh)) as yapymh_conv
	,yauyst as yauyst
	,ltrim(rtrim(yauyst)) as yauyst_conv
	,cast(yadtsp as [DECIMAL](38, 4)) as yadtsp
	,cast(yadtsp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as yadtsp_conv
	,cast(yaann8 as [DECIMAL](38, 4)) as yaann8
	,yappnb as yappnb
	,ltrim(rtrim(yappnb)) as yappnb_conv
	,cast(yapydt as [INT]) as yapydt
	,case when cast(yapydt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yapydt as [INT]) %1000, dateadd(year, cast(yapydt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yapydt_conv
	,cast(yacben as [NVARCHAR](1)) as yacben
	,cast(yadobm as [DECIMAL](38, 4)) as yadobm
	,cast(yadstm as [DECIMAL](38, 4)) as yadstm
	,cast(yapsal as [DECIMAL](38, 4)) as yapsal
	,cast(yapsal as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yapsal_conv
	,yahipn as yahipn
	,ltrim(rtrim(yahipn)) as yahipn_conv
	,cast(yacm as [NVARCHAR](2)) as yacm
	,cast(yalsal as [DECIMAL](38, 4)) as yalsal
	,cast(yalsal as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yalsal_conv
	,yadivc as yadivc
	,ltrim(rtrim(yadivc)) as yadivc_conv
	,cast(yavshf as [NVARCHAR](1)) as yavshf
	,cast(yapyrv as [DECIMAL](38, 4)) as yapyrv
	,cast(yapyrv as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as yapyrv_conv
	,cast(yaanpa as [DECIMAL](38, 4)) as yaanpa
	,yapgrd as yapgrd
	,ltrim(rtrim(yapgrd)) as yapgrd_conv
	,yapgrs as yapgrs
	,ltrim(rtrim(yapgrs)) as yapgrs_conv
	,cast(yasloc as [NVARCHAR](8)) as yasloc
	,cast(yanrvw as [INT]) as yanrvw
	,case when cast(yanrvw as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yanrvw as [INT]) %1000, dateadd(year, cast(yanrvw as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yanrvw_conv
	,cast(yatinc as [NVARCHAR](1)) as yatinc
	,cast(yahm01 as [NVARCHAR](1)) as yahm01
	,cast(yahm02 as [NVARCHAR](1)) as yahm02
	,cast(yahm03 as [NVARCHAR](1)) as yahm03
	,cast(yahm04 as [NVARCHAR](1)) as yahm04
	,yapos as yapos
	,ltrim(rtrim(yapos)) as yapos_conv
	,cast(yaed01 as [INT]) as yaed01
	,case when cast(yaed01 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed01 as [INT]) %1000, dateadd(year, cast(yaed01 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed01_conv
	,cast(yaed02 as [INT]) as yaed02
	,case when cast(yaed02 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed02 as [INT]) %1000, dateadd(year, cast(yaed02 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed02_conv
	,cast(yaed03 as [INT]) as yaed03
	,case when cast(yaed03 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed03 as [INT]) %1000, dateadd(year, cast(yaed03 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed03_conv
	,cast(yaed04 as [INT]) as yaed04
	,case when cast(yaed04 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed04 as [INT]) %1000, dateadd(year, cast(yaed04 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed04_conv
	,cast(yaed05 as [INT]) as yaed05
	,case when cast(yaed05 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed05 as [INT]) %1000, dateadd(year, cast(yaed05 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed05_conv
	,cast(yaed06 as [INT]) as yaed06
	,case when cast(yaed06 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed06 as [INT]) %1000, dateadd(year, cast(yaed06 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed06_conv
	,cast(yaed07 as [INT]) as yaed07
	,case when cast(yaed07 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed07 as [INT]) %1000, dateadd(year, cast(yaed07 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed07_conv
	,cast(yaed08 as [INT]) as yaed08
	,case when cast(yaed08 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed08 as [INT]) %1000, dateadd(year, cast(yaed08 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed08_conv
	,cast(yaed09 as [INT]) as yaed09
	,case when cast(yaed09 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed09 as [INT]) %1000, dateadd(year, cast(yaed09 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed09_conv
	,cast(yaed10 as [INT]) as yaed10
	,case when cast(yaed10 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed10 as [INT]) %1000, dateadd(year, cast(yaed10 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed10_conv
	,cast(yaed11 as [INT]) as yaed11
	,case when cast(yaed11 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed11 as [INT]) %1000, dateadd(year, cast(yaed11 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed11_conv
	,cast(yaed12 as [INT]) as yaed12
	,case when cast(yaed12 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed12 as [INT]) %1000, dateadd(year, cast(yaed12 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed12_conv
	,cast(yaed13 as [INT]) as yaed13
	,case when cast(yaed13 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed13 as [INT]) %1000, dateadd(year, cast(yaed13 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed13_conv
	,cast(yaed14 as [INT]) as yaed14
	,case when cast(yaed14 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed14 as [INT]) %1000, dateadd(year, cast(yaed14 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed14_conv
	,cast(yaed15 as [INT]) as yaed15
	,case when cast(yaed15 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed15 as [INT]) %1000, dateadd(year, cast(yaed15 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed15_conv
	,cast(yaed16 as [INT]) as yaed16
	,case when cast(yaed16 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed16 as [INT]) %1000, dateadd(year, cast(yaed16 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed16_conv
	,cast(yaed17 as [INT]) as yaed17
	,case when cast(yaed17 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed17 as [INT]) %1000, dateadd(year, cast(yaed17 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed17_conv
	,cast(yaed18 as [INT]) as yaed18
	,case when cast(yaed18 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed18 as [INT]) %1000, dateadd(year, cast(yaed18 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed18_conv
	,cast(yaed19 as [INT]) as yaed19
	,case when cast(yaed19 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed19 as [INT]) %1000, dateadd(year, cast(yaed19 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed19_conv
	,cast(yaed20 as [INT]) as yaed20
	,case when cast(yaed20 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaed20 as [INT]) %1000, dateadd(year, cast(yaed20 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaed20_conv
	,yadept as yadept
	,ltrim(rtrim(yadept)) as yadept_conv
	,cast(yafage as [DECIMAL](38, 4)) as yafage
	,cast(yafsal as [DECIMAL](38, 4)) as yafsal
	,cast(yafsal as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yafsal_conv
	,cast(yaadsd as [INT]) as yaadsd
	,case when cast(yaadsd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaadsd as [INT]) %1000, dateadd(year, cast(yaadsd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaadsd_conv
	,cast(yacmpa as [DECIMAL](38, 4)) as yacmpa
	,cast(yacmpa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yacmpa_conv
	,cast(yaepnt as [DECIMAL](38, 4)) as yaepnt
	,cast(yaepnt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as yaepnt_conv
	,yajobn as yajobn
	,ltrim(rtrim(yajobn)) as yajobn_conv
	,yauser as yauser
	,ltrim(rtrim(yauser)) as yauser_conv
	,yapid as yapid
	,ltrim(rtrim(yapid)) as yapid_conv
	,cast(yaupmj as [INT]) as yaupmj
	,case when cast(yaupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yaupmj as [INT]) %1000, dateadd(year, cast(yaupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yaupmj_conv
	,cast(yak001 as [NVARCHAR](1)) as yak001
	,cast(yak002 as [NVARCHAR](1)) as yak002
	,cast(yak003 as [NVARCHAR](1)) as yak003
	,cast(yak004 as [NVARCHAR](1)) as yak004
	,cast(yak005 as [NVARCHAR](1)) as yak005
	,cast(yak006 as [NVARCHAR](1)) as yak006
	,cast(yak007 as [NVARCHAR](1)) as yak007
	,cast(yak008 as [NVARCHAR](1)) as yak008
	,cast(yak009 as [NVARCHAR](1)) as yak009
	,cast(yak010 as [NVARCHAR](1)) as yak010
	,cast(yaatpy as [DECIMAL](38, 4)) as yaatpy
	,cast(yapens as [NVARCHAR](1)) as yapens
	,cast(yaorg as [NVARCHAR](1)) as yaorg
	,cast(yabens as [NVARCHAR](1)) as yabens
	,cast(yafte as [DECIMAL](38, 4)) as yafte
	,cast(yafte as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yafte_conv
	,cast(yaaaf as [NVARCHAR](1)) as yaaaf
	,cast(yasui as [NVARCHAR](1)) as yasui
	,cast(yadtsf as [INT]) as yadtsf
	,case when cast(yadtsf as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(yadtsf as [INT]) %1000, dateadd(year, cast(yadtsf as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as yadtsf_conv
	,cast(yasmoy as [DECIMAL](38, 4)) as yasmoy
	,cast(yasmoy as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as yasmoy_conv
	,cast(yaldid as [NVARCHAR](5)) as yaldid
	,cast(yattar as [NVARCHAR](1)) as yattar
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
	,yaan8
	,yaalph
	,yassn
	,yaoemp
	,yasex
	,yamstx
	,yamsti
	,yawsps
	,yaeic
	,yandep
	,yaest
	,yaecnt
	,yatarr
	,yatara
	,yascdc
	,yahmst
	,yawkse
	,yahmlc
	,yalwk1
	,yalwk2
	,yahmco
	,yamcu
	,yahmcu
	,yasg
	,yamail
	,yapast
	,yapfrq
	,yasaly
	,yapycb
	,yabcb
	,yaun
	,yajbcd
	,yajbst
	,yaeeoj
	,yaeeom
	,yatrs
	,yawcmp
	,yaflsa
	,yaws
	,yashft
	,yalmth
	,yapbrt
	,yalf
	,yasal
	,yaphrt
	,yapprt
	,yapwrn
	,yahrtn
	,yabrtn
	,yasaln
	,yastdh
	,yastdd
	,yasdyy
	,yausr
	,yauflg
	,yans
	,yaifn
	,yaimn
	,yadob
	,yadsi
	,yadt
	,yadst
	,yapsdt
	,yaptdt
	,yanrdt
	,yanbdt
	,yanpdt
	,yalcdt
	,yadr
	,yactdt
	,yaladt
	,yabsdt
	,yacpdt
	,yaficm
	,yap001
	,yap002
	,yap003
	,yap004
	,yap005
	,yap006
	,yap007
	,yap008
	,yap009
	,yap010
	,yap011
	,yap012
	,yap013
	,yap014
	,yap015
	,yap016
	,yap017
	,yap018
	,yap019
	,yap020
	,yae001
	,yae002
	,yae003
	,yae004
	,yae005
	,yae006
	,yae007
	,yae008
	,yae009
	,yae010
	,yaicc
	,yanmax
	,yasocc
	,yaiusr
	,yaitrm
	,yainbt
	,yaregn
	,yapstp
	,yaih
	,yaccpr
	,yaadpn
	,yatcnf
	,yaraf
	,yatipe
	,yarccd
	,yabdrt
	,yaborn
	,yanbdr
	,yawet
	,yaaflg
	,yarmst
	,yalmst
	,yasmkr
	,yacbac
	,yacbaf
	,yapccd
	,yarcdt
	,yalsdt
	,yapadt
	,yapymh
	,yauyst
	,yadtsp
	,yaann8
	,yappnb
	,yapydt
	,yacben
	,yadobm
	,yadstm
	,yapsal
	,yahipn
	,yacm
	,yalsal
	,yadivc
	,yavshf
	,yapyrv
	,yaanpa
	,yapgrd
	,yapgrs
	,yasloc
	,yanrvw
	,yatinc
	,yahm01
	,yahm02
	,yahm03
	,yahm04
	,yapos
	,yaed01
	,yaed02
	,yaed03
	,yaed04
	,yaed05
	,yaed06
	,yaed07
	,yaed08
	,yaed09
	,yaed10
	,yaed11
	,yaed12
	,yaed13
	,yaed14
	,yaed15
	,yaed16
	,yaed17
	,yaed18
	,yaed19
	,yaed20
	,yadept
	,yafage
	,yafsal
	,yaadsd
	,yacmpa
	,yaepnt
	,yajobn
	,yauser
	,yapid
	,yaupmj
	,yak001
	,yak002
	,yak003
	,yak004
	,yak005
	,yak006
	,yak007
	,yak008
	,yak009
	,yak010
	,yaatpy
	,yapens
	,yaorg
	,yabens
	,yafte
	,yaaaf
	,yasui
	,yadtsf
	,yasmoy
	,yaldid
	,yattar,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f060116_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f060116_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f060116_cdc_sys_integration_id ON stg_jde.tmp_upsert_f060116_cdc(sys_integration_id); 
