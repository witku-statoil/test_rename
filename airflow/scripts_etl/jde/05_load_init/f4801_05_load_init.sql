-- Create rep new table for init
IF OBJECT_ID('rep_jde.f4801_new') IS NOT NULL
    DROP TABLE rep_jde.f4801_new


CREATE TABLE rep_jde.f4801_new
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
	,cast(wadcto as [NVARCHAR](2)) as wadcto
	,cast(wadoco as [DECIMAL](38, 4)) as wadoco
	,wasfxo as wasfxo
	,ltrim(rtrim(wasfxo)) as wasfxo_conv
	,cast(warcto as [NVARCHAR](2)) as warcto
	,warorn as warorn
	,ltrim(rtrim(warorn)) as warorn_conv
	,cast(walnid as [DECIMAL](38, 4)) as walnid
	,cast(walnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as walnid_conv
	,cast(waptwo as [DECIMAL](38, 4)) as waptwo
	,wapars as wapars
	,ltrim(rtrim(wapars)) as wapars_conv
	,cast(watyps as [NVARCHAR](1)) as watyps
	,cast(waprts as [NVARCHAR](1)) as waprts
	,wadl01 as wadl01
	,ltrim(rtrim(wadl01)) as wadl01_conv
	,wastcm as wastcm
	,ltrim(rtrim(wastcm)) as wastcm_conv
	,waco as waco
	,ltrim(rtrim(waco)) as waco_conv
	,wamcu as wamcu
	,ltrim(rtrim(wamcu)) as wamcu_conv
	,wammcu as wammcu
	,ltrim(rtrim(wammcu)) as wammcu_conv
	,walocn as walocn
	,ltrim(rtrim(walocn)) as walocn_conv
	,waaisl as waaisl
	,ltrim(rtrim(waaisl)) as waaisl_conv
	,wabin as wabin
	,ltrim(rtrim(wabin)) as wabin_conv
	,cast(wasrst as [NVARCHAR](2)) as wasrst
	,cast(wadcg as [INT]) as wadcg
	,case when cast(wadcg as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wadcg as [INT]) %1000, dateadd(year, cast(wadcg as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wadcg_conv
	,wasub as wasub
	,ltrim(rtrim(wasub)) as wasub_conv
	,cast(waan8 as [DECIMAL](38, 4)) as waan8
	,cast(waano as [DECIMAL](38, 4)) as waano
	,cast(waansa as [DECIMAL](38, 4)) as waansa
	,cast(waanpa as [DECIMAL](38, 4)) as waanpa
	,cast(waanp as [DECIMAL](38, 4)) as waanp
	,cast(wadpl as [INT]) as wadpl
	,case when cast(wadpl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wadpl as [INT]) %1000, dateadd(year, cast(wadpl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wadpl_conv
	,cast(waant as [DECIMAL](38, 4)) as waant
	,cast(wanan8 as [DECIMAL](38, 4)) as wanan8
	,cast(watrdj as [INT]) as watrdj
	,case when cast(watrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(watrdj as [INT]) %1000, dateadd(year, cast(watrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as watrdj_conv
	,cast(wastrt as [INT]) as wastrt
	,case when cast(wastrt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wastrt as [INT]) %1000, dateadd(year, cast(wastrt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wastrt_conv
	,cast(wadrqj as [INT]) as wadrqj
	,case when cast(wadrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wadrqj as [INT]) %1000, dateadd(year, cast(wadrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wadrqj_conv
	,cast(wastrx as [INT]) as wastrx
	,case when cast(wastrx as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wastrx as [INT]) %1000, dateadd(year, cast(wastrx as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wastrx_conv
	,cast(wadap as [INT]) as wadap
	,case when cast(wadap as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wadap as [INT]) %1000, dateadd(year, cast(wadap as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wadap_conv
	,cast(wadat as [INT]) as wadat
	,case when cast(wadat as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wadat as [INT]) %1000, dateadd(year, cast(wadat as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wadat_conv
	,cast(wappdt as [INT]) as wappdt
	,case when cast(wappdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wappdt as [INT]) %1000, dateadd(year, cast(wappdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wappdt_conv
	,cast(wawr01 as [NVARCHAR](4)) as wawr01
	,cast(wawr02 as [NVARCHAR](3)) as wawr02
	,cast(wawr03 as [NVARCHAR](3)) as wawr03
	,cast(wawr04 as [NVARCHAR](3)) as wawr04
	,cast(wawr05 as [NVARCHAR](3)) as wawr05
	,cast(wawr06 as [NVARCHAR](3)) as wawr06
	,cast(wawr07 as [NVARCHAR](3)) as wawr07
	,cast(wawr08 as [NVARCHAR](3)) as wawr08
	,cast(wawr09 as [NVARCHAR](3)) as wawr09
	,cast(wawr10 as [NVARCHAR](3)) as wawr10
	,wavr01 as wavr01
	,ltrim(rtrim(wavr01)) as wavr01_conv
	,wavr02 as wavr02
	,ltrim(rtrim(wavr02)) as wavr02_conv
	,cast(waamto as [DECIMAL](38, 4)) as waamto
	,cast(waamto as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waamto_conv
	,cast(wasetc as [DECIMAL](38, 4)) as wasetc
	,cast(wabrt as [DECIMAL](38, 4)) as wabrt
	,cast(wabrt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wabrt_conv
	,wapayt as wapayt
	,ltrim(rtrim(wapayt)) as wapayt_conv
	,cast(waamtc as [DECIMAL](38, 4)) as waamtc
	,cast(waamtc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waamtc_conv
	,cast(wahrso as [DECIMAL](38, 4)) as wahrso
	,cast(wahrso as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wahrso_conv
	,cast(wahrsc as [DECIMAL](38, 4)) as wahrsc
	,cast(wahrsc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wahrsc_conv
	,cast(waamta as [DECIMAL](38, 4)) as waamta
	,cast(waamta as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waamta_conv
	,cast(wahrsa as [DECIMAL](38, 4)) as wahrsa
	,cast(wahrsa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wahrsa_conv
	,cast(waitm as [DECIMAL](38, 4)) as waitm
	,waaitm as waaitm
	,ltrim(rtrim(waaitm)) as waaitm_conv
	,walitm as walitm
	,ltrim(rtrim(walitm)) as walitm_conv
	,cast(wanumb as [DECIMAL](38, 4)) as wanumb
	,waapid as waapid
	,ltrim(rtrim(waapid)) as waapid_conv
	,cast(wauorg as [DECIMAL](38, 4)) as wauorg
	,cast(wauorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wauorg_conv
	,cast(wasobk as [DECIMAL](38, 4)) as wasobk
	,cast(wasobk as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wasobk_conv
	,cast(wasocn as [DECIMAL](38, 4)) as wasocn
	,cast(wasocn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wasocn_conv
	,cast(wasoqs as [DECIMAL](38, 4)) as wasoqs
	,cast(wasoqs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wasoqs_conv
	,cast(waqtyt as [DECIMAL](38, 4)) as waqtyt
	,cast(waqtyt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as waqtyt_conv
	,cast(wauom as [NVARCHAR](2)) as wauom
	,washno as washno
	,ltrim(rtrim(washno)) as washno_conv
	,cast(wapbtm as [DECIMAL](38, 4)) as wapbtm
	,cast(watbm as [NVARCHAR](3)) as watbm
	,cast(watrt as [NVARCHAR](3)) as watrt
	,cast(washty as [NVARCHAR](1)) as washty
	,cast(wapec as [NVARCHAR](1)) as wapec
	,cast(wappfg as [NVARCHAR](1)) as wappfg
	,wabm as wabm
	,ltrim(rtrim(wabm)) as wabm_conv
	,wartg as wartg
	,ltrim(rtrim(wartg)) as wartg_conv
	,cast(wasprt as [NVARCHAR](1)) as wasprt
	,cast(wauncd as [NVARCHAR](1)) as wauncd
	,waindc as waindc
	,ltrim(rtrim(waindc)) as waindc_conv
	,cast(waresc as [DECIMAL](38, 4)) as waresc
	,cast(waresc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waresc_conv
	,cast(wamoh as [DECIMAL](38, 4)) as wamoh
	,cast(wamoh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wamoh_conv
	,cast(watdt as [INT]) as watdt
	,case when cast(watdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(watdt as [INT]) %1000, dateadd(year, cast(watdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as watdt_conv
	,cast(wapou as [DECIMAL](38, 4)) as wapou
	,cast(wapou as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wapou_conv
	,cast(wapc as [DECIMAL](38, 4)) as wapc
	,cast(waltlv as [DECIMAL](38, 4)) as waltlv
	,cast(waltlv as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as waltlv_conv
	,cast(waltcm as [DECIMAL](38, 4)) as waltcm
	,cast(waltcm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as waltcm_conv
	,cast(wacts1 as [DECIMAL](38, 4)) as wacts1
	,cast(wacts1 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wacts1_conv
	,walotn as walotn
	,ltrim(rtrim(walotn)) as walotn_conv
	,cast(walotp as [DECIMAL](38, 4)) as walotp
	,cast(walotp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as walotp_conv
	,cast(walotg as [NVARCHAR](3)) as walotg
	,cast(warat1 as [DECIMAL](38, 4)) as warat1
	,cast(warat1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as warat1_conv
	,cast(warat2 as [DECIMAL](38, 4)) as warat2
	,cast(warat2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as warat2_conv
	,cast(wadct as [NVARCHAR](2)) as wadct
	,cast(wasbli as [NVARCHAR](1)) as wasbli
	,warkco as warkco
	,ltrim(rtrim(warkco)) as warkco_conv
	,wabrev as wabrev
	,ltrim(rtrim(wabrev)) as wabrev_conv
	,warrev as warrev
	,ltrim(rtrim(warrev)) as warrev_conv
	,wadrwc as wadrwc
	,ltrim(rtrim(wadrwc)) as wadrwc_conv
	,wartch as wartch
	,ltrim(rtrim(wartch)) as wartch_conv
	,wapnrq as wapnrq
	,ltrim(rtrim(wapnrq)) as wapnrq_conv
	,cast(wareas as [NVARCHAR](2)) as wareas
	,cast(waphse as [NVARCHAR](3)) as waphse
	,cast(waxdsp as [NVARCHAR](3)) as waxdsp
	,wabomc as wabomc
	,ltrim(rtrim(wabomc)) as wabomc_conv
	,waurcd as waurcd
	,ltrim(rtrim(waurcd)) as waurcd_conv
	,cast(waurdt as [INT]) as waurdt
	,case when cast(waurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(waurdt as [INT]) %1000, dateadd(year, cast(waurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as waurdt_conv
	,cast(waurat as [DECIMAL](38, 4)) as waurat
	,cast(waurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waurat_conv
	,cast(waurab as [DECIMAL](38, 4)) as waurab
	,cast(waurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as waurab_conv
	,waurrf as waurrf
	,ltrim(rtrim(waurrf)) as waurrf_conv
	,wauser as wauser
	,ltrim(rtrim(wauser)) as wauser_conv
	,wapid as wapid
	,ltrim(rtrim(wapid)) as wapid_conv
	,wajobn as wajobn
	,ltrim(rtrim(wajobn)) as wajobn_conv
	,cast(waupmj as [INT]) as waupmj
	,case when cast(waupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(waupmj as [INT]) %1000, dateadd(year, cast(waupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as waupmj_conv
	,cast(watday as [DECIMAL](38, 4)) as watday
	,cast(waaaid as [DECIMAL](38, 4)) as waaaid
	,cast(wantst as [NVARCHAR](2)) as wantst
	,cast(waxrto as [DECIMAL](38, 4)) as waxrto
	,cast(waxrto as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as waxrto_conv
	,cast(waesdn as [DECIMAL](38, 4)) as waesdn
	,cast(waesdn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waesdn_conv
	,cast(waacdn as [DECIMAL](38, 4)) as waacdn
	,cast(waacdn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waacdn_conv
	,cast(wasaid as [DECIMAL](38, 4)) as wasaid
	,cast(wampos as [DECIMAL](38, 4)) as wampos
	,cast(wampos as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wampos_conv
	,cast(waaprt as [NVARCHAR](3)) as waaprt
	,cast(waamlc as [DECIMAL](38, 4)) as waamlc
	,cast(waamlc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waamlc_conv
	,cast(waammc as [DECIMAL](38, 4)) as waammc
	,cast(waammc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waammc_conv
	,cast(waamot as [DECIMAL](38, 4)) as waamot
	,cast(waamot as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as waamot_conv
	,cast(walbam as [DECIMAL](38, 4)) as walbam
	,cast(walbam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as walbam_conv
	,cast(wamtam as [DECIMAL](38, 4)) as wamtam
	,cast(wamtam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wamtam_conv
FROM 
    stg_jde.tmp_f4801_init
OPTION ( LABEL = 'CREATE_rep_jde.f4801_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4801_sys_integration_id ON rep_jde.f4801_new(sys_integration_id); 
