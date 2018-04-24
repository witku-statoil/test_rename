-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4201_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4201_new


CREATE TABLE rep_jde_qat.f4201_new
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
	,shkcoo as shkcoo
	,ltrim(rtrim(shkcoo)) as shkcoo_conv
	,cast(shdoco as [DECIMAL](38, 4)) as shdoco
	,cast(shdcto as [NVARCHAR](2)) as shdcto
	,shsfxo as shsfxo
	,ltrim(rtrim(shsfxo)) as shsfxo_conv
	,shmcu as shmcu
	,ltrim(rtrim(shmcu)) as shmcu_conv
	,shco as shco
	,ltrim(rtrim(shco)) as shco_conv
	,shokco as shokco
	,ltrim(rtrim(shokco)) as shokco_conv
	,shoorn as shoorn
	,ltrim(rtrim(shoorn)) as shoorn_conv
	,cast(shocto as [NVARCHAR](2)) as shocto
	,shrkco as shrkco
	,ltrim(rtrim(shrkco)) as shrkco_conv
	,shrorn as shrorn
	,ltrim(rtrim(shrorn)) as shrorn_conv
	,cast(shrcto as [NVARCHAR](2)) as shrcto
	,cast(shan8 as [DECIMAL](38, 4)) as shan8
	,cast(shshan as [DECIMAL](38, 4)) as shshan
	,cast(shpa8 as [DECIMAL](38, 4)) as shpa8
	,cast(shdrqj as [INT]) as shdrqj
	,case when cast(shdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shdrqj as [INT]) %1000, dateadd(year, cast(shdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shdrqj_conv
	,cast(shtrdj as [INT]) as shtrdj
	,case when cast(shtrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shtrdj as [INT]) %1000, dateadd(year, cast(shtrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shtrdj_conv
	,cast(shpddj as [INT]) as shpddj
	,case when cast(shpddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shpddj as [INT]) %1000, dateadd(year, cast(shpddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shpddj_conv
	,cast(shopdj as [INT]) as shopdj
	,case when cast(shopdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shopdj as [INT]) %1000, dateadd(year, cast(shopdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shopdj_conv
	,cast(shaddj as [INT]) as shaddj
	,case when cast(shaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shaddj as [INT]) %1000, dateadd(year, cast(shaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shaddj_conv
	,cast(shcndj as [INT]) as shcndj
	,case when cast(shcndj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shcndj as [INT]) %1000, dateadd(year, cast(shcndj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shcndj_conv
	,cast(shpefj as [INT]) as shpefj
	,case when cast(shpefj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shpefj as [INT]) %1000, dateadd(year, cast(shpefj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shpefj_conv
	,cast(shppdj as [INT]) as shppdj
	,case when cast(shppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shppdj as [INT]) %1000, dateadd(year, cast(shppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shppdj_conv
	,shvr01 as shvr01
	,ltrim(rtrim(shvr01)) as shvr01_conv
	,shvr02 as shvr02
	,ltrim(rtrim(shvr02)) as shvr02_conv
	,shdel1 as shdel1
	,ltrim(rtrim(shdel1)) as shdel1_conv
	,shdel2 as shdel2
	,ltrim(rtrim(shdel2)) as shdel2_conv
	,cast(shinmg as [NVARCHAR](10)) as shinmg
	,shptc as shptc
	,ltrim(rtrim(shptc)) as shptc_conv
	,cast(shryin as [NVARCHAR](1)) as shryin
	,cast(shasn as [NVARCHAR](8)) as shasn
	,cast(shprgp as [NVARCHAR](8)) as shprgp
	,cast(shtrdc as [DECIMAL](38, 4)) as shtrdc
	,cast(shtrdc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as shtrdc_conv
	,cast(shpcrt as [DECIMAL](38, 4)) as shpcrt
	,cast(shpcrt as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as shpcrt_conv
	,shtxa1 as shtxa1
	,ltrim(rtrim(shtxa1)) as shtxa1_conv
	,cast(shexr1 as [NVARCHAR](2)) as shexr1
	,shtxct as shtxct
	,ltrim(rtrim(shtxct)) as shtxct_conv
	,shatxt as shatxt
	,ltrim(rtrim(shatxt)) as shatxt_conv
	,cast(shprio as [NVARCHAR](1)) as shprio
	,shback as shback
	,ltrim(rtrim(shback)) as shback_conv
	,shsbal as shsbal
	,ltrim(rtrim(shsbal)) as shsbal_conv
	,cast(shhold as [NVARCHAR](2)) as shhold
	,shplst as shplst
	,ltrim(rtrim(shplst)) as shplst_conv
	,cast(shinvc as [DECIMAL](38, 4)) as shinvc
	,cast(shinvc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shinvc_conv
	,cast(shntr as [NVARCHAR](2)) as shntr
	,cast(shanby as [DECIMAL](38, 4)) as shanby
	,cast(shcars as [DECIMAL](38, 4)) as shcars
	,cast(shcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shcars_conv
	,cast(shmot as [NVARCHAR](3)) as shmot
	,cast(shcot as [NVARCHAR](3)) as shcot
	,cast(shrout as [NVARCHAR](3)) as shrout
	,cast(shstop as [NVARCHAR](3)) as shstop
	,cast(shzon as [NVARCHAR](3)) as shzon
	,shcnid as shcnid
	,ltrim(rtrim(shcnid)) as shcnid_conv
	,cast(shfrth as [NVARCHAR](3)) as shfrth
	,shaft as shaft
	,ltrim(rtrim(shaft)) as shaft_conv
	,cast(shfuf1 as [NVARCHAR](1)) as shfuf1
	,shfrtc as shfrtc
	,ltrim(rtrim(shfrtc)) as shfrtc_conv
	,shmord as shmord
	,ltrim(rtrim(shmord)) as shmord_conv
	,cast(shrcd as [NVARCHAR](3)) as shrcd
	,shfuf2 as shfuf2
	,ltrim(rtrim(shfuf2)) as shfuf2_conv
	,cast(shotot as [DECIMAL](38, 4)) as shotot
	,cast(shotot as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as shotot_conv
	,cast(shtotc as [DECIMAL](38, 4)) as shtotc
	,cast(shtotc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as shtotc_conv
	,cast(shwumd as [NVARCHAR](2)) as shwumd
	,cast(shvumd as [NVARCHAR](2)) as shvumd
	,shautn as shautn
	,ltrim(rtrim(shautn)) as shautn_conv
	,shcact as shcact
	,ltrim(rtrim(shcact)) as shcact_conv
	,cast(shcexp as [INT]) as shcexp
	,case when cast(shcexp as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shcexp as [INT]) %1000, dateadd(year, cast(shcexp as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shcexp_conv
	,cast(shsbli as [NVARCHAR](1)) as shsbli
	,cast(shcrmd as [NVARCHAR](1)) as shcrmd
	,cast(shcrrm as [NVARCHAR](1)) as shcrrm
	,shcrcd as shcrcd
	,ltrim(rtrim(shcrcd)) as shcrcd_conv
	,cast(shcrr as [DECIMAL](38, 4)) as shcrr
	,cast(shlngp as [NVARCHAR](2)) as shlngp
	,cast(shfap as [DECIMAL](38, 4)) as shfap
	,cast(shfap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as shfap_conv
	,cast(shfcst as [DECIMAL](38, 4)) as shfcst
	,cast(shfcst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as shfcst_conv
	,shorby as shorby
	,ltrim(rtrim(shorby)) as shorby_conv
	,shtkby as shtkby
	,ltrim(rtrim(shtkby)) as shtkby_conv
	,shurcd as shurcd
	,ltrim(rtrim(shurcd)) as shurcd_conv
	,cast(shurdt as [INT]) as shurdt
	,case when cast(shurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shurdt as [INT]) %1000, dateadd(year, cast(shurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shurdt_conv
	,cast(shurat as [DECIMAL](38, 4)) as shurat
	,cast(shurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as shurat_conv
	,cast(shurab as [DECIMAL](38, 4)) as shurab
	,cast(shurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shurab_conv
	,shurrf as shurrf
	,ltrim(rtrim(shurrf)) as shurrf_conv
	,shuser as shuser
	,ltrim(rtrim(shuser)) as shuser_conv
	,shpid as shpid
	,ltrim(rtrim(shpid)) as shpid_conv
	,shjobn as shjobn
	,ltrim(rtrim(shjobn)) as shjobn_conv
	,cast(shupmj as [INT]) as shupmj
	,case when cast(shupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shupmj as [INT]) %1000, dateadd(year, cast(shupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shupmj_conv
	,cast(shtday as [DECIMAL](38, 4)) as shtday
	,shir01 as shir01
	,ltrim(rtrim(shir01)) as shir01_conv
	,shir02 as shir02
	,ltrim(rtrim(shir02)) as shir02_conv
	,shir03 as shir03
	,ltrim(rtrim(shir03)) as shir03_conv
	,shir04 as shir04
	,ltrim(rtrim(shir04)) as shir04_conv
	,shir05 as shir05
	,ltrim(rtrim(shir05)) as shir05_conv
	,shvr03 as shvr03
	,ltrim(rtrim(shvr03)) as shvr03_conv
	,cast(shsoor as [INT]) as shsoor
	,cast(shsoor as [INT]) / cast(1 as [INT]) as shsoor_conv
	,cast(shpmdt as [DECIMAL](38, 4)) as shpmdt
	,cast(shrsdt as [DECIMAL](38, 4)) as shrsdt
	,cast(shrqsj as [INT]) as shrqsj
	,case when cast(shrqsj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shrqsj as [INT]) %1000, dateadd(year, cast(shrqsj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shrqsj_conv
	,cast(shpstm as [DECIMAL](38, 4)) as shpstm
	,cast(shpdtt as [DECIMAL](38, 4)) as shpdtt
	,cast(shoptt as [DECIMAL](38, 4)) as shoptt
	,cast(shdrqt as [DECIMAL](38, 4)) as shdrqt
	,cast(shadtm as [DECIMAL](38, 4)) as shadtm
	,cast(shadlj as [INT]) as shadlj
	,case when cast(shadlj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shadlj as [INT]) %1000, dateadd(year, cast(shadlj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shadlj_conv
	,cast(shpban as [DECIMAL](38, 4)) as shpban
	,cast(shpban as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shpban_conv
	,cast(shitan as [DECIMAL](38, 4)) as shitan
	,cast(shitan as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shitan_conv
	,cast(shftan as [DECIMAL](38, 4)) as shftan
	,cast(shftan as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shftan_conv
	,cast(shdvan as [DECIMAL](38, 4)) as shdvan
	,cast(shdvan as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shdvan_conv
	,cast(shdoc1 as [DECIMAL](38, 4)) as shdoc1
	,cast(shdct4 as [NVARCHAR](2)) as shdct4
	,cast(shcord as [DECIMAL](38, 4)) as shcord
	,cast(shbsc as [NVARCHAR](10)) as shbsc
	,shbcrc as shbcrc
	,ltrim(rtrim(shbcrc)) as shbcrc_conv
	,cast(shauft as [NVARCHAR](1)) as shauft
	,cast(shaufi as [NVARCHAR](1)) as shaufi
	,cast(shopbo as [NVARCHAR](30)) as shopbo
	,cast(shoptc as [DECIMAL](38, 4)) as shoptc
	,cast(shoptc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as shoptc_conv
	,cast(shopld as [INT]) as shopld
	,case when cast(shopld as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(shopld as [INT]) %1000, dateadd(year, cast(shopld as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as shopld_conv
	,cast(shopbk as [DECIMAL](38, 4)) as shopbk
	,cast(shopbk as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shopbk_conv
	,cast(shopsb as [DECIMAL](38, 4)) as shopsb
	,cast(shopsb as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shopsb_conv
	,shopps as shopps
	,ltrim(rtrim(shopps)) as shopps_conv
	,shoppl as shoppl
	,ltrim(rtrim(shoppl)) as shoppl_conv
	,shopms as shopms
	,ltrim(rtrim(shopms)) as shopms_conv
	,shopss as shopss
	,ltrim(rtrim(shopss)) as shopss_conv
	,shopba as shopba
	,ltrim(rtrim(shopba)) as shopba_conv
	,shopll as shopll
	,ltrim(rtrim(shopll)) as shopll_conv
	,cast(shpran8 as [DECIMAL](38, 4)) as shpran8
	,cast(shpran8 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shpran8_conv
	,cast(shoppid as [DECIMAL](38, 4)) as shoppid
	,cast(shoppid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shoppid_conv
	,shsdattn as shsdattn
	,ltrim(rtrim(shsdattn)) as shsdattn_conv
	,shspattn as shspattn
	,ltrim(rtrim(shspattn)) as shspattn_conv
	,shotind as shotind
	,ltrim(rtrim(shotind)) as shotind_conv
	,cast(shprcidln as [DECIMAL](38, 4)) as shprcidln
	,cast(shprcidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shprcidln_conv
	,cast(shccidln as [DECIMAL](38, 4)) as shccidln
	,cast(shccidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shccidln_conv
	,cast(shshccidln as [DECIMAL](38, 4)) as shshccidln
	,cast(shshccidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as shshccidln_conv
FROM 
    stg_jde_qat.tmp_f4201_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4201_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4201_sys_integration_id ON rep_jde_qat.f4201_new(sys_integration_id); 
