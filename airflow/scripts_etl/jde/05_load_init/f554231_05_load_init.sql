-- Create rep new table for init
IF OBJECT_ID('rep_jde.f554231_new') IS NOT NULL
    DROP TABLE rep_jde.f554231_new


CREATE TABLE rep_jde.f554231_new
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
	,cast(szan83 as [DECIMAL](38, 4)) as szan83
	,cast(szdoc as [DECIMAL](38, 4)) as szdoc
	,cast(szaddj as [INT]) as szaddj
	,case when cast(szaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szaddj as [INT]) %1000, dateadd(year, cast(szaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szaddj_conv
	,cast(szcmtb as [INT]) as szcmtb
	,case when cast(szcmtb as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szcmtb as [INT]) %1000, dateadd(year, cast(szcmtb as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szcmtb_conv
	,cast(szptmb as [DECIMAL](38, 4)) as szptmb
	,cast(szcmte as [INT]) as szcmte
	,case when cast(szcmte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szcmte as [INT]) %1000, dateadd(year, cast(szcmte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szcmte_conv
	,cast(szptme as [DECIMAL](38, 4)) as szptme
	,sztx1 as sztx1
	,ltrim(rtrim(sztx1)) as sztx1_conv
	,cast(szlnid as [DECIMAL](38, 4)) as szlnid
	,cast(szlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szlnid_conv
	,cast(szaexp as [DECIMAL](38, 4)) as szaexp
	,cast(szaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szaexp_conv
	,cast(szuprc as [DECIMAL](38, 4)) as szuprc
	,cast(szuprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as szuprc_conv
	,cast(szecst as [DECIMAL](38, 4)) as szecst
	,cast(szecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szecst_conv
	,cast(szlprc as [DECIMAL](38, 4)) as szlprc
	,cast(szlprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as szlprc_conv
	,cast(sztcst as [DECIMAL](38, 4)) as sztcst
	,cast(sztcst as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as sztcst_conv
	,cast(szaopn as [DECIMAL](38, 4)) as szaopn
	,cast(szaopn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szaopn_conv
	,szdl02 as szdl02
	,ltrim(rtrim(szdl02)) as szdl02_conv
	,szdods as szdods
	,ltrim(rtrim(szdods)) as szdods_conv
	,szcrcd as szcrcd
	,ltrim(rtrim(szcrcd)) as szcrcd_conv
	,sztdty as sztdty
	,ltrim(rtrim(sztdty)) as sztdty_conv
	,sztx2 as sztx2
	,ltrim(rtrim(sztx2)) as sztx2_conv
	,cast(szan81 as [DECIMAL](38, 4)) as szan81
	,cast(szan82 as [DECIMAL](38, 4)) as szan82
	,szafit as szafit
	,ltrim(rtrim(szafit)) as szafit_conv
	,szgfl3 as szgfl3
	,ltrim(rtrim(szgfl3)) as szgfl3_conv
	,szlitm as szlitm
	,ltrim(rtrim(szlitm)) as szlitm_conv
	,szcsht as szcsht
	,ltrim(rtrim(szcsht)) as szcsht_conv
	,sztkid as sztkid
	,ltrim(rtrim(sztkid)) as sztkid_conv
	,cast(szuorg as [DECIMAL](38, 4)) as szuorg
	,cast(szuorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as szuorg_conv
	,cast(szukid as [DECIMAL](38, 4)) as szukid
	,cast(szctr as [NVARCHAR](3)) as szctr
	,szaccd21 as szaccd21
	,ltrim(rtrim(szaccd21)) as szaccd21_conv
	,cast(szshan as [DECIMAL](38, 4)) as szshan
	,cast(szan8 as [DECIMAL](38, 4)) as szan8
	,cast(szvend as [DECIMAL](38, 4)) as szvend
	,cast(szvend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as szvend_conv
	,szemcu as szemcu
	,ltrim(rtrim(szemcu)) as szemcu_conv
	,szmcu as szmcu
	,ltrim(rtrim(szmcu)) as szmcu_conv
	,sza801 as sza801
	,ltrim(rtrim(sza801)) as sza801_conv
	,szlocn as szlocn
	,ltrim(rtrim(szlocn)) as szlocn_conv
	,cast(szttem as [DECIMAL](38, 4)) as szttem
	,cast(szttem as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szttem_conv
	,cast(szstpu as [NVARCHAR](1)) as szstpu
	,cast(szdend as [DECIMAL](38, 4)) as szdend
	,cast(szdend as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as szdend_conv
	,cast(szdntp as [NVARCHAR](1)) as szdntp
	,szlnty as szlnty
	,ltrim(rtrim(szlnty)) as szlnty_conv
	,szgfl5 as szgfl5
	,ltrim(rtrim(szgfl5)) as szgfl5_conv
	,szgfl4 as szgfl4
	,ltrim(rtrim(szgfl4)) as szgfl4_conv
	,szgfl6 as szgfl6
	,ltrim(rtrim(szgfl6)) as szgfl6_conv
	,szekco as szekco
	,ltrim(rtrim(szekco)) as szekco_conv
	,szedct as szedct
	,ltrim(rtrim(szedct)) as szedct_conv
	,cast(szedoc as [DECIMAL](38, 4)) as szedoc
	,cast(szedln as [DECIMAL](38, 4)) as szedln
	,cast(szedln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szedln_conv
	,szedsp as szedsp
	,ltrim(rtrim(szedsp)) as szedsp_conv
	,szokco as szokco
	,ltrim(rtrim(szokco)) as szokco_conv
	,szoorn as szoorn
	,ltrim(rtrim(szoorn)) as szoorn_conv
	,cast(szocto as [NVARCHAR](2)) as szocto
	,cast(szogno as [DECIMAL](38, 4)) as szogno
	,cast(szogno as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szogno_conv
	,szgfl7 as szgfl7
	,ltrim(rtrim(szgfl7)) as szgfl7_conv
	,szkcoo as szkcoo
	,ltrim(rtrim(szkcoo)) as szkcoo_conv
	,cast(szdoco as [DECIMAL](38, 4)) as szdoco
	,szcitm as szcitm
	,ltrim(rtrim(szcitm)) as szcitm_conv
	,cast(szdcto as [NVARCHAR](2)) as szdcto
	,cast(szlnic as [DECIMAL](38, 4)) as szlnic
	,cast(szlnic as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szlnic_conv
	,szgfl8 as szgfl8
	,ltrim(rtrim(szgfl8)) as szgfl8_conv
	,szrkco as szrkco
	,ltrim(rtrim(szrkco)) as szrkco_conv
	,szrorn as szrorn
	,ltrim(rtrim(szrorn)) as szrorn_conv
	,cast(szrcto as [NVARCHAR](2)) as szrcto
	,cast(szrlln as [DECIMAL](38, 4)) as szrlln
	,cast(szrlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szrlln_conv
	,szgfl2 as szgfl2
	,ltrim(rtrim(szgfl2)) as szgfl2_conv
	,szgflag1 as szgflag1
	,ltrim(rtrim(szgflag1)) as szgflag1_conv
	,szgflag2 as szgflag2
	,ltrim(rtrim(szgflag2)) as szgflag2_conv
	,szgflg as szgflg
	,ltrim(rtrim(szgflg)) as szgflg_conv
	,cast(szdtbs as [NVARCHAR](1)) as szdtbs
	,cast(szbbdj as [INT]) as szbbdj
	,case when cast(szbbdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szbbdj as [INT]) %1000, dateadd(year, cast(szbbdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szbbdj_conv
	,cast(szecod as [INT]) as szecod
	,case when cast(szecod as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szecod as [INT]) %1000, dateadd(year, cast(szecod as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szecod_conv
	,szedck as szedck
	,ltrim(rtrim(szedck)) as szedck_conv
	,cast(szurat as [DECIMAL](38, 4)) as szurat
	,cast(szurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szurat_conv
	,cast(szurab as [DECIMAL](38, 4)) as szurab
	,cast(szurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as szurab_conv
	,cast(szurnum12 as [DECIMAL](38, 4)) as szurnum12
	,cast(szurnum12 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as szurnum12_conv
	,cast(szurnum13 as [DECIMAL](38, 4)) as szurnum13
	,cast(szurnum13 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as szurnum13_conv
	,cast(szaa as [DECIMAL](38, 4)) as szaa
	,cast(szaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szaa_conv
	,cast(szaa1 as [DECIMAL](38, 4)) as szaa1
	,cast(szaa1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szaa1_conv
	,cast(szaa2 as [DECIMAL](38, 4)) as szaa2
	,cast(szaa2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szaa2_conv
	,cast(szaa3 as [DECIMAL](38, 4)) as szaa3
	,cast(szaa3 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szaa3_conv
	,szdsc as szdsc
	,ltrim(rtrim(szdsc)) as szdsc_conv
	,szdsc1 as szdsc1
	,ltrim(rtrim(szdsc1)) as szdsc1_conv
	,szdsc2 as szdsc2
	,ltrim(rtrim(szdsc2)) as szdsc2_conv
	,szdsc3 as szdsc3
	,ltrim(rtrim(szdsc3)) as szdsc3_conv
	,szdsc4 as szdsc4
	,ltrim(rtrim(szdsc4)) as szdsc4_conv
	,cast(szdate01 as [INT]) as szdate01
	,case when cast(szdate01 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szdate01 as [INT]) %1000, dateadd(year, cast(szdate01 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szdate01_conv
	,cast(szdate02 as [INT]) as szdate02
	,case when cast(szdate02 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szdate02 as [INT]) %1000, dateadd(year, cast(szdate02 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szdate02_conv
	,cast(szdate03 as [INT]) as szdate03
	,case when cast(szdate03 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szdate03 as [INT]) %1000, dateadd(year, cast(szdate03 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szdate03_conv
	,cast(szdate04 as [INT]) as szdate04
	,case when cast(szdate04 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szdate04 as [INT]) %1000, dateadd(year, cast(szdate04 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szdate04_conv
	,szcm01 as szcm01
	,ltrim(rtrim(szcm01)) as szcm01_conv
	,szcm02 as szcm02
	,ltrim(rtrim(szcm02)) as szcm02_conv
	,szcm03 as szcm03
	,ltrim(rtrim(szcm03)) as szcm03_conv
	,szcm04 as szcm04
	,ltrim(rtrim(szcm04)) as szcm04_conv
	,szcm05 as szcm05
	,ltrim(rtrim(szcm05)) as szcm05_conv
	,szedtn as szedtn
	,ltrim(rtrim(szedtn)) as szedtn_conv
	,szeffutime as szeffutime
	,szeffutime as szeffutime_conv
	,szeetm as szeetm
	,szeetm as szeetm_conv
	,szedbt as szedbt
	,ltrim(rtrim(szedbt)) as szedbt_conv
	,szgfl1 as szgfl1
	,ltrim(rtrim(szgfl1)) as szgfl1_conv
	,szaitm as szaitm
	,ltrim(rtrim(szaitm)) as szaitm_conv
	,szy55char1 as szy55char1
	,ltrim(rtrim(szy55char1)) as szy55char1_conv
	,szy55char2 as szy55char2
	,ltrim(rtrim(szy55char2)) as szy55char2_conv
	,szy55strg1 as szy55strg1
	,ltrim(rtrim(szy55strg1)) as szy55strg1_conv
	,szy55strg2 as szy55strg2
	,ltrim(rtrim(szy55strg2)) as szy55strg2_conv
	,szy55strg3 as szy55strg3
	,ltrim(rtrim(szy55strg3)) as szy55strg3_conv
	,szy55strg4 as szy55strg4
	,ltrim(rtrim(szy55strg4)) as szy55strg4_conv
	,szy55strg5 as szy55strg5
	,ltrim(rtrim(szy55strg5)) as szy55strg5_conv
	,cast(szy55amnt1 as [DECIMAL](38, 4)) as szy55amnt1
	,cast(szy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szy55amnt1_conv
	,cast(szy55amnt2 as [DECIMAL](38, 4)) as szy55amnt2
	,cast(szy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as szy55amnt2_conv
	,cast(szaaq1 as [DECIMAL](38, 4)) as szaaq1
	,cast(szaaq1 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szaaq1_conv
	,cast(szaaq2 as [DECIMAL](38, 4)) as szaaq2
	,cast(szaaq2 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szaaq2_conv
	,cast(szaaq3 as [DECIMAL](38, 4)) as szaaq3
	,cast(szaaq3 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szaaq3_conv
	,cast(szaaq4 as [DECIMAL](38, 4)) as szaaq4
	,cast(szaaq4 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as szaaq4_conv
	,cast(szy55time1 as [DECIMAL](38, 4)) as szy55time1
	,cast(szy55time2 as [DECIMAL](38, 4)) as szy55time2
	,cast(szy55date1 as [INT]) as szy55date1
	,case when cast(szy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szy55date1 as [INT]) %1000, dateadd(year, cast(szy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szy55date1_conv
	,cast(szy55date2 as [INT]) as szy55date2
	,case when cast(szy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szy55date2 as [INT]) %1000, dateadd(year, cast(szy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szy55date2_conv
	,szuser as szuser
	,ltrim(rtrim(szuser)) as szuser_conv
	,cast(szupmj as [INT]) as szupmj
	,case when cast(szupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(szupmj as [INT]) %1000, dateadd(year, cast(szupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as szupmj_conv
	,cast(szupmt as [DECIMAL](38, 4)) as szupmt
	,szjobn as szjobn
	,ltrim(rtrim(szjobn)) as szjobn_conv
	,szpid as szpid
	,ltrim(rtrim(szpid)) as szpid_conv
FROM 
    stg_jde.tmp_f554231_init
OPTION ( LABEL = 'CREATE_rep_jde.f554231_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554231_sys_integration_id ON rep_jde.f554231_new(sys_integration_id); 
