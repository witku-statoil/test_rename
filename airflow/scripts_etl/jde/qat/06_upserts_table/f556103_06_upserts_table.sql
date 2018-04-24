-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f556103_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f556103_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f556103_cdc
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
	,cast(saitm as [DECIMAL](38, 4)) as saitm
	,cast(sactr as [NVARCHAR](3)) as sactr
	,saflag as saflag
	,ltrim(rtrim(saflag)) as saflag_conv
	,say55prhy as say55prhy
	,ltrim(rtrim(say55prhy)) as say55prhy_conv
	,say55mcat as say55mcat
	,ltrim(rtrim(say55mcat)) as say55mcat_conv
	,cast(say55mnm as [NVARCHAR](8)) as say55mnm
	,sagnnm as sagnnm
	,ltrim(rtrim(sagnnm)) as sagnnm_conv
	,cast(sanevo as [DECIMAL](38, 4)) as sanevo
	,cast(sanevo as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sanevo_conv
	,cast(saprouom as [NVARCHAR](2)) as saprouom
	,cast(saliqv as [DECIMAL](38, 4)) as saliqv
	,cast(saliqv as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saliqv_conv
	,cast(savolum as [NVARCHAR](2)) as savolum
	,cast(say55tcat as [NVARCHAR](6)) as say55tcat
	,saactiveyn as saactiveyn
	,ltrim(rtrim(saactiveyn)) as saactiveyn_conv
	,samkt3 as samkt3
	,ltrim(rtrim(samkt3)) as samkt3_conv
	,sapr016 as sapr016
	,ltrim(rtrim(sapr016)) as sapr016_conv
	,cast(say55ilcl as [NVARCHAR](9)) as say55ilcl
	,say55csif as say55csif
	,ltrim(rtrim(say55csif)) as say55csif_conv
	,cast(sagdep as [DECIMAL](38, 4)) as sagdep
	,cast(sagdep as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sagdep_conv
	,cast(sagwid as [DECIMAL](38, 4)) as sagwid
	,cast(sagwid as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sagwid_conv
	,cast(saghet as [DECIMAL](38, 4)) as saghet
	,cast(saghet as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saghet_conv
	,cast(sadimuom as [NVARCHAR](2)) as sadimuom
	,cast(say55clu01 as [NVARCHAR](8)) as say55clu01
	,cast(say55clu02 as [NVARCHAR](8)) as say55clu02
	,cast(say55clu03 as [NVARCHAR](8)) as say55clu03
	,cast(say55clu04 as [NVARCHAR](8)) as say55clu04
	,cast(say55clu05 as [NVARCHAR](8)) as say55clu05
	,cast(say55clu06 as [NVARCHAR](8)) as say55clu06
	,cast(say55clu07 as [NVARCHAR](8)) as say55clu07
	,cast(say55clu08 as [NVARCHAR](8)) as say55clu08
	,cast(say55clu09 as [NVARCHAR](8)) as say55clu09
	,cast(say55clu10 as [NVARCHAR](8)) as say55clu10
	,cast(say55clu11 as [NVARCHAR](8)) as say55clu11
	,cast(say55clu12 as [NVARCHAR](8)) as say55clu12
	,cast(say55clu13 as [NVARCHAR](8)) as say55clu13
	,cast(say55clu14 as [NVARCHAR](8)) as say55clu14
	,cast(say55clu15 as [NVARCHAR](8)) as say55clu15
	,sapmfg as sapmfg
	,ltrim(rtrim(sapmfg)) as sapmfg_conv
	,sadl01 as sadl01
	,ltrim(rtrim(sadl01)) as sadl01_conv
	,sadl02 as sadl02
	,ltrim(rtrim(sadl02)) as sadl02_conv
	,sadl03 as sadl03
	,ltrim(rtrim(sadl03)) as sadl03_conv
	,sagfl1 as sagfl1
	,ltrim(rtrim(sagfl1)) as sagfl1_conv
	,sagfl2 as sagfl2
	,ltrim(rtrim(sagfl2)) as sagfl2_conv
	,sagfl3 as sagfl3
	,ltrim(rtrim(sagfl3)) as sagfl3_conv
	,sagfl4 as sagfl4
	,ltrim(rtrim(sagfl4)) as sagfl4_conv
	,sagfl5 as sagfl5
	,ltrim(rtrim(sagfl5)) as sagfl5_conv
	,cast(sadate01 as [INT]) as sadate01
	,case when cast(sadate01 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sadate01 as [INT]) %1000, dateadd(year, cast(sadate01 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sadate01_conv
	,cast(sadate02 as [INT]) as sadate02
	,case when cast(sadate02 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sadate02 as [INT]) %1000, dateadd(year, cast(sadate02 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sadate02_conv
	,cast(sadate03 as [INT]) as sadate03
	,case when cast(sadate03 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sadate03 as [INT]) %1000, dateadd(year, cast(sadate03 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sadate03_conv
	,sadesc3 as sadesc3
	,ltrim(rtrim(sadesc3)) as sadesc3_conv
	,sadesc4 as sadesc4
	,ltrim(rtrim(sadesc4)) as sadesc4_conv
	,sadesc5 as sadesc5
	,ltrim(rtrim(sadesc5)) as sadesc5_conv
	,sadesc6 as sadesc6
	,ltrim(rtrim(sadesc6)) as sadesc6_conv
	,sadesc7 as sadesc7
	,ltrim(rtrim(sadesc7)) as sadesc7_conv
	,cast(saaa as [DECIMAL](38, 4)) as saaa
	,cast(saaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saaa_conv
	,cast(saaa1 as [DECIMAL](38, 4)) as saaa1
	,cast(saaa1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saaa1_conv
	,cast(saaa2 as [DECIMAL](38, 4)) as saaa2
	,cast(saaa2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saaa2_conv
	,cast(saaa3 as [DECIMAL](38, 4)) as saaa3
	,cast(saaa3 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saaa3_conv
	,cast(samath06 as [DECIMAL](38, 4)) as samath06
	,cast(samath06 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as samath06_conv
	,cast(samath07 as [DECIMAL](38, 4)) as samath07
	,cast(samath07 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as samath07_conv
	,cast(samath08 as [DECIMAL](38, 4)) as samath08
	,cast(samath08 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as samath08_conv
	,cast(samath09 as [DECIMAL](38, 4)) as samath09
	,cast(samath09 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as samath09_conv
	,cast(samath10 as [DECIMAL](38, 4)) as samath10
	,cast(samath10 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as samath10_conv
	,cast(saurab as [DECIMAL](38, 4)) as saurab
	,cast(saurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as saurab_conv
	,cast(saurdt as [INT]) as saurdt
	,case when cast(saurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(saurdt as [INT]) %1000, dateadd(year, cast(saurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as saurdt_conv
	,cast(saurat as [DECIMAL](38, 4)) as saurat
	,cast(saurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saurat_conv
	,saurrf as saurrf
	,ltrim(rtrim(saurrf)) as saurrf_conv
	,saurcd as saurcd
	,ltrim(rtrim(saurcd)) as saurcd_conv
	,sauser as sauser
	,ltrim(rtrim(sauser)) as sauser_conv
	,sapid as sapid
	,ltrim(rtrim(sapid)) as sapid_conv
	,samkey as samkey
	,ltrim(rtrim(samkey)) as samkey_conv
	,cast(saupmj as [INT]) as saupmj
	,case when cast(saupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(saupmj as [INT]) %1000, dateadd(year, cast(saupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as saupmj_conv
	,cast(saupmt as [DECIMAL](38, 4)) as saupmt
	,sacm01 as sacm01
	,ltrim(rtrim(sacm01)) as sacm01_conv
	,sacm02 as sacm02
	,ltrim(rtrim(sacm02)) as sacm02_conv
	,sacm03 as sacm03
	,ltrim(rtrim(sacm03)) as sacm03_conv
	,sacm04 as sacm04
	,ltrim(rtrim(sacm04)) as sacm04_conv
	,sacm05 as sacm05
	,ltrim(rtrim(sacm05)) as sacm05_conv
	,sadl05 as sadl05
	,ltrim(rtrim(sadl05)) as sadl05_conv
	,sadl06 as sadl06
	,ltrim(rtrim(sadl06)) as sadl06_conv
	,sadl07 as sadl07
	,ltrim(rtrim(sadl07)) as sadl07_conv
	,sadl08 as sadl08
	,ltrim(rtrim(sadl08)) as sadl08_conv
	,sadl09 as sadl09
	,ltrim(rtrim(sadl09)) as sadl09_conv
	,cast(say55ascd as [NVARCHAR](8)) as say55ascd
	,saev01 as saev01
	,ltrim(rtrim(saev01)) as saev01_conv
	,saev02 as saev02
	,ltrim(rtrim(saev02)) as saev02_conv
	,saev03 as saev03
	,ltrim(rtrim(saev03)) as saev03_conv
	,saev04 as saev04
	,ltrim(rtrim(saev04)) as saev04_conv
	,saev05 as saev05
	,ltrim(rtrim(saev05)) as saev05_conv
	,cast(saua02 as [INT]) as saua02
	,case when cast(saua02 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(saua02 as [INT]) %1000, dateadd(year, cast(saua02 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as saua02_conv
	,cast(saua03 as [INT]) as saua03
	,case when cast(saua03 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(saua03 as [INT]) %1000, dateadd(year, cast(saua03 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as saua03_conv
	,cast(saua04 as [INT]) as saua04
	,case when cast(saua04 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(saua04 as [INT]) %1000, dateadd(year, cast(saua04 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as saua04_conv
	,cast(saua05 as [INT]) as saua05
	,case when cast(saua05 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(saua05 as [INT]) %1000, dateadd(year, cast(saua05 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as saua05_conv
	,say55salbr as say55salbr
	,ltrim(rtrim(say55salbr)) as say55salbr_conv
	,say55purbr as say55purbr
	,ltrim(rtrim(say55purbr)) as say55purbr_conv
	,cast(say55posrp as [NVARCHAR](4)) as say55posrp
	,saky as saky
	,ltrim(rtrim(saky)) as saky_conv
	,saev06 as saev06
	,ltrim(rtrim(saev06)) as saev06_conv
	,saev07 as saev07
	,ltrim(rtrim(saev07)) as saev07_conv
	,saev08 as saev08
	,ltrim(rtrim(saev08)) as saev08_conv
	,saev09 as saev09
	,ltrim(rtrim(saev09)) as saev09_conv
	,saev10 as saev10
	,ltrim(rtrim(saev10)) as saev10_conv
	,say55rctxt as say55rctxt
	,ltrim(rtrim(say55rctxt)) as say55rctxt_conv
	,cast(say55prchg as [NVARCHAR](5)) as say55prchg
	,cast(say55necon as [DECIMAL](38, 4)) as say55necon
	,cast(say55necon as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as say55necon_conv
	,cast(sancooc as [NVARCHAR](3)) as sancooc
	,cast(satax1 as [NVARCHAR](1)) as satax1
	,saaca1 as saaca1
	,ltrim(rtrim(saaca1)) as saaca1_conv
	,cast(saaap1 as [DECIMAL](38, 4)) as saaap1
	,cast(saaap1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saaap1_conv
	,cast(saaap2 as [DECIMAL](38, 4)) as saaap2
	,cast(saaap2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saaap2_conv
	,cast(saaap3 as [DECIMAL](38, 4)) as saaap3
	,cast(saaap3 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saaap3_conv
	,cast(sak001 as [NVARCHAR](1)) as sak001
	,cast(sak002 as [NVARCHAR](1)) as sak002
	,cast(sak003 as [NVARCHAR](1)) as sak003
	,cast(sak004 as [NVARCHAR](1)) as sak004
	,sadscrp as sadscrp
	,ltrim(rtrim(sadscrp)) as sadscrp_conv
	,sadscrp1 as sadscrp1
	,ltrim(rtrim(sadscrp1)) as sadscrp1_conv
	,sadscrp2 as sadscrp2
	,ltrim(rtrim(sadscrp2)) as sadscrp2_conv
	,sadscrp3 as sadscrp3
	,ltrim(rtrim(sadscrp3)) as sadscrp3_conv
	,sadscrp4 as sadscrp4
	,ltrim(rtrim(sadscrp4)) as sadscrp4_conv
	,sadscrp5 as sadscrp5
	,ltrim(rtrim(sadscrp5)) as sadscrp5_conv
	,cast(saba01 as [DECIMAL](38, 4)) as saba01
	,cast(saba01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saba01_conv
	,cast(saba02 as [DECIMAL](38, 4)) as saba02
	,cast(saba02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saba02_conv
	,cast(saba03 as [DECIMAL](38, 4)) as saba03
	,cast(saba03 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saba03_conv
	,cast(satxb as [NVARCHAR](1)) as satxb
	,say55bycrc as say55bycrc
	,ltrim(rtrim(say55bycrc)) as say55bycrc_conv
	,say55rewitm as say55rewitm
	,ltrim(rtrim(say55rewitm)) as say55rewitm_conv
	,say55suppid as say55suppid
	,ltrim(rtrim(say55suppid)) as say55suppid_conv
	,say55catlcod as say55catlcod
	,ltrim(rtrim(say55catlcod)) as say55catlcod_conv
	,say55supcode as say55supcode
	,ltrim(rtrim(say55supcode)) as say55supcode_conv
	,cast(say55uprc as [DECIMAL](38, 4)) as say55uprc
	,cast(say55uprc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as say55uprc_conv
	,say55loydesc as say55loydesc
	,ltrim(rtrim(say55loydesc)) as say55loydesc_conv
	,say55ccobom as say55ccobom
	,ltrim(rtrim(say55ccobom)) as say55ccobom_conv
	,cast(say55alcpc as [DECIMAL](38, 4)) as say55alcpc
	,cast(say55alcpc as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as say55alcpc_conv
	,cast(saagpr as [DECIMAL](38, 4)) as saagpr
	,cast(saagpr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as saagpr_conv
	,cast(say55rnum1 as [DECIMAL](38, 4)) as say55rnum1
	,cast(say55rnum1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as say55rnum1_conv
	,cast(say55rnum2 as [DECIMAL](38, 4)) as say55rnum2
	,cast(say55rnum2 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as say55rnum2_conv
	,cast(say55rnum3 as [DECIMAL](38, 4)) as say55rnum3
	,cast(say55rnum3 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as say55rnum3_conv
	,cast(say55rnum4 as [DECIMAL](38, 4)) as say55rnum4
	,cast(say55rnum4 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as say55rnum4_conv
	,cast(say55rnum5 as [DECIMAL](38, 4)) as say55rnum5
	,cast(say55rnum5 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as say55rnum5_conv
	,cast(say55rdte1 as [INT]) as say55rdte1
	,case when cast(say55rdte1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(say55rdte1 as [INT]) %1000, dateadd(year, cast(say55rdte1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as say55rdte1_conv
	,cast(say55rtme1 as [DECIMAL](38, 4)) as say55rtme1
	,cast(say55rtme1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as say55rtme1_conv
	,cast(say55rdte2 as [INT]) as say55rdte2
	,case when cast(say55rdte2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(say55rdte2 as [INT]) %1000, dateadd(year, cast(say55rdte2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as say55rdte2_conv
	,cast(say55rtme2 as [DECIMAL](38, 4)) as say55rtme2
	,cast(say55rtme2 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as say55rtme2_conv
	,cast(say55rdte3 as [INT]) as say55rdte3
	,case when cast(say55rdte3 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(say55rdte3 as [INT]) %1000, dateadd(year, cast(say55rdte3 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as say55rdte3_conv
	,cast(say55rtme3 as [DECIMAL](38, 4)) as say55rtme3
	,cast(say55rtme3 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as say55rtme3_conv
	,say55rchr1 as say55rchr1
	,ltrim(rtrim(say55rchr1)) as say55rchr1_conv
	,say55rchr2 as say55rchr2
	,ltrim(rtrim(say55rchr2)) as say55rchr2_conv
	,say55rchr3 as say55rchr3
	,ltrim(rtrim(say55rchr3)) as say55rchr3_conv
	,say55rstr1 as say55rstr1
	,ltrim(rtrim(say55rstr1)) as say55rstr1_conv
	,say55rstr2 as say55rstr2
	,ltrim(rtrim(say55rstr2)) as say55rstr2_conv
	,say55rstr3 as say55rstr3
	,ltrim(rtrim(say55rstr3)) as say55rstr3_conv
	,say55rstr4 as say55rstr4
	,ltrim(rtrim(say55rstr4)) as say55rstr4_conv
	,say55rstr5 as say55rstr5
	,ltrim(rtrim(say55rstr5)) as say55rstr5_conv
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
	,saitm
	,sactr
	,saflag
	,say55prhy
	,say55mcat
	,say55mnm
	,sagnnm
	,sanevo
	,saprouom
	,saliqv
	,savolum
	,say55tcat
	,saactiveyn
	,samkt3
	,sapr016
	,say55ilcl
	,say55csif
	,sagdep
	,sagwid
	,saghet
	,sadimuom
	,say55clu01
	,say55clu02
	,say55clu03
	,say55clu04
	,say55clu05
	,say55clu06
	,say55clu07
	,say55clu08
	,say55clu09
	,say55clu10
	,say55clu11
	,say55clu12
	,say55clu13
	,say55clu14
	,say55clu15
	,sapmfg
	,sadl01
	,sadl02
	,sadl03
	,sagfl1
	,sagfl2
	,sagfl3
	,sagfl4
	,sagfl5
	,sadate01
	,sadate02
	,sadate03
	,sadesc3
	,sadesc4
	,sadesc5
	,sadesc6
	,sadesc7
	,saaa
	,saaa1
	,saaa2
	,saaa3
	,samath06
	,samath07
	,samath08
	,samath09
	,samath10
	,saurab
	,saurdt
	,saurat
	,saurrf
	,saurcd
	,sauser
	,sapid
	,samkey
	,saupmj
	,saupmt
	,sacm01
	,sacm02
	,sacm03
	,sacm04
	,sacm05
	,sadl05
	,sadl06
	,sadl07
	,sadl08
	,sadl09
	,say55ascd
	,saev01
	,saev02
	,saev03
	,saev04
	,saev05
	,saua02
	,saua03
	,saua04
	,saua05
	,say55salbr
	,say55purbr
	,say55posrp
	,saky
	,saev06
	,saev07
	,saev08
	,saev09
	,saev10
	,say55rctxt
	,say55prchg
	,say55necon
	,sancooc
	,satax1
	,saaca1
	,saaap1
	,saaap2
	,saaap3
	,sak001
	,sak002
	,sak003
	,sak004
	,sadscrp
	,sadscrp1
	,sadscrp2
	,sadscrp3
	,sadscrp4
	,sadscrp5
	,saba01
	,saba02
	,saba03
	,satxb
	,say55bycrc
	,say55rewitm
	,say55suppid
	,say55catlcod
	,say55supcode
	,say55uprc
	,say55loydesc
	,say55ccobom
	,say55alcpc
	,saagpr
	,say55rnum1
	,say55rnum2
	,say55rnum3
	,say55rnum4
	,say55rnum5
	,say55rdte1
	,say55rtme1
	,say55rdte2
	,say55rtme2
	,say55rdte3
	,say55rtme3
	,say55rchr1
	,say55rchr2
	,say55rchr3
	,say55rstr1
	,say55rstr2
	,say55rstr3
	,say55rstr4
	,say55rstr5,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f556103_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f556103_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f556103_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f556103_cdc(sys_integration_id); 
