-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0006_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0006_cdc


CREATE TABLE stg_jde.tmp_upsert_f0006_cdc
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
	,mcmcu as mcmcu
	,ltrim(rtrim(mcmcu)) as mcmcu_conv
	,mcstyl as mcstyl
	,stg_jde.prc_decode_f0005_00('00','MC',ltrim(rtrim(mcstyl)))  as mcstyl_desc
	,mcdc as mcdc
	,ltrim(rtrim(mcdc)) as mcdc_conv
	,mcldm as mcldm
	,stg_jde.prc_decode_f0005_h00('H00','LD',ltrim(rtrim(mcldm)))  as mcldm_desc
	,mcco as mcco
	,ltrim(rtrim(mcco)) as mcco_conv
	,cast(mcan8 as [DECIMAL](38, 4)) as mcan8
	,cast(mcan8o as [DECIMAL](38, 4)) as mcan8o
	,mccnty as mccnty
	,stg_jde.prc_decode_f0005_00('00','CT',ltrim(rtrim(mccnty)))  as mccnty_desc
	,mcadds as mcadds
	,stg_jde.prc_decode_f0005_00('00','S',ltrim(rtrim(mcadds)))  as mcadds_desc
	,mcfmod as mcfmod
	,stg_jde.prc_decode_f0005_h09('H09','MD',ltrim(rtrim(mcfmod)))  as mcfmod_desc
	,mcdl01 as mcdl01
	,ltrim(rtrim(mcdl01)) as mcdl01_conv
	,mcdl02 as mcdl02
	,ltrim(rtrim(mcdl02)) as mcdl02_conv
	,mcdl03 as mcdl03
	,ltrim(rtrim(mcdl03)) as mcdl03_conv
	,mcdl04 as mcdl04
	,ltrim(rtrim(mcdl04)) as mcdl04_conv
	,mcrp01 as mcrp01
	,stg_jde.prc_decode_f0005_00('00','01',ltrim(rtrim(mcrp01)))  as mcrp01_desc
	,mcrp02 as mcrp02
	,stg_jde.prc_decode_f0005_00('00','02',ltrim(rtrim(mcrp02)))  as mcrp02_desc
	,mcrp03 as mcrp03
	,stg_jde.prc_decode_f0005_00('00','03',ltrim(rtrim(mcrp03)))  as mcrp03_desc
	,mcrp04 as mcrp04
	,stg_jde.prc_decode_f0005_00('00','04',ltrim(rtrim(mcrp04)))  as mcrp04_desc
	,mcrp05 as mcrp05
	,stg_jde.prc_decode_f0005_00('00','05',ltrim(rtrim(mcrp05)))  as mcrp05_desc
	,mcrp06 as mcrp06
	,stg_jde.prc_decode_f0005_00('00','06',ltrim(rtrim(mcrp06)))  as mcrp06_desc
	,mcrp07 as mcrp07
	,stg_jde.prc_decode_f0005_00('00','07',ltrim(rtrim(mcrp07)))  as mcrp07_desc
	,mcrp08 as mcrp08
	,stg_jde.prc_decode_f0005_00('00','08',ltrim(rtrim(mcrp08)))  as mcrp08_desc
	,mcrp09 as mcrp09
	,stg_jde.prc_decode_f0005_00('00','09',ltrim(rtrim(mcrp09)))  as mcrp09_desc
	,mcrp10 as mcrp10
	,stg_jde.prc_decode_f0005_00('00','10',ltrim(rtrim(mcrp10)))  as mcrp10_desc
	,mcrp11 as mcrp11
	,stg_jde.prc_decode_f0005_00('00','11',ltrim(rtrim(mcrp11)))  as mcrp11_desc
	,mcrp12 as mcrp12
	,stg_jde.prc_decode_f0005_00('00','12',ltrim(rtrim(mcrp12)))  as mcrp12_desc
	,mcrp13 as mcrp13
	,stg_jde.prc_decode_f0005_00('00','13',ltrim(rtrim(mcrp13)))  as mcrp13_desc
	,mcrp14 as mcrp14
	,stg_jde.prc_decode_f0005_00('00','14',ltrim(rtrim(mcrp14)))  as mcrp14_desc
	,mcrp15 as mcrp15
	,stg_jde.prc_decode_f0005_00('00','15',ltrim(rtrim(mcrp15)))  as mcrp15_desc
	,mcrp16 as mcrp16
	,stg_jde.prc_decode_f0005_00('00','16',ltrim(rtrim(mcrp16)))  as mcrp16_desc
	,mcrp17 as mcrp17
	,stg_jde.prc_decode_f0005_00('00','17',ltrim(rtrim(mcrp17)))  as mcrp17_desc
	,mcrp18 as mcrp18
	,stg_jde.prc_decode_f0005_00('00','18',ltrim(rtrim(mcrp18)))  as mcrp18_desc
	,mcrp19 as mcrp19
	,stg_jde.prc_decode_f0005_00('00','19',ltrim(rtrim(mcrp19)))  as mcrp19_desc
	,mcrp20 as mcrp20
	,stg_jde.prc_decode_f0005_00('00','20',ltrim(rtrim(mcrp20)))  as mcrp20_desc
	,mcrp21 as mcrp21
	,stg_jde.prc_decode_f0005_00('00','21',ltrim(rtrim(mcrp21)))  as mcrp21_desc
	,mcrp22 as mcrp22
	,stg_jde.prc_decode_f0005_00('00','22',ltrim(rtrim(mcrp22)))  as mcrp22_desc
	,mcrp23 as mcrp23
	,stg_jde.prc_decode_f0005_00('00','23',ltrim(rtrim(mcrp23)))  as mcrp23_desc
	,mcrp24 as mcrp24
	,stg_jde.prc_decode_f0005_00('00','24',ltrim(rtrim(mcrp24)))  as mcrp24_desc
	,mcrp25 as mcrp25
	,stg_jde.prc_decode_f0005_00('00','25',ltrim(rtrim(mcrp25)))  as mcrp25_desc
	,mcrp26 as mcrp26
	,stg_jde.prc_decode_f0005_00('00','26',ltrim(rtrim(mcrp26)))  as mcrp26_desc
	,mcrp27 as mcrp27
	,stg_jde.prc_decode_f0005_00('00','27',ltrim(rtrim(mcrp27)))  as mcrp27_desc
	,mcrp28 as mcrp28
	,stg_jde.prc_decode_f0005_00('00','28',ltrim(rtrim(mcrp28)))  as mcrp28_desc
	,mcrp29 as mcrp29
	,stg_jde.prc_decode_f0005_00('00','29',ltrim(rtrim(mcrp29)))  as mcrp29_desc
	,mcrp30 as mcrp30
	,stg_jde.prc_decode_f0005_00('00','30',ltrim(rtrim(mcrp30)))  as mcrp30_desc
	,mcta as mcta
	,ltrim(rtrim(mcta)) as mcta_conv
	,cast(mctxjs as [DECIMAL](38, 4)) as mctxjs
	,mctxa1 as mctxa1
	,ltrim(rtrim(mctxa1)) as mctxa1_conv
	,mcexr1 as mcexr1
	,stg_jde.prc_decode_f0005_00('00','EX',ltrim(rtrim(mcexr1)))  as mcexr1_desc
	,mctc01 as mctc01
	,ltrim(rtrim(mctc01)) as mctc01_conv
	,mctc02 as mctc02
	,ltrim(rtrim(mctc02)) as mctc02_conv
	,mctc03 as mctc03
	,ltrim(rtrim(mctc03)) as mctc03_conv
	,mctc04 as mctc04
	,ltrim(rtrim(mctc04)) as mctc04_conv
	,mctc05 as mctc05
	,ltrim(rtrim(mctc05)) as mctc05_conv
	,mctc06 as mctc06
	,ltrim(rtrim(mctc06)) as mctc06_conv
	,mctc07 as mctc07
	,ltrim(rtrim(mctc07)) as mctc07_conv
	,mctc08 as mctc08
	,ltrim(rtrim(mctc08)) as mctc08_conv
	,mctc09 as mctc09
	,ltrim(rtrim(mctc09)) as mctc09_conv
	,mctc10 as mctc10
	,ltrim(rtrim(mctc10)) as mctc10_conv
	,mcnd01 as mcnd01
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd01)))  as mcnd01_desc
	,mcnd02 as mcnd02
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd02)))  as mcnd02_desc
	,mcnd03 as mcnd03
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd03)))  as mcnd03_desc
	,mcnd04 as mcnd04
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd04)))  as mcnd04_desc
	,mcnd05 as mcnd05
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd05)))  as mcnd05_desc
	,mcnd06 as mcnd06
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd06)))  as mcnd06_desc
	,mcnd07 as mcnd07
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd07)))  as mcnd07_desc
	,mcnd08 as mcnd08
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd08)))  as mcnd08_desc
	,mcnd09 as mcnd09
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd09)))  as mcnd09_desc
	,mcnd10 as mcnd10
	,stg_jde.prc_decode_f0005_h00('H00','ND',ltrim(rtrim(mcnd10)))  as mcnd10_desc
	,mccc01 as mccc01
	,ltrim(rtrim(mccc01)) as mccc01_conv
	,mccc02 as mccc02
	,ltrim(rtrim(mccc02)) as mccc02_conv
	,mccc03 as mccc03
	,ltrim(rtrim(mccc03)) as mccc03_conv
	,mccc04 as mccc04
	,ltrim(rtrim(mccc04)) as mccc04_conv
	,mccc05 as mccc05
	,ltrim(rtrim(mccc05)) as mccc05_conv
	,mccc06 as mccc06
	,ltrim(rtrim(mccc06)) as mccc06_conv
	,mccc07 as mccc07
	,ltrim(rtrim(mccc07)) as mccc07_conv
	,mccc08 as mccc08
	,ltrim(rtrim(mccc08)) as mccc08_conv
	,mccc09 as mccc09
	,ltrim(rtrim(mccc09)) as mccc09_conv
	,mccc10 as mccc10
	,ltrim(rtrim(mccc10)) as mccc10_conv
	,mcpecc as mcpecc
	,stg_jde.prc_decode_f0005_00('00','PF',ltrim(rtrim(mcpecc)))  as mcpecc_desc
	,mcals as mcals
	,stg_jde.prc_decode_f0005_h00('H00','AL',ltrim(rtrim(mcals)))  as mcals_desc
	,mciss as mciss
	,stg_jde.prc_decode_f0005_h00('H00','IS',ltrim(rtrim(mciss)))  as mciss_desc
	,mcglba as mcglba
	,ltrim(rtrim(mcglba)) as mcglba_conv
	,mcalcl as mcalcl
	,ltrim(rtrim(mcalcl)) as mcalcl_conv
	,mclmth as mclmth
	,stg_jde.prc_decode_f0005_07('07','LT',ltrim(rtrim(mclmth)))  as mclmth_desc
	,cast(mclf as [DECIMAL](38, 4)) as mclf
	,cast(mclf as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mclf_conv
	,mcobj1 as mcobj1
	,ltrim(rtrim(mcobj1)) as mcobj1_conv
	,mcobj2 as mcobj2
	,ltrim(rtrim(mcobj2)) as mcobj2_conv
	,mcobj3 as mcobj3
	,ltrim(rtrim(mcobj3)) as mcobj3_conv
	,mcsub1 as mcsub1
	,ltrim(rtrim(mcsub1)) as mcsub1_conv
	,cast(mctou as [DECIMAL](38, 4)) as mctou
	,cast(mctou as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as mctou_conv
	,mcsbli as mcsbli
	,stg_jde.prc_decode_f0005_00('00','SI',ltrim(rtrim(mcsbli)))  as mcsbli_desc
	,cast(mcanpa as [DECIMAL](38, 4)) as mcanpa
	,mcct as mcct
	,stg_jde.prc_decode_f0005_51('51','CT',ltrim(rtrim(mcct)))  as mcct_desc
	,mccert as mccert
	,stg_jde.prc_decode_f0005_h00('H00','CE',ltrim(rtrim(mccert)))  as mccert_desc
	,mcmcus as mcmcus
	,ltrim(rtrim(mcmcus)) as mcmcus_conv
	,mcbtyp as mcbtyp
	,ltrim(rtrim(mcbtyp)) as mcbtyp_conv
	,cast(mcpc as [DECIMAL](38, 4)) as mcpc
	,cast(mcpca as [DECIMAL](38, 4)) as mcpca
	,cast(mcpca as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as mcpca_conv
	,cast(mcpcc as [DECIMAL](38, 4)) as mcpcc
	,cast(mcpcc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as mcpcc_conv
	,mcinta as mcinta
	,ltrim(rtrim(mcinta)) as mcinta_conv
	,mcintl as mcintl
	,ltrim(rtrim(mcintl)) as mcintl_conv
	,cast(mcd1j as [INT]) as mcd1j
	,case when cast(mcd1j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mcd1j as [INT]) %1000, dateadd(year, cast(mcd1j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mcd1j_conv
	,cast(mcd2j as [INT]) as mcd2j
	,case when cast(mcd2j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mcd2j as [INT]) %1000, dateadd(year, cast(mcd2j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mcd2j_conv
	,cast(mcd3j as [INT]) as mcd3j
	,case when cast(mcd3j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mcd3j as [INT]) %1000, dateadd(year, cast(mcd3j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mcd3j_conv
	,cast(mcd4j as [INT]) as mcd4j
	,case when cast(mcd4j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mcd4j as [INT]) %1000, dateadd(year, cast(mcd4j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mcd4j_conv
	,cast(mcd5j as [INT]) as mcd5j
	,case when cast(mcd5j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mcd5j as [INT]) %1000, dateadd(year, cast(mcd5j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mcd5j_conv
	,cast(mcd6j as [INT]) as mcd6j
	,case when cast(mcd6j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mcd6j as [INT]) %1000, dateadd(year, cast(mcd6j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mcd6j_conv
	,cast(mcfpdj as [INT]) as mcfpdj
	,case when cast(mcfpdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mcfpdj as [INT]) %1000, dateadd(year, cast(mcfpdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mcfpdj_conv
	,cast(mccac as [DECIMAL](38, 4)) as mccac
	,cast(mccac as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as mccac_conv
	,cast(mcpac as [DECIMAL](38, 4)) as mcpac
	,cast(mcpac as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as mcpac_conv
	,mceeo as mceeo
	,stg_jde.prc_decode_f0005_h00('H00','EE',ltrim(rtrim(mceeo)))  as mceeo_desc
	,mcerc as mcerc
	,stg_jde.prc_decode_f0005_00('00','RC',ltrim(rtrim(mcerc)))  as mcerc_desc
	,mcuser as mcuser
	,ltrim(rtrim(mcuser)) as mcuser_conv
	,mcpid as mcpid
	,ltrim(rtrim(mcpid)) as mcpid_conv
	,cast(mcupmj as [INT]) as mcupmj
	,case when cast(mcupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mcupmj as [INT]) %1000, dateadd(year, cast(mcupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mcupmj_conv
	,mcjobn as mcjobn
	,ltrim(rtrim(mcjobn)) as mcjobn_conv
	,cast(mcupmt as [DECIMAL](38, 4)) as mcupmt
	,mcbptp as mcbptp
	,ltrim(rtrim(mcbptp)) as mcbptp_conv
	,mcapsb as mcapsb
	,ltrim(rtrim(mcapsb)) as mcapsb_conv
	,mctsbu as mctsbu
	,ltrim(rtrim(mctsbu)) as mctsbu_conv
	,mcrp31 as mcrp31
	,stg_jde.prc_decode_f0005_00('00','31',ltrim(rtrim(mcrp31)))  as mcrp31_desc
	,mcrp32 as mcrp32
	,stg_jde.prc_decode_f0005_00('00','32',ltrim(rtrim(mcrp32)))  as mcrp32_desc
	,mcrp33 as mcrp33
	,stg_jde.prc_decode_f0005_00('00','33',ltrim(rtrim(mcrp33)))  as mcrp33_desc
	,mcrp34 as mcrp34
	,stg_jde.prc_decode_f0005_00('00','34',ltrim(rtrim(mcrp34)))  as mcrp34_desc
	,mcrp35 as mcrp35
	,stg_jde.prc_decode_f0005_00('00','35',ltrim(rtrim(mcrp35)))  as mcrp35_desc
	,mcrp36 as mcrp36
	,stg_jde.prc_decode_f0005_00('00','36',ltrim(rtrim(mcrp36)))  as mcrp36_desc
	,mcrp37 as mcrp37
	,stg_jde.prc_decode_f0005_00('00','37',ltrim(rtrim(mcrp37)))  as mcrp37_desc
	,mcrp38 as mcrp38
	,stg_jde.prc_decode_f0005_00('00','38',ltrim(rtrim(mcrp38)))  as mcrp38_desc
	,mcrp39 as mcrp39
	,stg_jde.prc_decode_f0005_00('00','39',ltrim(rtrim(mcrp39)))  as mcrp39_desc
	,mcrp40 as mcrp40
	,stg_jde.prc_decode_f0005_00('00','40',ltrim(rtrim(mcrp40)))  as mcrp40_desc
	,mcrp41 as mcrp41
	,stg_jde.prc_decode_f0005_00('00','41',ltrim(rtrim(mcrp41)))  as mcrp41_desc
	,mcrp42 as mcrp42
	,stg_jde.prc_decode_f0005_00('00','42',ltrim(rtrim(mcrp42)))  as mcrp42_desc
	,mcrp43 as mcrp43
	,stg_jde.prc_decode_f0005_00('00','43',ltrim(rtrim(mcrp43)))  as mcrp43_desc
	,mcrp44 as mcrp44
	,stg_jde.prc_decode_f0005_00('00','44',ltrim(rtrim(mcrp44)))  as mcrp44_desc
	,mcrp45 as mcrp45
	,stg_jde.prc_decode_f0005_00('00','45',ltrim(rtrim(mcrp45)))  as mcrp45_desc
	,mcrp46 as mcrp46
	,stg_jde.prc_decode_f0005_00('00','46',ltrim(rtrim(mcrp46)))  as mcrp46_desc
	,mcrp47 as mcrp47
	,stg_jde.prc_decode_f0005_00('00','47',ltrim(rtrim(mcrp47)))  as mcrp47_desc
	,mcrp48 as mcrp48
	,stg_jde.prc_decode_f0005_00('00','48',ltrim(rtrim(mcrp48)))  as mcrp48_desc
	,mcrp49 as mcrp49
	,stg_jde.prc_decode_f0005_00('00','49',ltrim(rtrim(mcrp49)))  as mcrp49_desc
	,mcrp50 as mcrp50
	,stg_jde.prc_decode_f0005_00('00','50',ltrim(rtrim(mcrp50)))  as mcrp50_desc
	,cast(mcan8gca1 as [DECIMAL](38, 4)) as mcan8gca1
	,cast(mcan8gca2 as [DECIMAL](38, 4)) as mcan8gca2
	,cast(mcan8gca3 as [DECIMAL](38, 4)) as mcan8gca3
	,cast(mcan8gca4 as [DECIMAL](38, 4)) as mcan8gca4
	,cast(mcan8gca5 as [DECIMAL](38, 4)) as mcan8gca5
	,mcrmcu1 as mcrmcu1
	,ltrim(rtrim(mcrmcu1)) as mcrmcu1_conv
	,cast(mcdoco as [DECIMAL](38, 4)) as mcdoco
	,cast(mcpctn as [DECIMAL](38, 4)) as mcpctn
	,cast(mcclnu as [DECIMAL](38, 4)) as mcclnu
	,mcbuca as mcbuca
	,stg_jde.prc_decode_f0005_48s('48S','BC',ltrim(rtrim(mcbuca)))  as mcbuca_desc
	,mcadjent as mcadjent
	,ltrim(rtrim(mcadjent)) as mcadjent_conv
	,mcuafl as mcuafl
	,ltrim(rtrim(mcuafl)) as mcuafl_conv
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
	,mcmcu
	,mcstyl
	,mcdc
	,mcldm
	,mcco
	,mcan8
	,mcan8o
	,mccnty
	,mcadds
	,mcfmod
	,mcdl01
	,mcdl02
	,mcdl03
	,mcdl04
	,mcrp01
	,mcrp02
	,mcrp03
	,mcrp04
	,mcrp05
	,mcrp06
	,mcrp07
	,mcrp08
	,mcrp09
	,mcrp10
	,mcrp11
	,mcrp12
	,mcrp13
	,mcrp14
	,mcrp15
	,mcrp16
	,mcrp17
	,mcrp18
	,mcrp19
	,mcrp20
	,mcrp21
	,mcrp22
	,mcrp23
	,mcrp24
	,mcrp25
	,mcrp26
	,mcrp27
	,mcrp28
	,mcrp29
	,mcrp30
	,mcta
	,mctxjs
	,mctxa1
	,mcexr1
	,mctc01
	,mctc02
	,mctc03
	,mctc04
	,mctc05
	,mctc06
	,mctc07
	,mctc08
	,mctc09
	,mctc10
	,mcnd01
	,mcnd02
	,mcnd03
	,mcnd04
	,mcnd05
	,mcnd06
	,mcnd07
	,mcnd08
	,mcnd09
	,mcnd10
	,mccc01
	,mccc02
	,mccc03
	,mccc04
	,mccc05
	,mccc06
	,mccc07
	,mccc08
	,mccc09
	,mccc10
	,mcpecc
	,mcals
	,mciss
	,mcglba
	,mcalcl
	,mclmth
	,mclf
	,mcobj1
	,mcobj2
	,mcobj3
	,mcsub1
	,mctou
	,mcsbli
	,mcanpa
	,mcct
	,mccert
	,mcmcus
	,mcbtyp
	,mcpc
	,mcpca
	,mcpcc
	,mcinta
	,mcintl
	,mcd1j
	,mcd2j
	,mcd3j
	,mcd4j
	,mcd5j
	,mcd6j
	,mcfpdj
	,mccac
	,mcpac
	,mceeo
	,mcerc
	,mcuser
	,mcpid
	,mcupmj
	,mcjobn
	,mcupmt
	,mcbptp
	,mcapsb
	,mctsbu
	,mcrp31
	,mcrp32
	,mcrp33
	,mcrp34
	,mcrp35
	,mcrp36
	,mcrp37
	,mcrp38
	,mcrp39
	,mcrp40
	,mcrp41
	,mcrp42
	,mcrp43
	,mcrp44
	,mcrp45
	,mcrp46
	,mcrp47
	,mcrp48
	,mcrp49
	,mcrp50
	,mcan8gca1
	,mcan8gca2
	,mcan8gca3
	,mcan8gca4
	,mcan8gca5
	,mcrmcu1
	,mcdoco
	,mcpctn
	,mcclnu
	,mcbuca
	,mcadjent
	,mcuafl,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0006_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0006_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0006_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0006_cdc(sys_integration_id); 
