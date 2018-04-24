--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f0006_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f0006_init

CREATE EXTERNAL TABLE stg_jde.ext_f0006_init
(
	[sys_file_name] [NVARCHAR](100) 
	,[sys_file_ln] [VARCHAR](20) 
	,[sys_integration_id] [NVARCHAR](200) 
	,[sys_extract_dt] [NVARCHAR](40) 
	,[sys_cdc_dt] [NVARCHAR](40) 
	,[sys_cdc_scn] [NVARCHAR](14) 
	,[sys_cdc_operation_type] [NVARCHAR](15) 
	,[sys_cdc_before_after] [NVARCHAR](10) 
	,[sys_line_modified_ind] [VARCHAR](20) 
	,[mcmcu] [NVARCHAR](12) 
	,[mcstyl] [NVARCHAR](2) 
	,[mcdc] [NVARCHAR](40) 
	,[mcldm] [NVARCHAR](1) 
	,[mcco] [NVARCHAR](5) 
	,[mcan8] [DECIMAL](38, 4) 
	,[mcan8o] [DECIMAL](38, 4) 
	,[mccnty] [NVARCHAR](3) 
	,[mcadds] [NVARCHAR](3) 
	,[mcfmod] [NVARCHAR](1) 
	,[mcdl01] [NVARCHAR](30) 
	,[mcdl02] [NVARCHAR](30) 
	,[mcdl03] [NVARCHAR](30) 
	,[mcdl04] [NVARCHAR](30) 
	,[mcrp01] [NVARCHAR](3) 
	,[mcrp02] [NVARCHAR](3) 
	,[mcrp03] [NVARCHAR](3) 
	,[mcrp04] [NVARCHAR](3) 
	,[mcrp05] [NVARCHAR](3) 
	,[mcrp06] [NVARCHAR](3) 
	,[mcrp07] [NVARCHAR](3) 
	,[mcrp08] [NVARCHAR](3) 
	,[mcrp09] [NVARCHAR](3) 
	,[mcrp10] [NVARCHAR](3) 
	,[mcrp11] [NVARCHAR](3) 
	,[mcrp12] [NVARCHAR](3) 
	,[mcrp13] [NVARCHAR](3) 
	,[mcrp14] [NVARCHAR](3) 
	,[mcrp15] [NVARCHAR](3) 
	,[mcrp16] [NVARCHAR](3) 
	,[mcrp17] [NVARCHAR](3) 
	,[mcrp18] [NVARCHAR](3) 
	,[mcrp19] [NVARCHAR](3) 
	,[mcrp20] [NVARCHAR](3) 
	,[mcrp21] [NVARCHAR](10) 
	,[mcrp22] [NVARCHAR](10) 
	,[mcrp23] [NVARCHAR](10) 
	,[mcrp24] [NVARCHAR](10) 
	,[mcrp25] [NVARCHAR](10) 
	,[mcrp26] [NVARCHAR](10) 
	,[mcrp27] [NVARCHAR](10) 
	,[mcrp28] [NVARCHAR](10) 
	,[mcrp29] [NVARCHAR](10) 
	,[mcrp30] [NVARCHAR](10) 
	,[mcta] [NVARCHAR](10) 
	,[mctxjs] [DECIMAL](38, 4) 
	,[mctxa1] [NVARCHAR](10) 
	,[mcexr1] [NVARCHAR](2) 
	,[mctc01] [NVARCHAR](4) 
	,[mctc02] [NVARCHAR](4) 
	,[mctc03] [NVARCHAR](4) 
	,[mctc04] [NVARCHAR](4) 
	,[mctc05] [NVARCHAR](4) 
	,[mctc06] [NVARCHAR](4) 
	,[mctc07] [NVARCHAR](4) 
	,[mctc08] [NVARCHAR](4) 
	,[mctc09] [NVARCHAR](4) 
	,[mctc10] [NVARCHAR](4) 
	,[mcnd01] [NVARCHAR](1) 
	,[mcnd02] [NVARCHAR](1) 
	,[mcnd03] [NVARCHAR](1) 
	,[mcnd04] [NVARCHAR](1) 
	,[mcnd05] [NVARCHAR](1) 
	,[mcnd06] [NVARCHAR](1) 
	,[mcnd07] [NVARCHAR](1) 
	,[mcnd08] [NVARCHAR](1) 
	,[mcnd09] [NVARCHAR](1) 
	,[mcnd10] [NVARCHAR](1) 
	,[mccc01] [NVARCHAR](1) 
	,[mccc02] [NVARCHAR](1) 
	,[mccc03] [NVARCHAR](1) 
	,[mccc04] [NVARCHAR](1) 
	,[mccc05] [NVARCHAR](1) 
	,[mccc06] [NVARCHAR](1) 
	,[mccc07] [NVARCHAR](1) 
	,[mccc08] [NVARCHAR](1) 
	,[mccc09] [NVARCHAR](1) 
	,[mccc10] [NVARCHAR](1) 
	,[mcpecc] [NVARCHAR](1) 
	,[mcals] [NVARCHAR](1) 
	,[mciss] [NVARCHAR](1) 
	,[mcglba] [NVARCHAR](8) 
	,[mcalcl] [NVARCHAR](2) 
	,[mclmth] [NVARCHAR](1) 
	,[mclf] [DECIMAL](38, 4) 
	,[mcobj1] [NVARCHAR](6) 
	,[mcobj2] [NVARCHAR](6) 
	,[mcobj3] [NVARCHAR](6) 
	,[mcsub1] [NVARCHAR](8) 
	,[mctou] [DECIMAL](38, 4) 
	,[mcsbli] [NVARCHAR](1) 
	,[mcanpa] [DECIMAL](38, 4) 
	,[mcct] [NVARCHAR](4) 
	,[mccert] [NVARCHAR](1) 
	,[mcmcus] [NVARCHAR](12) 
	,[mcbtyp] [NVARCHAR](1) 
	,[mcpc] [DECIMAL](38, 4) 
	,[mcpca] [DECIMAL](38, 4) 
	,[mcpcc] [DECIMAL](38, 4) 
	,[mcinta] [NVARCHAR](4) 
	,[mcintl] [NVARCHAR](4) 
	,[mcd1j] [VARCHAR](20) 
	,[mcd2j] [VARCHAR](20) 
	,[mcd3j] [VARCHAR](20) 
	,[mcd4j] [VARCHAR](20) 
	,[mcd5j] [VARCHAR](20) 
	,[mcd6j] [VARCHAR](20) 
	,[mcfpdj] [VARCHAR](20) 
	,[mccac] [DECIMAL](38, 4) 
	,[mcpac] [DECIMAL](38, 4) 
	,[mceeo] [NVARCHAR](1) 
	,[mcerc] [NVARCHAR](2) 
	,[mcuser] [NVARCHAR](10) 
	,[mcpid] [NVARCHAR](10) 
	,[mcupmj] [VARCHAR](20) 
	,[mcjobn] [NVARCHAR](10) 
	,[mcupmt] [DECIMAL](38, 4) 
	,[mcbptp] [NVARCHAR](15) 
	,[mcapsb] [NVARCHAR](1) 
	,[mctsbu] [NVARCHAR](12) 
	,[mcrp31] [NVARCHAR](10) 
	,[mcrp32] [NVARCHAR](10) 
	,[mcrp33] [NVARCHAR](10) 
	,[mcrp34] [NVARCHAR](10) 
	,[mcrp35] [NVARCHAR](10) 
	,[mcrp36] [NVARCHAR](10) 
	,[mcrp37] [NVARCHAR](10) 
	,[mcrp38] [NVARCHAR](10) 
	,[mcrp39] [NVARCHAR](10) 
	,[mcrp40] [NVARCHAR](10) 
	,[mcrp41] [NVARCHAR](10) 
	,[mcrp42] [NVARCHAR](10) 
	,[mcrp43] [NVARCHAR](10) 
	,[mcrp44] [NVARCHAR](10) 
	,[mcrp45] [NVARCHAR](10) 
	,[mcrp46] [NVARCHAR](10) 
	,[mcrp47] [NVARCHAR](10) 
	,[mcrp48] [NVARCHAR](10) 
	,[mcrp49] [NVARCHAR](10) 
	,[mcrp50] [NVARCHAR](10) 
	,[mcan8gca1] [DECIMAL](38, 4) 
	,[mcan8gca2] [DECIMAL](38, 4) 
	,[mcan8gca3] [DECIMAL](38, 4) 
	,[mcan8gca4] [DECIMAL](38, 4) 
	,[mcan8gca5] [DECIMAL](38, 4) 
	,[mcrmcu1] [NVARCHAR](12) 
	,[mcdoco] [DECIMAL](38, 4) 
	,[mcpctn] [DECIMAL](38, 4) 
	,[mcclnu] [DECIMAL](38, 4) 
	,[mcbuca] [NVARCHAR](5) 
	,[mcadjent] [NVARCHAR](1) 
	,[mcuafl] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f0006/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
