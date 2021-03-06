--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f1201_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f1201_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f1201_cdc
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
	,[faco] [NVARCHAR](5) 
	,[fanumb] [DECIMAL](38, 4) 
	,[faapid] [NVARCHAR](12) 
	,[faaaid] [DECIMAL](38, 4) 
	,[faasid] [NVARCHAR](25) 
	,[faseq] [DECIMAL](38, 4) 
	,[faacl1] [NVARCHAR](3) 
	,[faacl2] [NVARCHAR](3) 
	,[faacl3] [NVARCHAR](3) 
	,[faacl4] [NVARCHAR](3) 
	,[faacl5] [NVARCHAR](3) 
	,[famcu] [NVARCHAR](12) 
	,[fadl01] [NVARCHAR](30) 
	,[fadl02] [NVARCHAR](30) 
	,[fadl03] [NVARCHAR](30) 
	,[fadscc] [NVARCHAR](40) 
	,[fadaj] [VARCHAR](20) 
	,[fadsp] [VARCHAR](20) 
	,[faeqst] [NVARCHAR](2) 
	,[fanoru] [NVARCHAR](1) 
	,[faaesv] [DECIMAL](38, 4) 
	,[faarpc] [DECIMAL](38, 4) 
	,[faalrc] [DECIMAL](38, 4) 
	,[faamcu] [NVARCHAR](12) 
	,[faaobj] [NVARCHAR](6) 
	,[faasub] [NVARCHAR](8) 
	,[fadmcu] [NVARCHAR](12) 
	,[fadobj] [NVARCHAR](6) 
	,[fadsub] [NVARCHAR](8) 
	,[faxmcu] [NVARCHAR](12) 
	,[faxobj] [NVARCHAR](6) 
	,[faxsub] [NVARCHAR](8) 
	,[farmcu] [NVARCHAR](12) 
	,[farobj] [NVARCHAR](6) 
	,[farsub] [NVARCHAR](8) 
	,[faarcq] [DECIMAL](38, 4) 
	,[faaroq] [DECIMAL](38, 4) 
	,[fatxjs] [DECIMAL](38, 4) 
	,[faaity] [DECIMAL](38, 4) 
	,[faaitp] [DECIMAL](38, 4) 
	,[fafinc] [NVARCHAR](1) 
	,[faitco] [NVARCHAR](1) 
	,[fapuro] [NVARCHAR](1) 
	,[faapur] [DECIMAL](38, 4) 
	,[fapurp] [DECIMAL](38, 4) 
	,[faapom] [DECIMAL](38, 4) 
	,[falano] [DECIMAL](38, 4) 
	,[fajcd] [VARCHAR](20) 
	,[fadexj] [VARCHAR](20) 
	,[faamf] [DECIMAL](38, 4) 
	,[farmk] [NVARCHAR](30) 
	,[farmk2] [NVARCHAR](30) 
	,[fainsp] [NVARCHAR](25) 
	,[fainsc] [NVARCHAR](25) 
	,[fainsm] [DECIMAL](38, 4) 
	,[fainsa] [DECIMAL](38, 4) 
	,[faaiv] [DECIMAL](38, 4) 
	,[fainsi] [DECIMAL](38, 4) 
	,[fauser] [NVARCHAR](10) 
	,[falct] [VARCHAR](20) 
	,[faloc] [NVARCHAR](12) 
	,[faadds] [NVARCHAR](3) 
	,[fapid] [NVARCHAR](10) 
	,[faeftb] [VARCHAR](20) 
	,[fader] [VARCHAR](20) 
	,[famsga] [NVARCHAR](1) 
	,[faex] [NVARCHAR](30) 
	,[faexr] [NVARCHAR](30) 
	,[faan8] [DECIMAL](38, 4) 
	,[faacl6] [NVARCHAR](3) 
	,[faacl7] [NVARCHAR](3) 
	,[faacl8] [NVARCHAR](3) 
	,[faacl9] [NVARCHAR](3) 
	,[faacl0] [NVARCHAR](3) 
	,[fasfc] [DECIMAL](38, 4) 
	,[fadadc] [NVARCHAR](12) 
	,[fadado] [NVARCHAR](6) 
	,[fadads] [NVARCHAR](8) 
	,[faunit] [NVARCHAR](8) 
	,[fakit] [DECIMAL](38, 4) 
	,[fakitl] [NVARCHAR](25) 
	,[faafe] [NVARCHAR](12) 
	,[fajbcd] [NVARCHAR](6) 
	,[fajbst] [NVARCHAR](4) 
	,[faun] [NVARCHAR](6) 
	,[fasbli] [NVARCHAR](1) 
	,[faupmj] [VARCHAR](20) 
	,[fajobn] [NVARCHAR](10) 
	,[faupmt] [DECIMAL](38, 4) 
	,[fafa0] [NVARCHAR](3) 
	,[fafa1] [NVARCHAR](3) 
	,[fafa2] [NVARCHAR](3) 
	,[fafa3] [NVARCHAR](3) 
	,[fafa4] [NVARCHAR](3) 
	,[fafa5] [NVARCHAR](3) 
	,[fafa6] [NVARCHAR](3) 
	,[fafa7] [NVARCHAR](3) 
	,[fafa8] [NVARCHAR](3) 
	,[fafa9] [NVARCHAR](3) 
	,[fafa21] [NVARCHAR](10) 
	,[fafa22] [NVARCHAR](10) 
	,[fafa23] [NVARCHAR](10) 
	,[fawoyn] [NVARCHAR](1) 
	,[facrtl] [DECIMAL](38, 4) 
	,[fawrfl] [NVARCHAR](1) 
	,[fawarj] [VARCHAR](20) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f1201/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
