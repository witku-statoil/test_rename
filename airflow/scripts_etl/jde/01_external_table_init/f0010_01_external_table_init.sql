--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f0010_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f0010_init

CREATE EXTERNAL TABLE stg_jde.ext_f0010_init
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
	,[ccco] [NVARCHAR](5) 
	,[ccname] [NVARCHAR](30) 
	,[ccaltc] [NVARCHAR](40) 
	,[ccdfyj] [VARCHAR](20) 
	,[ccpnc] [DECIMAL](38, 4) 
	,[ccdot1] [NVARCHAR](1) 
	,[cccryr] [NVARCHAR](1) 
	,[cccryc] [NVARCHAR](1) 
	,[ccdot2] [NVARCHAR](1) 
	,[ccmcua] [NVARCHAR](1) 
	,[ccmcud] [NVARCHAR](1) 
	,[ccmcuc] [NVARCHAR](1) 
	,[ccmcub] [NVARCHAR](1) 
	,[ccdprc] [NVARCHAR](1) 
	,[ccbktx] [NVARCHAR](1) 
	,[cctxbm] [DECIMAL](38, 4) 
	,[cctxbo] [DECIMAL](38, 4) 
	,[ccnwxj] [VARCHAR](20) 
	,[ccglie] [NVARCHAR](1) 
	,[ccabin] [NVARCHAR](1) 
	,[cccald] [NVARCHAR](2) 
	,[ccdtpn] [NVARCHAR](1) 
	,[ccpnf] [DECIMAL](38, 4) 
	,[ccdff] [DECIMAL](38, 4) 
	,[cccrcd] [NVARCHAR](3) 
	,[ccsmi] [NVARCHAR](1) 
	,[ccsmu] [NVARCHAR](1) 
	,[ccsms] [NVARCHAR](1) 
	,[ccdnlt] [NVARCHAR](1) 
	,[ccstmt] [NVARCHAR](1) 
	,[ccatcs] [NVARCHAR](1) 
	,[ccalgm] [NVARCHAR](2) 
	,[ccagem] [NVARCHAR](1) 
	,[cccrdy] [DECIMAL](38, 4) 
	,[ccagr1] [DECIMAL](38, 4) 
	,[ccagr2] [DECIMAL](38, 4) 
	,[ccagr3] [DECIMAL](38, 4) 
	,[ccagr4] [DECIMAL](38, 4) 
	,[ccagr5] [DECIMAL](38, 4) 
	,[ccagr6] [DECIMAL](38, 4) 
	,[ccagr7] [DECIMAL](38, 4) 
	,[ccfry] [DECIMAL](38, 4) 
	,[ccfrp] [DECIMAL](38, 4) 
	,[ccnnp] [DECIMAL](38, 4) 
	,[ccfp] [DECIMAL](38, 4) 
	,[ccage] [NVARCHAR](1) 
	,[ccdag] [VARCHAR](20) 
	,[ccarpn] [DECIMAL](38, 4) 
	,[ccappn] [DECIMAL](38, 4) 
	,[ccarfj] [VARCHAR](20) 
	,[ccapfj] [VARCHAR](20) 
	,[ccan8] [DECIMAL](38, 4) 
	,[ccsgbk] [NVARCHAR](40) 
	,[ccptco] [NVARCHAR](1) 
	,[ccx1] [NVARCHAR](1) 
	,[ccx2] [NVARCHAR](1) 
	,[ccuser] [NVARCHAR](10) 
	,[ccpid] [NVARCHAR](10) 
	,[ccupmj] [VARCHAR](20) 
	,[ccjobn] [NVARCHAR](10) 
	,[ccupmt] [DECIMAL](38, 4) 
	,[cctsid] [DECIMAL](38, 4) 
	,[cctsco] [NVARCHAR](5) 
	,[ccthco] [NVARCHAR](3) 
	,[ccan8c] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f0010/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
