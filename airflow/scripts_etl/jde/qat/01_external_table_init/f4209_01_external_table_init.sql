--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f4209_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4209_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f4209_init
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
	,[hohcod] [NVARCHAR](2) 
	,[horper] [DECIMAL](38, 4) 
	,[hoan8] [DECIMAL](38, 4) 
	,[homcu] [NVARCHAR](12) 
	,[hokcoo] [NVARCHAR](5) 
	,[hodoco] [DECIMAL](38, 4) 
	,[hodcto] [NVARCHAR](2) 
	,[hosfxo] [NVARCHAR](3) 
	,[holnid] [DECIMAL](38, 4) 
	,[hoitm] [DECIMAL](38, 4) 
	,[holitm] [NVARCHAR](25) 
	,[hoaitm] [NVARCHAR](25) 
	,[hotrdj] [VARCHAR](20) 
	,[hodrqj] [VARCHAR](20) 
	,[hopddj] [VARCHAR](20) 
	,[hoctyp] [NVARCHAR](2) 
	,[hordc] [NVARCHAR](2) 
	,[hordb] [NVARCHAR](10) 
	,[hordj] [VARCHAR](20) 
	,[hordt] [DECIMAL](38, 4) 
	,[hoartg] [NVARCHAR](12) 
	,[hoasts] [NVARCHAR](2) 
	,[hoaty] [NVARCHAR](1) 
	,[hoedei] [NVARCHAR](4) 
	,[hodlnid] [DECIMAL](38, 4) 
	,[hopa8] [DECIMAL](38, 4) 
	,[hoshan] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4209/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
