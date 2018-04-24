--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f4105_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4105_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f4105_cdc
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
	,[coitm] [DECIMAL](38, 4) 
	,[colitm] [NVARCHAR](25) 
	,[coaitm] [NVARCHAR](25) 
	,[comcu] [NVARCHAR](12) 
	,[colocn] [NVARCHAR](20) 
	,[colotn] [NVARCHAR](30) 
	,[colotg] [NVARCHAR](3) 
	,[coledg] [NVARCHAR](2) 
	,[councs] [DECIMAL](38, 4) 
	,[cocspo] [NVARCHAR](1) 
	,[cocsin] [NVARCHAR](1) 
	,[courcd] [NVARCHAR](2) 
	,[courdt] [VARCHAR](20) 
	,[courat] [DECIMAL](38, 4) 
	,[courab] [DECIMAL](38, 4) 
	,[courrf] [NVARCHAR](15) 
	,[couser] [NVARCHAR](10) 
	,[copid] [NVARCHAR](10) 
	,[cojobn] [NVARCHAR](10) 
	,[coupmj] [VARCHAR](20) 
	,[cotday] [DECIMAL](38, 4) 
	,[coccfl] [NVARCHAR](1) 
	,[cocrcs] [DECIMAL](38, 4) 
	,[coostc] [DECIMAL](38, 4) 
	,[costoc] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4105/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
