--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f38112_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f38112_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f38112_cdc
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
	,[dxdoco] [DECIMAL](38, 4) 
	,[dxdcto] [NVARCHAR](2) 
	,[dxkcoo] [NVARCHAR](5) 
	,[dxlnid] [DECIMAL](38, 4) 
	,[dxdmct] [NVARCHAR](12) 
	,[dxdmcs] [DECIMAL](38, 4) 
	,[dxmcu] [NVARCHAR](12) 
	,[dxseq] [DECIMAL](38, 4) 
	,[dxpsr] [NVARCHAR](12) 
	,[dxpsry] [NVARCHAR](2) 
	,[dxqcom] [DECIMAL](38, 4) 
	,[dxcnqy] [NVARCHAR](1) 
	,[dxaa] [DECIMAL](38, 4) 
	,[dxcrcd] [NVARCHAR](3) 
	,[dxuom] [NVARCHAR](2) 
	,[dxtrdj] [VARCHAR](20) 
	,[dxtv01] [NVARCHAR](1) 
	,[dxtv02] [NVARCHAR](1) 
	,[dxtv03] [NVARCHAR](1) 
	,[dxurcd] [NVARCHAR](2) 
	,[dxurdt] [VARCHAR](20) 
	,[dxurab] [DECIMAL](38, 4) 
	,[dxurrf] [NVARCHAR](15) 
	,[dxuser] [NVARCHAR](10) 
	,[dxpid] [NVARCHAR](10) 
	,[dxjobn] [NVARCHAR](10) 
	,[dxupmj] [VARCHAR](20) 
	,[dxtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f38112/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
