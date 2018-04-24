--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f4096_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4096_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f4096_cdc
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
	,[faanum] [DECIMAL](38, 4) 
	,[faast] [NVARCHAR](8) 
	,[faco] [NVARCHAR](5) 
	,[faitem] [NVARCHAR](6) 
	,[faobjf] [NVARCHAR](6) 
	,[faobjt] [NVARCHAR](6) 
	,[fadcto] [NVARCHAR](2) 
	,[fa_1rt] [NVARCHAR](1) 
	,[fael] [DECIMAL](38, 4) 
	,[fadl01] [NVARCHAR](30) 
	,[fasblt] [NVARCHAR](1) 
	,[fasegs] [DECIMAL](38, 4) 
	,[fasfit] [NVARCHAR](10) 
	,[fasfdt] [NVARCHAR](1) 
	,[faabt1] [NVARCHAR](1) 
	,[fafile] [NVARCHAR](10) 
	,[fapid] [NVARCHAR](10) 
	,[fajobn] [NVARCHAR](10) 
	,[fauser] [NVARCHAR](10) 
	,[faupmj] [VARCHAR](20) 
	,[fatday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4096/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
