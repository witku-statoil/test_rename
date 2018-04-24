--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f41003_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f41003_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f41003_init
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
	,[ucum] [NVARCHAR](2) 
	,[ucrum] [NVARCHAR](2) 
	,[ucconv] [DECIMAL](38, 4) 
	,[ucuser] [NVARCHAR](10) 
	,[ucpid] [NVARCHAR](10) 
	,[ucjobn] [NVARCHAR](10) 
	,[ucupmj] [VARCHAR](20) 
	,[uctday] [DECIMAL](38, 4) 
	,[ucexpo] [NVARCHAR](1) 
	,[ucexso] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f41003/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
