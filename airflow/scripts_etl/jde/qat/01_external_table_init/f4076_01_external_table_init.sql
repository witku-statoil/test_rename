--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f4076_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4076_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f4076_init
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
	,[fmfrmn] [NVARCHAR](10) 
	,[fmfml] [NVARCHAR](160) 
	,[fmaprs] [NVARCHAR](1) 
	,[fmuser] [NVARCHAR](10) 
	,[fmpid] [NVARCHAR](10) 
	,[fmjobn] [NVARCHAR](10) 
	,[fmupmj] [VARCHAR](20) 
	,[fmtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4076/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
