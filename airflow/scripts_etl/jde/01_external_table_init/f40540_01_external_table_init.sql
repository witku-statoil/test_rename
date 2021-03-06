--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f40540_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f40540_init

CREATE EXTERNAL TABLE stg_jde.ext_f40540_init
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
	,[icitm] [DECIMAL](38, 4) 
	,[iccmdcde] [NVARCHAR](15) 
	,[icunspsc] [NVARCHAR](8) 
	,[icuser] [NVARCHAR](10) 
	,[icpid] [NVARCHAR](10) 
	,[icjobn] [NVARCHAR](10) 
	,[icupmt] [DECIMAL](38, 4) 
	,[icupmj] [VARCHAR](20) 
)
WITH
(
    LOCATION='processing-clean/jde/f40540/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
