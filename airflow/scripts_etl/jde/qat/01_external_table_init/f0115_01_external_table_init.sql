--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f0115_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f0115_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f0115_init
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
	,[wpan8] [DECIMAL](38, 4) 
	,[wpidln] [DECIMAL](38, 4) 
	,[wprck7] [DECIMAL](38, 4) 
	,[wpcnln] [DECIMAL](38, 4) 
	,[wpphtp] [NVARCHAR](4) 
	,[wpar1] [NVARCHAR](6) 
	,[wpph1] [NVARCHAR](20) 
	,[wpuser] [NVARCHAR](10) 
	,[wppid] [NVARCHAR](10) 
	,[wpupmj] [VARCHAR](20) 
	,[wpjobn] [NVARCHAR](10) 
	,[wpupmt] [DECIMAL](38, 4) 
	,[wpcfno1] [DECIMAL](38, 4) 
	,[wpgen1] [NVARCHAR](10) 
	,[wpfalge] [NVARCHAR](1) 
	,[wpsyncs] [DECIMAL](38, 4) 
	,[wpcaad] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f0115/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
