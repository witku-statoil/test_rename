--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f1113_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f1113_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f1113_cdc
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
	,[c1rtty] [NVARCHAR](2) 
	,[c1crdc] [NVARCHAR](3) 
	,[c1crcd] [NVARCHAR](3) 
	,[c1eft] [VARCHAR](20) 
	,[c1crr] [DECIMAL](38, 4) 
	,[c1crrd] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f1113/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
