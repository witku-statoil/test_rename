--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f4075_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4075_init

CREATE EXTERNAL TABLE stg_jde.ext_f4075_init
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
	,[vbvbt] [NVARCHAR](10) 
	,[vbcrcd] [NVARCHAR](3) 
	,[vbuom] [NVARCHAR](2) 
	,[vbuprc] [DECIMAL](38, 4) 
	,[vbeftj] [VARCHAR](20) 
	,[vbexdj] [VARCHAR](20) 
	,[vbaprs] [NVARCHAR](1) 
	,[vbuser] [NVARCHAR](10) 
	,[vbpid] [NVARCHAR](10) 
	,[vbjobn] [NVARCHAR](10) 
	,[vbupmj] [VARCHAR](20) 
	,[vbtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f4075/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
