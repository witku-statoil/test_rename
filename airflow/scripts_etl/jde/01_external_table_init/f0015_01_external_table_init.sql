--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f0015_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f0015_init

CREATE EXTERNAL TABLE stg_jde.ext_f0015_init
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
	,[cxcrcd] [NVARCHAR](3) 
	,[cxcrdc] [NVARCHAR](3) 
	,[cxan8] [DECIMAL](38, 4) 
	,[cxrttyp] [NVARCHAR](2) 
	,[cxeft] [VARCHAR](20) 
	,[cxclmeth] [NVARCHAR](1) 
	,[cxcrcm] [NVARCHAR](1) 
	,[cxtrcr] [NVARCHAR](3) 
	,[cxcrr] [DECIMAL](38, 4) 
	,[cxcrrd] [DECIMAL](38, 4) 
	,[cxcsr] [NVARCHAR](1) 
	,[cxuser] [NVARCHAR](10) 
	,[cxpid] [NVARCHAR](10) 
	,[cxjobn] [NVARCHAR](10) 
	,[cxupmj] [VARCHAR](20) 
	,[cxupmt] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f0015/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
