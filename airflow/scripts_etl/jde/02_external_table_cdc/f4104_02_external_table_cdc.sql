--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f4104_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4104_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f4104_cdc
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
	,[ivan8] [DECIMAL](38, 4) 
	,[ivxrt] [NVARCHAR](2) 
	,[ivitm] [DECIMAL](38, 4) 
	,[ivexdj] [VARCHAR](20) 
	,[iveftj] [VARCHAR](20) 
	,[ivmcu] [NVARCHAR](12) 
	,[ivcitm] [NVARCHAR](25) 
	,[ivdsc1] [NVARCHAR](30) 
	,[ivdsc2] [NVARCHAR](30) 
	,[ivaln] [NVARCHAR](30) 
	,[ivlitm] [NVARCHAR](25) 
	,[ivaitm] [NVARCHAR](25) 
	,[ivurcd] [NVARCHAR](2) 
	,[ivurdt] [VARCHAR](20) 
	,[ivurat] [DECIMAL](38, 4) 
	,[ivurab] [DECIMAL](38, 4) 
	,[ivurrf] [NVARCHAR](15) 
	,[ivuser] [NVARCHAR](10) 
	,[ivpid] [NVARCHAR](10) 
	,[ivjobn] [NVARCHAR](10) 
	,[ivupmj] [VARCHAR](20) 
	,[ivtday] [DECIMAL](38, 4) 
	,[ivrato] [DECIMAL](38, 4) 
	,[ivapsp] [DECIMAL](38, 4) 
	,[ividem] [NVARCHAR](1) 
	,[ivapss] [NVARCHAR](1) 
	,[ivcirv] [NVARCHAR](20) 
	,[ivadind] [NVARCHAR](1) 
	,[ivbpind] [NVARCHAR](1) 
	,[ivcardno] [NVARCHAR](4) 
)
WITH
(
    LOCATION='processing-clean/jde/f4104/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
