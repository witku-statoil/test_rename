--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f3460_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f3460_init

CREATE EXTERNAL TABLE stg_jde.ext_f3460_init
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
	,[mfitm] [DECIMAL](38, 4) 
	,[mflitm] [NVARCHAR](25) 
	,[mfaitm] [NVARCHAR](25) 
	,[mfmcu] [NVARCHAR](12) 
	,[mfdrqj] [VARCHAR](20) 
	,[mfan8] [DECIMAL](38, 4) 
	,[mfuorg] [DECIMAL](38, 4) 
	,[mfaexp] [DECIMAL](38, 4) 
	,[mffam] [DECIMAL](38, 4) 
	,[mffqt] [DECIMAL](38, 4) 
	,[mftypf] [NVARCHAR](2) 
	,[mfdcto] [NVARCHAR](2) 
	,[mfbpfc] [NVARCHAR](1) 
	,[mfrvis] [NVARCHAR](1) 
	,[mfupmj] [VARCHAR](20) 
	,[mfuser] [NVARCHAR](10) 
	,[mfjobn] [NVARCHAR](10) 
	,[mfpid] [NVARCHAR](10) 
	,[mfyr] [DECIMAL](38, 4) 
	,[mftday] [DECIMAL](38, 4) 
	,[mfpmpn] [NVARCHAR](30) 
	,[mfpns] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f3460/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
