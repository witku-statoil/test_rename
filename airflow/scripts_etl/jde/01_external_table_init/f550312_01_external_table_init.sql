--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f550312_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f550312_init

CREATE EXTERNAL TABLE stg_jde.ext_f550312_init
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
	,[sglrsegt] [NVARCHAR](6) 
	,[sgctr] [NVARCHAR](3) 
	,[sgac04] [NVARCHAR](3) 
	,[sgdsc1] [NVARCHAR](30) 
	,[sgac05] [NVARCHAR](3) 
	,[sgdsc2] [NVARCHAR](30) 
	,[sgac01] [NVARCHAR](3) 
	,[sgclass03] [NVARCHAR](3) 
	,[sgsrp1] [NVARCHAR](3) 
	,[sgy55char1] [NVARCHAR](1) 
	,[sgy55char2] [NVARCHAR](1) 
	,[sgy55strg1] [NVARCHAR](30) 
	,[sgy55strg2] [NVARCHAR](30) 
	,[sgy55amnt1] [DECIMAL](38, 4) 
	,[sgy55amnt2] [DECIMAL](38, 4) 
	,[sgy55time1] [DECIMAL](38, 4) 
	,[sgy55time2] [DECIMAL](38, 4) 
	,[sgy55date1] [VARCHAR](20) 
	,[sgy55date2] [VARCHAR](20) 
	,[sguser] [NVARCHAR](10) 
	,[sgpid] [NVARCHAR](10) 
	,[sgjobn] [NVARCHAR](10) 
	,[sgupmj] [VARCHAR](20) 
	,[sgupmt] [DECIMAL](38, 4) 
	,[sgdesc] [NVARCHAR](30) 
	,[sgan8] [DECIMAL](38, 4) 
	,[sgalph] [NVARCHAR](40) 
)
WITH
(
    LOCATION='processing-clean/jde/f550312/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
