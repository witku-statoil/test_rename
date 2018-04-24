--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f3412_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f3412_init

CREATE EXTERNAL TABLE stg_jde.ext_f3412_init
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
	,[mwitm] [DECIMAL](38, 4) 
	,[mwmcu] [NVARCHAR](12) 
	,[mwdrqj] [VARCHAR](20) 
	,[mwlovd] [DECIMAL](38, 4) 
	,[mwkit] [DECIMAL](38, 4) 
	,[mwmmcu] [NVARCHAR](12) 
	,[mwuorg] [DECIMAL](38, 4) 
	,[mwdoco] [DECIMAL](38, 4) 
	,[mwdcto] [NVARCHAR](2) 
	,[mwrkco] [NVARCHAR](5) 
	,[mwrorn] [NVARCHAR](8) 
	,[mwrcto] [NVARCHAR](2) 
	,[mwrlln] [DECIMAL](38, 4) 
	,[mwukid] [DECIMAL](38, 4) 
	,[mwplnk] [DECIMAL](38, 4) 
	,[mwprjm] [DECIMAL](38, 4) 
	,[mwsrdm] [DECIMAL](38, 4) 
	,[mwpns] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f3412/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
