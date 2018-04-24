--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f41061_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f41061_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f41061_cdc
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
	,[cbmcu] [NVARCHAR](12) 
	,[cban8] [DECIMAL](38, 4) 
	,[cbitm] [DECIMAL](38, 4) 
	,[cblitm] [NVARCHAR](25) 
	,[cbaitm] [NVARCHAR](25) 
	,[cbcatn] [NVARCHAR](8) 
	,[cbdmct] [NVARCHAR](12) 
	,[cbdmcs] [DECIMAL](38, 4) 
	,[cbkcoo] [NVARCHAR](5) 
	,[cbdoco] [DECIMAL](38, 4) 
	,[cbdcto] [NVARCHAR](2) 
	,[cblnid] [DECIMAL](38, 4) 
	,[cbcrcd] [NVARCHAR](3) 
	,[cbuom] [NVARCHAR](2) 
	,[cbprrc] [DECIMAL](38, 4) 
	,[cbuorg] [DECIMAL](38, 4) 
	,[cbeftj] [VARCHAR](20) 
	,[cbexdj] [VARCHAR](20) 
	,[cbuser] [NVARCHAR](10) 
	,[cbpid] [NVARCHAR](10) 
	,[cbjobn] [NVARCHAR](10) 
	,[cbupmj] [VARCHAR](20) 
	,[cbtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f41061/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
