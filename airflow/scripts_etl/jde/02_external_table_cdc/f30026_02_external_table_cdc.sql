--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f30026_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f30026_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f30026_cdc
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
	,[ieitm] [DECIMAL](38, 4) 
	,[ielitm] [NVARCHAR](25) 
	,[ieaitm] [NVARCHAR](25) 
	,[iemmcu] [NVARCHAR](12) 
	,[ielocn] [NVARCHAR](20) 
	,[ielotn] [NVARCHAR](30) 
	,[ieledg] [NVARCHAR](2) 
	,[iecost] [NVARCHAR](3) 
	,[ielotg] [NVARCHAR](3) 
	,[iestdc] [DECIMAL](38, 4) 
	,[iexsmc] [DECIMAL](38, 4) 
	,[iecsl] [DECIMAL](38, 4) 
	,[iexscr] [DECIMAL](38, 4) 
	,[iesctc] [NVARCHAR](4) 
	,[iexsfc] [NVARCHAR](4) 
	,[iestfc] [DECIMAL](38, 4) 
	,[iexsf] [DECIMAL](38, 4) 
	,[ierats] [NVARCHAR](4) 
	,[iexsrc] [NVARCHAR](4) 
	,[iertsd] [DECIMAL](38, 4) 
	,[iexsr] [DECIMAL](38, 4) 
	,[iepflg] [NVARCHAR](1) 
	,[ieuser] [NVARCHAR](10) 
	,[iepid] [NVARCHAR](10) 
	,[iejobn] [NVARCHAR](10) 
	,[ieupmj] [VARCHAR](20) 
	,[ietday] [DECIMAL](38, 4) 
	,[ieopsq] [DECIMAL](38, 4) 
	,[iemcul] [NVARCHAR](12) 
	,[iewmcu] [NVARCHAR](12) 
	,[ielda] [NVARCHAR](1) 
	,[ietbm] [NVARCHAR](3) 
	,[ieacq] [DECIMAL](38, 4) 
	,[ief] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f30026/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
