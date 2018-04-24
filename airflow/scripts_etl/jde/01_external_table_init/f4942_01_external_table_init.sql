--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f4942_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4942_init

CREATE EXTERNAL TABLE stg_jde.ext_f4942_init
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
	,[isshpn] [DECIMAL](38, 4) 
	,[isrssn] [DECIMAL](38, 4) 
	,[isdoco] [DECIMAL](38, 4) 
	,[isdcto] [NVARCHAR](2) 
	,[iskcoo] [NVARCHAR](5) 
	,[islnid] [DECIMAL](38, 4) 
	,[iswgts] [DECIMAL](38, 4) 
	,[iswtum] [NVARCHAR](2) 
	,[isscvl] [DECIMAL](38, 4) 
	,[isvlum] [NVARCHAR](2) 
	,[isitm] [DECIMAL](38, 4) 
	,[issoqs] [DECIMAL](38, 4) 
	,[isuom] [NVARCHAR](2) 
	,[isprp1] [NVARCHAR](3) 
	,[isnmfc] [NVARCHAR](4) 
	,[isdsgp] [NVARCHAR](3) 
	,[ishzdc] [NVARCHAR](3) 
	,[isfrt1] [NVARCHAR](6) 
	,[isfrt2] [NVARCHAR](6) 
	,[isaexp] [DECIMAL](38, 4) 
	,[isfea] [DECIMAL](38, 4) 
	,[iscrcd] [NVARCHAR](3) 
	,[isecst] [DECIMAL](38, 4) 
	,[isurcd] [NVARCHAR](2) 
	,[isurdt] [VARCHAR](20) 
	,[isurat] [DECIMAL](38, 4) 
	,[isurab] [DECIMAL](38, 4) 
	,[isurrf] [NVARCHAR](15) 
	,[isuser] [NVARCHAR](10) 
	,[ispid] [NVARCHAR](10) 
	,[isjobn] [NVARCHAR](10) 
	,[isupmj] [VARCHAR](20) 
	,[istday] [DECIMAL](38, 4) 
	,[iscums] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f4942/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
