--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f4070_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4070_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f4070_cdc
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
	,[snasn] [NVARCHAR](8) 
	,[snoseq] [DECIMAL](38, 4) 
	,[snanps] [DECIMAL](38, 4) 
	,[snast] [NVARCHAR](8) 
	,[snurcd] [NVARCHAR](2) 
	,[snurdt] [VARCHAR](20) 
	,[snurat] [DECIMAL](38, 4) 
	,[snurab] [DECIMAL](38, 4) 
	,[snurrf] [NVARCHAR](15) 
	,[sneftj] [VARCHAR](20) 
	,[snexdj] [VARCHAR](20) 
	,[sninht] [NVARCHAR](1) 
	,[sntier] [DECIMAL](38, 4) 
	,[snuser] [NVARCHAR](10) 
	,[snpid] [NVARCHAR](10) 
	,[snjobn] [NVARCHAR](10) 
	,[snupmj] [VARCHAR](20) 
	,[sntday] [DECIMAL](38, 4) 
	,[snsctype] [DECIMAL](38, 4) 
	,[snstprcf] [NVARCHAR](1) 
	,[snskipto] [DECIMAL](38, 4) 
	,[snskipend] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4070/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
