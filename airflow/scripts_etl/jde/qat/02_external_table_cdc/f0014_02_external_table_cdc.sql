--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f0014_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f0014_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f0014_cdc
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
	,[pnptc] [NVARCHAR](3) 
	,[pnptd] [NVARCHAR](30) 
	,[pndcp] [DECIMAL](38, 4) 
	,[pndcd] [DECIMAL](38, 4) 
	,[pnndtp] [DECIMAL](38, 4) 
	,[pnddj] [VARCHAR](20) 
	,[pnnsp] [DECIMAL](38, 4) 
	,[pndtpa] [DECIMAL](38, 4) 
	,[pneir] [DECIMAL](38, 4) 
	,[pnuser] [NVARCHAR](10) 
	,[pnpid] [NVARCHAR](10) 
	,[pnupmj] [VARCHAR](20) 
	,[pnjobn] [NVARCHAR](10) 
	,[pnupmt] [DECIMAL](38, 4) 
	,[pnpxdm] [DECIMAL](38, 4) 
	,[pnpxdd] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f0014/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
