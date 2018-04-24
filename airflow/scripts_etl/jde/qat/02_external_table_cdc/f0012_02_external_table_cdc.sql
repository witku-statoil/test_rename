--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f0012_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f0012_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f0012_cdc
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
	,[kgitem] [NVARCHAR](6) 
	,[kgco] [NVARCHAR](5) 
	,[kgmcu] [NVARCHAR](12) 
	,[kgobj] [NVARCHAR](6) 
	,[kgsub] [NVARCHAR](8) 
	,[kgdl01] [NVARCHAR](30) 
	,[kgdl02] [NVARCHAR](30) 
	,[kgdl03] [NVARCHAR](30) 
	,[kgdl04] [NVARCHAR](30) 
	,[kgdl05] [NVARCHAR](30) 
	,[kgmopt] [NVARCHAR](1) 
	,[kgoopt] [NVARCHAR](1) 
	,[kgsopt] [NVARCHAR](1) 
	,[kgsy] [NVARCHAR](4) 
	,[kgseqn] [DECIMAL](38, 4) 
	,[kguser] [NVARCHAR](10) 
	,[kgpid] [NVARCHAR](10) 
	,[kgupmj] [VARCHAR](20) 
	,[kgjobn] [NVARCHAR](10) 
	,[kgupmt] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f0012/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
