--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f4008_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4008_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f4008_cdc
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
	,[tatxa1] [NVARCHAR](10) 
	,[tataxa] [NVARCHAR](30) 
	,[tata1] [DECIMAL](38, 4) 
	,[tatxr1] [DECIMAL](38, 4) 
	,[tata2] [DECIMAL](38, 4) 
	,[tatxr2] [DECIMAL](38, 4) 
	,[tata3] [DECIMAL](38, 4) 
	,[tatxr3] [DECIMAL](38, 4) 
	,[tata4] [DECIMAL](38, 4) 
	,[tatxr4] [DECIMAL](38, 4) 
	,[tata5] [DECIMAL](38, 4) 
	,[tatxr5] [DECIMAL](38, 4) 
	,[taefdj] [VARCHAR](20) 
	,[taeftj] [VARCHAR](20) 
	,[tagl01] [NVARCHAR](4) 
	,[tagl02] [NVARCHAR](4) 
	,[tagl03] [NVARCHAR](4) 
	,[tagl04] [NVARCHAR](4) 
	,[tagl05] [NVARCHAR](4) 
	,[taitm] [DECIMAL](38, 4) 
	,[talitm] [NVARCHAR](25) 
	,[taaitm] [NVARCHAR](25) 
	,[tauom] [NVARCHAR](2) 
	,[tafvty] [NVARCHAR](1) 
	,[tamtax] [DECIMAL](38, 4) 
	,[tatc1] [NVARCHAR](1) 
	,[tatc2] [NVARCHAR](1) 
	,[tatc3] [NVARCHAR](1) 
	,[tatc4] [NVARCHAR](1) 
	,[tatc5] [NVARCHAR](1) 
	,[tatt1] [NVARCHAR](1) 
	,[tatt2] [NVARCHAR](1) 
	,[tatt3] [NVARCHAR](1) 
	,[tatt4] [NVARCHAR](1) 
	,[tatt5] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f4008/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
