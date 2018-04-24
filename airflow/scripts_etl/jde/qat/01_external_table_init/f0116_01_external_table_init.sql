--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f0116_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f0116_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f0116_init
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
	,[alan8] [DECIMAL](38, 4) 
	,[aleftb] [VARCHAR](20) 
	,[aleftf] [NVARCHAR](1) 
	,[aladd1] [NVARCHAR](40) 
	,[aladd2] [NVARCHAR](40) 
	,[aladd3] [NVARCHAR](40) 
	,[aladd4] [NVARCHAR](40) 
	,[aladdz] [NVARCHAR](12) 
	,[alcty1] [NVARCHAR](25) 
	,[alcoun] [NVARCHAR](25) 
	,[aladds] [NVARCHAR](3) 
	,[alcrte] [NVARCHAR](4) 
	,[albkml] [NVARCHAR](2) 
	,[alctr] [NVARCHAR](3) 
	,[aluser] [NVARCHAR](10) 
	,[alpid] [NVARCHAR](10) 
	,[alupmj] [VARCHAR](20) 
	,[aljobn] [NVARCHAR](10) 
	,[alupmt] [DECIMAL](38, 4) 
	,[alsyncs] [DECIMAL](38, 4) 
	,[alcaad] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f0116/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
