--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f41291_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f41291_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f41291_cdc
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
	,[igprp5] [NVARCHAR](3) 
	,[igitm] [DECIMAL](38, 4) 
	,[iglitm] [NVARCHAR](25) 
	,[igaitm] [NVARCHAR](25) 
	,[igmcu] [NVARCHAR](12) 
	,[iglocn] [NVARCHAR](20) 
	,[iglotn] [NVARCHAR](30) 
	,[iglvla] [NVARCHAR](3) 
	,[iglvlb] [NVARCHAR](3) 
	,[igpcst] [DECIMAL](38, 4) 
	,[igpamt] [DECIMAL](38, 4) 
	,[igratf] [DECIMAL](38, 4) 
	,[igratv] [DECIMAL](38, 4) 
	,[igan8] [DECIMAL](38, 4) 
	,[igglc] [NVARCHAR](4) 
	,[igglpt] [NVARCHAR](4) 
	,[igefff] [VARCHAR](20) 
	,[igefft] [VARCHAR](20) 
	,[igtx] [NVARCHAR](1) 
	,[iginyn] [NVARCHAR](1) 
	,[igrcyn] [NVARCHAR](1) 
	,[igaisl] [NVARCHAR](8) 
	,[igbin] [NVARCHAR](8) 
	,[iguser] [NVARCHAR](10) 
	,[igjobn] [NVARCHAR](10) 
	,[igpid] [NVARCHAR](10) 
	,[igupmj] [VARCHAR](20) 
	,[igtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f41291/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
