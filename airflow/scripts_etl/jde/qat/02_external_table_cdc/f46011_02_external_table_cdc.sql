--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f46011_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f46011_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f46011_cdc
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
	,[iqmcu] [NVARCHAR](12) 
	,[iqprp6] [NVARCHAR](6) 
	,[iqitm] [DECIMAL](38, 4) 
	,[iquom] [NVARCHAR](2) 
	,[iqgwid] [DECIMAL](38, 4) 
	,[iqgdep] [DECIMAL](38, 4) 
	,[iqghet] [DECIMAL](38, 4) 
	,[iqwium] [NVARCHAR](2) 
	,[iqgcub] [DECIMAL](38, 4) 
	,[iqvumd] [NVARCHAR](2) 
	,[iqgwei] [DECIMAL](38, 4) 
	,[iquwum] [NVARCHAR](2) 
	,[iqdmth] [NVARCHAR](1) 
	,[iqcrmt] [NVARCHAR](1) 
	,[iqequs] [NVARCHAR](1) 
	,[iqarot] [NVARCHAR](1) 
	,[iqabkd] [NVARCHAR](1) 
	,[iqarol] [NVARCHAR](1) 
	,[iqslim] [DECIMAL](38, 4) 
	,[iqeqty] [NVARCHAR](5) 
	,[iqrpck] [NVARCHAR](1) 
	,[iqpack] [NVARCHAR](4) 
	,[iqlipl] [NVARCHAR](1) 
	,[iqpptg] [NVARCHAR](1) 
	,[iqpktg] [NVARCHAR](1) 
	,[iqprtg] [NVARCHAR](1) 
	,[iqptra] [NVARCHAR](3) 
	,[iqktra] [NVARCHAR](3) 
	,[iqrtra] [NVARCHAR](3) 
	,[iquser] [NVARCHAR](10) 
	,[iqpid] [NVARCHAR](10) 
	,[iqjobn] [NVARCHAR](10) 
	,[iqupmj] [VARCHAR](20) 
	,[iqtday] [DECIMAL](38, 4) 
	,[iquccu] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f46011/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
