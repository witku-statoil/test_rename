--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f3411_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f3411_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f3411_cdc
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
	,[mmukid] [DECIMAL](38, 4) 
	,[mmitm] [DECIMAL](38, 4) 
	,[mmmcu] [NVARCHAR](12) 
	,[mmmmcu] [NVARCHAR](12) 
	,[mmmsgt] [NVARCHAR](1) 
	,[mmmsga] [NVARCHAR](1) 
	,[mmhcld] [NVARCHAR](1) 
	,[mmkcoo] [NVARCHAR](5) 
	,[mmdoco] [DECIMAL](38, 4) 
	,[mmdcto] [NVARCHAR](2) 
	,[mmlnid] [DECIMAL](38, 4) 
	,[mmsfxo] [NVARCHAR](3) 
	,[mmdsc1] [NVARCHAR](30) 
	,[mmtrqt] [DECIMAL](38, 4) 
	,[mmvend] [DECIMAL](38, 4) 
	,[mmdrqj] [VARCHAR](20) 
	,[mmstrt] [VARCHAR](20) 
	,[mmrstj] [VARCHAR](20) 
	,[mmrrqj] [VARCHAR](20) 
	,[mmupmj] [VARCHAR](20) 
	,[mmtday] [DECIMAL](38, 4) 
	,[mmuser] [NVARCHAR](10) 
	,[mmjobn] [NVARCHAR](10) 
	,[mmpid] [NVARCHAR](10) 
	,[mmredj] [VARCHAR](20) 
	,[mmoedj] [VARCHAR](20) 
	,[mmline] [NVARCHAR](12) 
	,[mmprjm] [DECIMAL](38, 4) 
	,[mmsrdm] [DECIMAL](38, 4) 
	,[mmostt] [NVARCHAR](1) 
	,[mmlotn] [NVARCHAR](30) 
	,[mmpns] [DECIMAL](38, 4) 
	,[mmpmpn] [NVARCHAR](30) 
)
WITH
(
    LOCATION='processing-clean/jde/f3411/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
