--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f41514_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f41514_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f41514_cdc
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
	,[rhmcu] [NVARCHAR](12) 
	,[rhitm] [DECIMAL](38, 4) 
	,[rhtkid] [NVARCHAR](8) 
	,[rhopdt] [VARCHAR](20) 
	,[rhrttm] [DECIMAL](38, 4) 
	,[rhincn] [DECIMAL](38, 4) 
	,[rhbum0] [NVARCHAR](2) 
	,[rhoutg] [DECIMAL](38, 4) 
	,[rhbum1] [NVARCHAR](2) 
	,[rhopns] [DECIMAL](38, 4) 
	,[rhbum2] [NVARCHAR](2) 
	,[rhclos] [DECIMAL](38, 4) 
	,[rhbum3] [NVARCHAR](2) 
	,[rhglqt] [DECIMAL](38, 4) 
	,[rhbum4] [NVARCHAR](2) 
	,[rhuser] [NVARCHAR](10) 
	,[rhpid] [NVARCHAR](10) 
	,[rhjobn] [NVARCHAR](10) 
	,[rhupmj] [VARCHAR](20) 
	,[rhtday] [DECIMAL](38, 4) 
	,[rhinam] [DECIMAL](38, 4) 
	,[rhhum1] [NVARCHAR](2) 
	,[rhinwg] [DECIMAL](38, 4) 
	,[rhhum2] [NVARCHAR](2) 
	,[rhogam] [DECIMAL](38, 4) 
	,[rhhum3] [NVARCHAR](2) 
	,[rhogwg] [DECIMAL](38, 4) 
	,[rhhum4] [NVARCHAR](2) 
	,[rhosam] [DECIMAL](38, 4) 
	,[rhhum5] [NVARCHAR](2) 
	,[rhoswg] [DECIMAL](38, 4) 
	,[rhhum6] [NVARCHAR](2) 
	,[rhcsam] [DECIMAL](38, 4) 
	,[rhhum7] [NVARCHAR](2) 
	,[rhcswg] [DECIMAL](38, 4) 
	,[rhhum8] [NVARCHAR](2) 
	,[rhglam] [DECIMAL](38, 4) 
	,[rhhum9] [NVARCHAR](2) 
	,[rhglwg] [DECIMAL](38, 4) 
	,[rhhuma] [NVARCHAR](2) 
	,[rhurrf] [NVARCHAR](15) 
	,[rhurdt] [VARCHAR](20) 
	,[rhurcd] [NVARCHAR](2) 
	,[rhurat] [DECIMAL](38, 4) 
	,[rhurab] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f41514/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
