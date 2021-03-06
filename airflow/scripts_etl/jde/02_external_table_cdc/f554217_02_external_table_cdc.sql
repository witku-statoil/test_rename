--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f554217_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f554217_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f554217_cdc
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
	,[span8] [DECIMAL](38, 4) 
	,[spac06] [NVARCHAR](3) 
	,[spaddz] [NVARCHAR](12) 
	,[spshan] [DECIMAL](38, 4) 
	,[spat1] [NVARCHAR](3) 
	,[spac01] [NVARCHAR](3) 
	,[spac02] [NVARCHAR](3) 
	,[spac03] [NVARCHAR](3) 
	,[spar1] [NVARCHAR](6) 
	,[spph1] [NVARCHAR](20) 
	,[spcmtb] [VARCHAR](20) 
	,[spptmb] [DECIMAL](38, 4) 
	,[spcmte] [VARCHAR](20) 
	,[spev01] [NVARCHAR](1) 
	,[spptme] [DECIMAL](38, 4) 
	,[spurab] [DECIMAL](38, 4) 
	,[spupmj] [VARCHAR](20) 
	,[spupmt] [DECIMAL](38, 4) 
	,[spjobn] [NVARCHAR](10) 
	,[sppid] [NVARCHAR](10) 
	,[spuser] [NVARCHAR](10) 
	,[spmcu] [NVARCHAR](12) 
	,[sprp24] [NVARCHAR](10) 
	,[spy55char1] [NVARCHAR](1) 
	,[spy55char2] [NVARCHAR](1) 
	,[spy55strg1] [NVARCHAR](30) 
	,[spy55strg2] [NVARCHAR](30) 
	,[spy55amnt1] [DECIMAL](38, 4) 
	,[spy55amnt2] [DECIMAL](38, 4) 
	,[spy55time1] [DECIMAL](38, 4) 
	,[spy55time2] [DECIMAL](38, 4) 
	,[spy55date1] [VARCHAR](20) 
	,[spy55date2] [VARCHAR](20) 
	,[spcrtj] [VARCHAR](20) 
	,[spterm] [NVARCHAR](10) 
	,[sppa8] [DECIMAL](38, 4) 
	,[sptx1] [NVARCHAR](16) 
	,[sptax] [NVARCHAR](20) 
	,[spaddznh] [NVARCHAR](12) 
)
WITH
(
    LOCATION='processing-clean/jde/f554217/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
