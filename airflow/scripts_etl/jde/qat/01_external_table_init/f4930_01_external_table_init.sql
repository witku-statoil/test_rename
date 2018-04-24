--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f4930_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4930_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f4930_init
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
	,[vmvehi] [NVARCHAR](12) 
	,[vmvehs] [NVARCHAR](25) 
	,[vmdl01] [NVARCHAR](30) 
	,[vmlrfg] [NVARCHAR](1) 
	,[vmmcu] [NVARCHAR](12) 
	,[vmvtyp] [NVARCHAR](12) 
	,[vmdumv] [NVARCHAR](1) 
	,[vmvown] [DECIMAL](38, 4) 
	,[vmnce] [DECIMAL](38, 4) 
	,[vmwtem] [DECIMAL](38, 4) 
	,[vmwtca] [DECIMAL](38, 4) 
	,[vmwtum] [NVARCHAR](2) 
	,[vmvlcp] [DECIMAL](38, 4) 
	,[vmvlcs] [DECIMAL](38, 4) 
	,[vmvlum] [NVARCHAR](2) 
	,[vmcvol] [DECIMAL](38, 4) 
	,[vmcvum] [NVARCHAR](2) 
	,[vmlcnt] [DECIMAL](38, 4) 
	,[vmmxml] [DECIMAL](38, 4) 
	,[vmumd1] [NVARCHAR](2) 
	,[vminmg] [NVARCHAR](10) 
	,[vmmest] [NVARCHAR](1) 
	,[vmvehn] [DECIMAL](38, 4) 
	,[vmurcd] [NVARCHAR](2) 
	,[vmurdt] [VARCHAR](20) 
	,[vmurat] [DECIMAL](38, 4) 
	,[vmurab] [DECIMAL](38, 4) 
	,[vmurrf] [NVARCHAR](15) 
	,[vmuser] [NVARCHAR](10) 
	,[vmpid] [NVARCHAR](10) 
	,[vmjobn] [NVARCHAR](10) 
	,[vmupmj] [VARCHAR](20) 
	,[vmtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4930/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
