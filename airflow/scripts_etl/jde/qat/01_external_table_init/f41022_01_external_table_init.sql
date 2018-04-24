--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f41022_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f41022_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f41022_init
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
	,[pnmcu] [NVARCHAR](12) 
	,[pnitm] [DECIMAL](38, 4) 
	,[pnstvl] [DECIMAL](38, 4) 
	,[pnbum0] [NVARCHAR](2) 
	,[pnlrfg] [NVARCHAR](1) 
	,[pnhcor] [NVARCHAR](1) 
	,[pnacor] [NVARCHAR](1) 
	,[pnabbl] [NVARCHAR](1) 
	,[pnatwh] [NVARCHAR](1) 
	,[pnrplt] [NVARCHAR](3) 
	,[pnbcat] [NVARCHAR](3) 
	,[pnfcat] [NVARCHAR](3) 
	,[pnrecd] [VARCHAR](20) 
	,[pnurcd] [NVARCHAR](2) 
	,[pnurat] [DECIMAL](38, 4) 
	,[pnurab] [DECIMAL](38, 4) 
	,[pnurrf] [NVARCHAR](15) 
	,[pnurdt] [VARCHAR](20) 
	,[pnuser] [NVARCHAR](10) 
	,[pnpid] [NVARCHAR](10) 
	,[pnjobn] [NVARCHAR](10) 
	,[pnupmj] [VARCHAR](20) 
	,[pntday] [DECIMAL](38, 4) 
	,[pnrtob] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f41022/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
