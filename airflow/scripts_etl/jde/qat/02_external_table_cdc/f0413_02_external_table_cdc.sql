--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f0413_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f0413_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f0413_cdc
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
	,[rmpyid] [DECIMAL](38, 4) 
	,[rmdctm] [NVARCHAR](2) 
	,[rmdocm] [DECIMAL](38, 4) 
	,[rmpye] [DECIMAL](38, 4) 
	,[rmglba] [NVARCHAR](8) 
	,[rmdmtj] [VARCHAR](20) 
	,[rmvdgj] [VARCHAR](20) 
	,[rmicu] [DECIMAL](38, 4) 
	,[rmicut] [NVARCHAR](2) 
	,[rmdicj] [VARCHAR](20) 
	,[rmpaap] [DECIMAL](38, 4) 
	,[rmcrcd] [NVARCHAR](3) 
	,[rmcrrm] [NVARCHAR](1) 
	,[rmam] [NVARCHAR](1) 
	,[rmvldt] [VARCHAR](20) 
	,[rmpyin] [NVARCHAR](1) 
	,[rmistp] [NVARCHAR](1) 
	,[rmcbnk] [NVARCHAR](20) 
	,[rmbktr] [NVARCHAR](8) 
	,[rmtorg] [NVARCHAR](10) 
	,[rmuser] [NVARCHAR](10) 
	,[rmpid] [NVARCHAR](10) 
	,[rmupmj] [VARCHAR](20) 
	,[rmupmt] [DECIMAL](38, 4) 
	,[rmjobn] [NVARCHAR](10) 
	,[rmmip] [NVARCHAR](1) 
	,[rmlrfl] [NVARCHAR](1) 
	,[rmprgf] [NVARCHAR](1) 
	,[rmgfl7] [NVARCHAR](1) 
	,[rmgfl8] [NVARCHAR](1) 
	,[rmgam3] [DECIMAL](38, 4) 
	,[rmgam4] [DECIMAL](38, 4) 
	,[rmgen6] [NVARCHAR](25) 
	,[rmgen7] [NVARCHAR](25) 
	,[rmnettcid] [DECIMAL](38, 4) 
	,[rmnetdoc] [DECIMAL](38, 4) 
	,[rmrcnd] [NVARCHAR](1) 
	,[rmr3] [NVARCHAR](8) 
	,[rmcntrtid] [DECIMAL](38, 4) 
	,[rmcntrtcd] [NVARCHAR](12) 
	,[rmwvid] [DECIMAL](38, 4) 
	,[rmblscd2] [NVARCHAR](10) 
	,[rmharper] [NVARCHAR](6) 
	,[rmharsfx] [NVARCHAR](10) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f0413/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
