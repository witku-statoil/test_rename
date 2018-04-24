--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f4961_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4961_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f4961_cdc
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
	,[llvmcu] [NVARCHAR](12) 
	,[llldnm] [DECIMAL](38, 4) 
	,[lltrpl] [DECIMAL](38, 4) 
	,[llorgn] [DECIMAL](38, 4) 
	,[llnmcu] [NVARCHAR](12) 
	,[llstsq] [DECIMAL](38, 4) 
	,[llscwt] [DECIMAL](38, 4) 
	,[llwtum] [NVARCHAR](2) 
	,[llscvl] [DECIMAL](38, 4) 
	,[llvlum] [NVARCHAR](2) 
	,[llsccu] [DECIMAL](38, 4) 
	,[llcvum] [NVARCHAR](2) 
	,[lllddt] [VARCHAR](20) 
	,[llldtm] [DECIMAL](38, 4) 
	,[llload] [VARCHAR](20) 
	,[lltmls] [DECIMAL](38, 4) 
	,[llppdj] [VARCHAR](20) 
	,[llpmdt] [DECIMAL](38, 4) 
	,[lladdj] [VARCHAR](20) 
	,[lladtm] [DECIMAL](38, 4) 
	,[lllrfg] [NVARCHAR](1) 
	,[llpcsd] [NVARCHAR](10) 
	,[lldwnc] [DECIMAL](38, 4) 
	,[llurcd] [NVARCHAR](2) 
	,[llurdt] [VARCHAR](20) 
	,[llurat] [DECIMAL](38, 4) 
	,[llurab] [DECIMAL](38, 4) 
	,[llurrf] [NVARCHAR](15) 
	,[lluser] [NVARCHAR](10) 
	,[llpid] [NVARCHAR](10) 
	,[lljobn] [NVARCHAR](10) 
	,[llupmj] [VARCHAR](20) 
	,[lltday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4961/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
