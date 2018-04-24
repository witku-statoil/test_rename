--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f4931_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4931_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f4931_cdc
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
	,[vgvtyp] [NVARCHAR](12) 
	,[vgdl01] [NVARCHAR](30) 
	,[vgmcu] [NVARCHAR](12) 
	,[vgmot] [NVARCHAR](3) 
	,[vgdsgp] [NVARCHAR](3) 
	,[vgdsgs] [NVARCHAR](3) 
	,[vgnce] [DECIMAL](38, 4) 
	,[vgwtem] [DECIMAL](38, 4) 
	,[vgwtca] [DECIMAL](38, 4) 
	,[vgwtum] [NVARCHAR](2) 
	,[vgvlcp] [DECIMAL](38, 4) 
	,[vgvlcs] [DECIMAL](38, 4) 
	,[vgvlum] [NVARCHAR](2) 
	,[vgcvol] [DECIMAL](38, 4) 
	,[vgcvum] [NVARCHAR](2) 
	,[vglcnt] [DECIMAL](38, 4) 
	,[vgaxle] [DECIMAL](38, 4) 
	,[vgwtax] [DECIMAL](38, 4) 
	,[vgseal] [DECIMAL](38, 4) 
	,[vglnle] [DECIMAL](38, 4) 
	,[vglnwe] [DECIMAL](38, 4) 
	,[vglnhe] [DECIMAL](38, 4) 
	,[vglnli] [DECIMAL](38, 4) 
	,[vglnwi] [DECIMAL](38, 4) 
	,[vglnhr] [DECIMAL](38, 4) 
	,[vglnhc] [DECIMAL](38, 4) 
	,[vglnhf] [DECIMAL](38, 4) 
	,[vglhrd] [DECIMAL](38, 4) 
	,[vglwrd] [DECIMAL](38, 4) 
	,[vglhsd] [DECIMAL](38, 4) 
	,[vglwsd] [DECIMAL](38, 4) 
	,[vglnvf] [DECIMAL](38, 4) 
	,[vgluom] [NVARCHAR](2) 
	,[vgcpfg] [NVARCHAR](1) 
	,[vgbpfg] [NVARCHAR](1) 
	,[vgmlln] [NVARCHAR](1) 
	,[vgurcd] [NVARCHAR](2) 
	,[vgurdt] [VARCHAR](20) 
	,[vgurab] [DECIMAL](38, 4) 
	,[vgurrf] [NVARCHAR](15) 
	,[vguser] [NVARCHAR](10) 
	,[vgpid] [NVARCHAR](10) 
	,[vgjobn] [NVARCHAR](10) 
	,[vgupmj] [VARCHAR](20) 
	,[vgtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4931/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)