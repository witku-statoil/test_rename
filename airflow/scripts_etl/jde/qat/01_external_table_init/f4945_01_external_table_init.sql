--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f4945_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4945_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f4945_init
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
	,[scshpn] [DECIMAL](38, 4) 
	,[scrssn] [DECIMAL](38, 4) 
	,[scvmcu] [NVARCHAR](12) 
	,[scldnm] [DECIMAL](38, 4) 
	,[scdlno] [DECIMAL](38, 4) 
	,[scoseq] [DECIMAL](38, 4) 
	,[scrtnm] [NVARCHAR](10) 
	,[scsdsq] [DECIMAL](38, 4) 
	,[scscsn] [DECIMAL](38, 4) 
	,[scnmfc] [NVARCHAR](4) 
	,[scdsgp] [NVARCHAR](3) 
	,[scfrt1] [NVARCHAR](6) 
	,[scfrt2] [NVARCHAR](6) 
	,[sccgc1] [NVARCHAR](3) 
	,[scag] [DECIMAL](38, 4) 
	,[scblpb] [NVARCHAR](1) 
	,[sccrdc] [NVARCHAR](3) 
	,[scfaa] [DECIMAL](38, 4) 
	,[scnamf] [DECIMAL](38, 4) 
	,[scrtdq] [DECIMAL](38, 4) 
	,[scnamt] [DECIMAL](38, 4) 
	,[scuom] [NVARCHAR](2) 
	,[scrtgb] [NVARCHAR](1) 
	,[sccrcd] [NVARCHAR](3) 
	,[scdoco] [DECIMAL](38, 4) 
	,[scdcto] [NVARCHAR](2) 
	,[sckcoo] [NVARCHAR](5) 
	,[sclnid] [DECIMAL](38, 4) 
	,[scovfg] [NVARCHAR](1) 
	,[scukid] [DECIMAL](38, 4) 
	,[scuk01] [DECIMAL](38, 4) 
	,[scurcd] [NVARCHAR](2) 
	,[scurdt] [VARCHAR](20) 
	,[scurat] [DECIMAL](38, 4) 
	,[scurab] [DECIMAL](38, 4) 
	,[scurrf] [NVARCHAR](15) 
	,[scuser] [NVARCHAR](10) 
	,[scpid] [NVARCHAR](10) 
	,[scjobn] [NVARCHAR](10) 
	,[scupmj] [VARCHAR](20) 
	,[sctday] [DECIMAL](38, 4) 
	,[scrtn] [DECIMAL](38, 4) 
	,[sclnmb] [DECIMAL](38, 4) 
	,[scprte] [NVARCHAR](1) 
	,[sceftj] [VARCHAR](20) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4945/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
