--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f4071_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4071_init

CREATE EXTERNAL TABLE stg_jde.ext_f4071_init
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
	,[atast] [NVARCHAR](8) 
	,[atprgr] [NVARCHAR](8) 
	,[atcpgp] [NVARCHAR](8) 
	,[atsdgr] [NVARCHAR](8) 
	,[atprfr] [NVARCHAR](2) 
	,[atlbt] [NVARCHAR](1) 
	,[atglc] [NVARCHAR](4) 
	,[atsbif] [NVARCHAR](1) 
	,[atacnt] [NVARCHAR](1) 
	,[atlnty] [NVARCHAR](2) 
	,[atmded] [NVARCHAR](1) 
	,[atabas] [NVARCHAR](1) 
	,[atolvl] [NVARCHAR](1) 
	,[attxb] [NVARCHAR](1) 
	,[atpa01] [NVARCHAR](1) 
	,[atpa02] [NVARCHAR](1) 
	,[atpa03] [NVARCHAR](1) 
	,[atpa04] [NVARCHAR](1) 
	,[atpa05] [NVARCHAR](1) 
	,[aturcd] [NVARCHAR](2) 
	,[aturdt] [VARCHAR](20) 
	,[aturat] [DECIMAL](38, 4) 
	,[aturab] [DECIMAL](38, 4) 
	,[aturrf] [NVARCHAR](15) 
	,[atenbm] [NVARCHAR](1) 
	,[atsrflag] [NVARCHAR](1) 
	,[atusadj] [NVARCHAR](1) 
	,[atatier] [DECIMAL](38, 4) 
	,[atbtier] [DECIMAL](38, 4) 
	,[atbnad] [DECIMAL](38, 4) 
	,[ataprp1] [NVARCHAR](3) 
	,[ataprp2] [NVARCHAR](3) 
	,[ataprp3] [NVARCHAR](3) 
	,[ataprp4] [NVARCHAR](6) 
	,[ataprp5] [NVARCHAR](6) 
	,[ataprp6] [NVARCHAR](6) 
	,[atadjgrp] [NVARCHAR](10) 
	,[atmeadj] [NVARCHAR](1) 
	,[atpdcl] [NVARCHAR](1) 
	,[atuser] [NVARCHAR](10) 
	,[atpid] [NVARCHAR](10) 
	,[atjobn] [NVARCHAR](10) 
	,[atupmj] [VARCHAR](20) 
	,[attday] [DECIMAL](38, 4) 
	,[atdidp] [NVARCHAR](12) 
	,[atpmtn] [NVARCHAR](12) 
	,[atphst] [NVARCHAR](1) 
	,[atpa06] [NVARCHAR](1) 
	,[atpa07] [NVARCHAR](1) 
	,[atpa08] [NVARCHAR](1) 
	,[atpa09] [NVARCHAR](1) 
	,[atpa10] [NVARCHAR](1) 
	,[atefcn] [NVARCHAR](1) 
	,[ataptype] [NVARCHAR](2) 
	,[atmoadj] [NVARCHAR](1) 
	,[atplgrp] [NVARCHAR](3) 
	,[atexcpl] [NVARCHAR](1) 
	,[atupmx] [VARCHAR](20) 
	,[atmnmxaj] [NVARCHAR](1) 
	,[atmnmxrl] [NVARCHAR](1) 
	,[attstrsnm] [NVARCHAR](30) 
	,[atadjqty] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f4071/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
