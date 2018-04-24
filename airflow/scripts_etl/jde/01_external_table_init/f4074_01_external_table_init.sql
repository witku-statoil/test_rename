--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f4074_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4074_init

CREATE EXTERNAL TABLE stg_jde.ext_f4074_init
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
	,[aldoco] [DECIMAL](38, 4) 
	,[aldcto] [NVARCHAR](2) 
	,[alkcoo] [NVARCHAR](5) 
	,[alsfxo] [NVARCHAR](3) 
	,[allnid] [DECIMAL](38, 4) 
	,[alakid] [DECIMAL](38, 4) 
	,[alsrcfd] [NVARCHAR](2) 
	,[aloseq] [DECIMAL](38, 4) 
	,[alsubseq] [DECIMAL](38, 4) 
	,[altier] [DECIMAL](38, 4) 
	,[alasn] [NVARCHAR](8) 
	,[alast] [NVARCHAR](8) 
	,[alitm] [DECIMAL](38, 4) 
	,[alan8] [DECIMAL](38, 4) 
	,[alcrcd] [NVARCHAR](3) 
	,[aluom] [NVARCHAR](2) 
	,[almnq] [DECIMAL](38, 4) 
	,[alledg] [NVARCHAR](2) 
	,[alfrmn] [NVARCHAR](10) 
	,[albscd] [NVARCHAR](1) 
	,[alfvtr] [DECIMAL](38, 4) 
	,[alabas] [NVARCHAR](1) 
	,[aluprc] [DECIMAL](38, 4) 
	,[alfup] [DECIMAL](38, 4) 
	,[alglc] [NVARCHAR](4) 
	,[alarsn] [NVARCHAR](3) 
	,[alacnt] [NVARCHAR](1) 
	,[alsbif] [NVARCHAR](1) 
	,[almded] [NVARCHAR](1) 
	,[alprov] [NVARCHAR](1) 
	,[alatid] [DECIMAL](38, 4) 
	,[aligid] [DECIMAL](38, 4) 
	,[alcgid] [DECIMAL](38, 4) 
	,[alogid] [DECIMAL](38, 4) 
	,[alanps] [DECIMAL](38, 4) 
	,[albsdval] [DECIMAL](38, 4) 
	,[alsrflag] [NVARCHAR](1) 
	,[aladjcal] [NVARCHAR](80) 
	,[alnbrord] [DECIMAL](38, 4) 
	,[aluomvid] [NVARCHAR](2) 
	,[alolvl] [NVARCHAR](1) 
	,[aladjsts] [NVARCHAR](1) 
	,[aladjref] [NVARCHAR](15) 
	,[alaccan8] [DECIMAL](38, 4) 
	,[albnad] [DECIMAL](38, 4) 
	,[aladjgrp] [NVARCHAR](10) 
	,[almeadj] [NVARCHAR](1) 
	,[alfvum] [NVARCHAR](2) 
	,[alpdcl] [NVARCHAR](1) 
	,[alcfgid] [DECIMAL](38, 4) 
	,[alcfgcid] [DECIMAL](38, 4) 
	,[alaprp1] [NVARCHAR](3) 
	,[alaprp2] [NVARCHAR](3) 
	,[alaprp3] [NVARCHAR](3) 
	,[alaprp4] [NVARCHAR](6) 
	,[alaprp5] [NVARCHAR](6) 
	,[alaprp6] [NVARCHAR](6) 
	,[alndpi] [NVARCHAR](1) 
	,[aluser] [NVARCHAR](10) 
	,[alpid] [NVARCHAR](10) 
	,[aljobn] [NVARCHAR](10) 
	,[alupmj] [VARCHAR](20) 
	,[altday] [DECIMAL](38, 4) 
	,[alpmtn] [NVARCHAR](12) 
	,[alrulename] [NVARCHAR](10) 
	,[alpa04] [NVARCHAR](1) 
	,[aladjqty] [NVARCHAR](1) 
	,[alqtypy] [DECIMAL](38, 4) 
	,[alstprcf] [NVARCHAR](1) 
	,[altstrsnm] [NVARCHAR](30) 
)
WITH
(
    LOCATION='processing-clean/jde/f4074/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
