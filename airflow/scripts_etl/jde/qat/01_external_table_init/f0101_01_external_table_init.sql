--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f0101_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f0101_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f0101_init
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
	,[aban8] [DECIMAL](38, 4) 
	,[abalky] [NVARCHAR](20) 
	,[abtax] [NVARCHAR](20) 
	,[abalph] [NVARCHAR](40) 
	,[abdc] [NVARCHAR](40) 
	,[abmcu] [NVARCHAR](12) 
	,[absic] [NVARCHAR](10) 
	,[ablngp] [NVARCHAR](2) 
	,[abat1] [NVARCHAR](3) 
	,[abcm] [NVARCHAR](2) 
	,[abtaxc] [NVARCHAR](1) 
	,[abat2] [NVARCHAR](1) 
	,[abat3] [NVARCHAR](1) 
	,[abat4] [NVARCHAR](1) 
	,[abat5] [NVARCHAR](1) 
	,[abatp] [NVARCHAR](1) 
	,[abatr] [NVARCHAR](1) 
	,[abatpr] [NVARCHAR](1) 
	,[abab3] [NVARCHAR](1) 
	,[abate] [NVARCHAR](1) 
	,[absbli] [NVARCHAR](1) 
	,[abeftb] [VARCHAR](20) 
	,[aban81] [DECIMAL](38, 4) 
	,[aban82] [DECIMAL](38, 4) 
	,[aban83] [DECIMAL](38, 4) 
	,[aban84] [DECIMAL](38, 4) 
	,[aban86] [DECIMAL](38, 4) 
	,[aban85] [DECIMAL](38, 4) 
	,[abac01] [NVARCHAR](3) 
	,[abac02] [NVARCHAR](3) 
	,[abac03] [NVARCHAR](3) 
	,[abac04] [NVARCHAR](3) 
	,[abac05] [NVARCHAR](3) 
	,[abac06] [NVARCHAR](3) 
	,[abac07] [NVARCHAR](3) 
	,[abac08] [NVARCHAR](3) 
	,[abac09] [NVARCHAR](3) 
	,[abac10] [NVARCHAR](3) 
	,[abac11] [NVARCHAR](3) 
	,[abac12] [NVARCHAR](3) 
	,[abac13] [NVARCHAR](3) 
	,[abac14] [NVARCHAR](3) 
	,[abac15] [NVARCHAR](3) 
	,[abac16] [NVARCHAR](3) 
	,[abac17] [NVARCHAR](3) 
	,[abac18] [NVARCHAR](3) 
	,[abac19] [NVARCHAR](3) 
	,[abac20] [NVARCHAR](3) 
	,[abac21] [NVARCHAR](3) 
	,[abac22] [NVARCHAR](3) 
	,[abac23] [NVARCHAR](3) 
	,[abac24] [NVARCHAR](3) 
	,[abac25] [NVARCHAR](3) 
	,[abac26] [NVARCHAR](3) 
	,[abac27] [NVARCHAR](3) 
	,[abac28] [NVARCHAR](3) 
	,[abac29] [NVARCHAR](3) 
	,[abac30] [NVARCHAR](3) 
	,[abglba] [NVARCHAR](8) 
	,[abpti] [DECIMAL](38, 4) 
	,[abpdi] [VARCHAR](20) 
	,[abmsga] [NVARCHAR](1) 
	,[abrmk] [NVARCHAR](30) 
	,[abtxct] [NVARCHAR](20) 
	,[abtx2] [NVARCHAR](20) 
	,[abalp1] [NVARCHAR](40) 
	,[aburcd] [NVARCHAR](2) 
	,[aburdt] [VARCHAR](20) 
	,[aburat] [DECIMAL](38, 4) 
	,[aburab] [DECIMAL](38, 4) 
	,[aburrf] [NVARCHAR](15) 
	,[abuser] [NVARCHAR](10) 
	,[abpid] [NVARCHAR](10) 
	,[abupmj] [VARCHAR](20) 
	,[abjobn] [NVARCHAR](10) 
	,[abupmt] [DECIMAL](38, 4) 
	,[abprgf] [NVARCHAR](1) 
	,[absccltp] [NVARCHAR](2) 
	,[abticker] [NVARCHAR](10) 
	,[abexchg] [NVARCHAR](10) 
	,[abduns] [NVARCHAR](13) 
	,[abclass01] [NVARCHAR](3) 
	,[abclass02] [NVARCHAR](3) 
	,[abclass03] [NVARCHAR](3) 
	,[abclass04] [NVARCHAR](3) 
	,[abclass05] [NVARCHAR](3) 
	,[abnoe] [DECIMAL](38, 4) 
	,[abgrowthr] [DECIMAL](38, 4) 
	,[abyearstar] [NVARCHAR](15) 
	,[abaempgp] [NVARCHAR](5) 
	,[abactin] [NVARCHAR](1) 
	,[abrevrng] [NVARCHAR](5) 
	,[absyncs] [DECIMAL](38, 4) 
	,[abperrs] [DECIMAL](38, 4) 
	,[abcaad] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f0101/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
