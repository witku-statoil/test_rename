--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f4301_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4301_init

CREATE EXTERNAL TABLE stg_jde.ext_f4301_init
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
	,[phkcoo] [NVARCHAR](5) 
	,[phdoco] [DECIMAL](38, 4) 
	,[phdcto] [NVARCHAR](2) 
	,[phsfxo] [NVARCHAR](3) 
	,[phmcu] [NVARCHAR](12) 
	,[phokco] [NVARCHAR](5) 
	,[phoorn] [NVARCHAR](8) 
	,[phocto] [NVARCHAR](2) 
	,[phrkco] [NVARCHAR](5) 
	,[phrorn] [NVARCHAR](8) 
	,[phrcto] [NVARCHAR](2) 
	,[phan8] [DECIMAL](38, 4) 
	,[phshan] [DECIMAL](38, 4) 
	,[phdrqj] [VARCHAR](20) 
	,[phtrdj] [VARCHAR](20) 
	,[phpddj] [VARCHAR](20) 
	,[phopdj] [VARCHAR](20) 
	,[phaddj] [VARCHAR](20) 
	,[phcndj] [VARCHAR](20) 
	,[phpefj] [VARCHAR](20) 
	,[phppdj] [VARCHAR](20) 
	,[phpsdj] [VARCHAR](20) 
	,[phvr01] [NVARCHAR](25) 
	,[phvr02] [NVARCHAR](25) 
	,[phdel1] [NVARCHAR](30) 
	,[phdel2] [NVARCHAR](30) 
	,[phrmk] [NVARCHAR](30) 
	,[phdesc] [NVARCHAR](30) 
	,[phinmg] [NVARCHAR](10) 
	,[phasn] [NVARCHAR](8) 
	,[phprgp] [NVARCHAR](8) 
	,[phptc] [NVARCHAR](3) 
	,[phexr1] [NVARCHAR](2) 
	,[phtxa1] [NVARCHAR](10) 
	,[phtxct] [NVARCHAR](20) 
	,[phhold] [NVARCHAR](2) 
	,[phatxt] [NVARCHAR](1) 
	,[phinvc] [DECIMAL](38, 4) 
	,[phntr] [NVARCHAR](2) 
	,[phcnid] [NVARCHAR](20) 
	,[phfrth] [NVARCHAR](3) 
	,[phzon] [NVARCHAR](3) 
	,[phanby] [DECIMAL](38, 4) 
	,[phancr] [DECIMAL](38, 4) 
	,[phmot] [NVARCHAR](3) 
	,[phcot] [NVARCHAR](3) 
	,[phrcd] [NVARCHAR](3) 
	,[phfrtc] [NVARCHAR](1) 
	,[phfuf1] [NVARCHAR](1) 
	,[phfuf2] [NVARCHAR](1) 
	,[photot] [DECIMAL](38, 4) 
	,[phpcrt] [DECIMAL](38, 4) 
	,[phrtnr] [NVARCHAR](3) 
	,[phwumd] [NVARCHAR](2) 
	,[phvumd] [NVARCHAR](2) 
	,[phpurg] [NVARCHAR](1) 
	,[phlgct] [NVARCHAR](1) 
	,[phprom] [NVARCHAR](1) 
	,[phmaty] [NVARCHAR](1) 
	,[phosts] [NVARCHAR](1) 
	,[phavch] [NVARCHAR](1) 
	,[phprpy] [NVARCHAR](1) 
	,[phcrmd] [NVARCHAR](1) 
	,[phprp5] [NVARCHAR](3) 
	,[phartg] [NVARCHAR](12) 
	,[phcord] [DECIMAL](38, 4) 
	,[phcrrm] [NVARCHAR](1) 
	,[phcrcd] [NVARCHAR](3) 
	,[phcrr] [DECIMAL](38, 4) 
	,[phlngp] [NVARCHAR](2) 
	,[phfap] [DECIMAL](38, 4) 
	,[phorby] [NVARCHAR](10) 
	,[phtkby] [NVARCHAR](10) 
	,[phurcd] [NVARCHAR](2) 
	,[phurdt] [VARCHAR](20) 
	,[phurat] [DECIMAL](38, 4) 
	,[phurab] [DECIMAL](38, 4) 
	,[phurrf] [NVARCHAR](15) 
	,[phuser] [NVARCHAR](10) 
	,[phpid] [NVARCHAR](10) 
	,[phjobn] [NVARCHAR](10) 
	,[phupmj] [VARCHAR](20) 
	,[phtday] [DECIMAL](38, 4) 
	,[phrsht] [DECIMAL](38, 4) 
	,[phdrqt] [DECIMAL](38, 4) 
	,[phdoc1] [DECIMAL](38, 4) 
	,[phdct4] [NVARCHAR](2) 
	,[phbcrc] [NVARCHAR](3) 
	,[phmkfr] [DECIMAL](38, 4) 
	,[phpohp01] [NVARCHAR](1) 
	,[phpohp02] [NVARCHAR](1) 
	,[phpohp03] [NVARCHAR](1) 
	,[phpohp04] [NVARCHAR](1) 
	,[phpohp05] [NVARCHAR](1) 
	,[phpohp06] [NVARCHAR](1) 
	,[phpohp07] [NVARCHAR](1) 
	,[phpohp08] [NVARCHAR](1) 
	,[phpohp09] [NVARCHAR](1) 
	,[phpohp10] [NVARCHAR](1) 
	,[phpohp11] [NVARCHAR](1) 
	,[phpohp12] [NVARCHAR](1) 
	,[phpohc01] [NVARCHAR](3) 
	,[phpohc02] [NVARCHAR](3) 
	,[phpohc03] [NVARCHAR](3) 
	,[phpohc04] [NVARCHAR](3) 
	,[phpohc05] [NVARCHAR](3) 
	,[phpohc06] [NVARCHAR](3) 
	,[phpohc07] [NVARCHAR](10) 
	,[phpohc08] [NVARCHAR](10) 
	,[phpohc09] [NVARCHAR](10) 
	,[phpohc10] [NVARCHAR](10) 
	,[phpohc11] [NVARCHAR](10) 
	,[phpohc12] [NVARCHAR](10) 
	,[phpohd01] [VARCHAR](20) 
	,[phpohd02] [VARCHAR](20) 
	,[phpohab01] [DECIMAL](38, 4) 
	,[phpohab02] [DECIMAL](38, 4) 
	,[phcukid] [DECIMAL](38, 4) 
	,[phpohp13] [NVARCHAR](30) 
	,[phpohu01] [NVARCHAR](11) 
	,[phpohu02] [NVARCHAR](11) 
	,[phreti] [NVARCHAR](1) 
	,[phclass01] [NVARCHAR](3) 
	,[phclass02] [NVARCHAR](3) 
	,[phclass03] [NVARCHAR](3) 
	,[phclass04] [NVARCHAR](3) 
	,[phclass05] [NVARCHAR](3) 
)
WITH
(
    LOCATION='processing-clean/jde/f4301/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)