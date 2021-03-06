--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f42019_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f42019_init

CREATE EXTERNAL TABLE stg_jde.ext_f42019_init
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
	,[shkcoo] [NVARCHAR](5) 
	,[shdoco] [DECIMAL](38, 4) 
	,[shdcto] [NVARCHAR](2) 
	,[shsfxo] [NVARCHAR](3) 
	,[shmcu] [NVARCHAR](12) 
	,[shco] [NVARCHAR](5) 
	,[shokco] [NVARCHAR](5) 
	,[shoorn] [NVARCHAR](8) 
	,[shocto] [NVARCHAR](2) 
	,[shrkco] [NVARCHAR](5) 
	,[shrorn] [NVARCHAR](8) 
	,[shrcto] [NVARCHAR](2) 
	,[shan8] [DECIMAL](38, 4) 
	,[shshan] [DECIMAL](38, 4) 
	,[shpa8] [DECIMAL](38, 4) 
	,[shdrqj] [VARCHAR](20) 
	,[shtrdj] [VARCHAR](20) 
	,[shpddj] [VARCHAR](20) 
	,[shopdj] [VARCHAR](20) 
	,[shaddj] [VARCHAR](20) 
	,[shcndj] [VARCHAR](20) 
	,[shpefj] [VARCHAR](20) 
	,[shppdj] [VARCHAR](20) 
	,[shvr01] [NVARCHAR](25) 
	,[shvr02] [NVARCHAR](25) 
	,[shdel1] [NVARCHAR](30) 
	,[shdel2] [NVARCHAR](30) 
	,[shinmg] [NVARCHAR](10) 
	,[shptc] [NVARCHAR](3) 
	,[shryin] [NVARCHAR](1) 
	,[shasn] [NVARCHAR](8) 
	,[shprgp] [NVARCHAR](8) 
	,[shtrdc] [DECIMAL](38, 4) 
	,[shpcrt] [DECIMAL](38, 4) 
	,[shtxa1] [NVARCHAR](10) 
	,[shexr1] [NVARCHAR](2) 
	,[shtxct] [NVARCHAR](20) 
	,[shatxt] [NVARCHAR](1) 
	,[shprio] [NVARCHAR](1) 
	,[shback] [NVARCHAR](1) 
	,[shsbal] [NVARCHAR](1) 
	,[shhold] [NVARCHAR](2) 
	,[shplst] [NVARCHAR](1) 
	,[shinvc] [DECIMAL](38, 4) 
	,[shntr] [NVARCHAR](2) 
	,[shanby] [DECIMAL](38, 4) 
	,[shcars] [DECIMAL](38, 4) 
	,[shmot] [NVARCHAR](3) 
	,[shcot] [NVARCHAR](3) 
	,[shrout] [NVARCHAR](3) 
	,[shstop] [NVARCHAR](3) 
	,[shzon] [NVARCHAR](3) 
	,[shcnid] [NVARCHAR](20) 
	,[shfrth] [NVARCHAR](3) 
	,[shaft] [NVARCHAR](1) 
	,[shfuf1] [NVARCHAR](1) 
	,[shfrtc] [NVARCHAR](1) 
	,[shmord] [NVARCHAR](1) 
	,[shrcd] [NVARCHAR](3) 
	,[shfuf2] [NVARCHAR](1) 
	,[shotot] [DECIMAL](38, 4) 
	,[shtotc] [DECIMAL](38, 4) 
	,[shwumd] [NVARCHAR](2) 
	,[shvumd] [NVARCHAR](2) 
	,[shautn] [NVARCHAR](10) 
	,[shcact] [NVARCHAR](25) 
	,[shcexp] [VARCHAR](20) 
	,[shsbli] [NVARCHAR](1) 
	,[shcrmd] [NVARCHAR](1) 
	,[shcrrm] [NVARCHAR](1) 
	,[shcrcd] [NVARCHAR](3) 
	,[shcrr] [DECIMAL](38, 4) 
	,[shlngp] [NVARCHAR](2) 
	,[shfap] [DECIMAL](38, 4) 
	,[shfcst] [DECIMAL](38, 4) 
	,[shorby] [NVARCHAR](10) 
	,[shtkby] [NVARCHAR](10) 
	,[shurcd] [NVARCHAR](2) 
	,[shurdt] [VARCHAR](20) 
	,[shurat] [DECIMAL](38, 4) 
	,[shurab] [DECIMAL](38, 4) 
	,[shurrf] [NVARCHAR](15) 
	,[shuser] [NVARCHAR](10) 
	,[shpid] [NVARCHAR](10) 
	,[shjobn] [NVARCHAR](10) 
	,[shupmj] [VARCHAR](20) 
	,[shtday] [DECIMAL](38, 4) 
	,[shir01] [NVARCHAR](30) 
	,[shir02] [NVARCHAR](30) 
	,[shir03] [NVARCHAR](30) 
	,[shir04] [NVARCHAR](30) 
	,[shir05] [NVARCHAR](30) 
	,[shvr03] [NVARCHAR](25) 
	,[shsoor] [VARCHAR](20) 
	,[shpmdt] [DECIMAL](38, 4) 
	,[shrsdt] [DECIMAL](38, 4) 
	,[shrqsj] [VARCHAR](20) 
	,[shpstm] [DECIMAL](38, 4) 
	,[shpdtt] [DECIMAL](38, 4) 
	,[shoptt] [DECIMAL](38, 4) 
	,[shdrqt] [DECIMAL](38, 4) 
	,[shadtm] [DECIMAL](38, 4) 
	,[shadlj] [VARCHAR](20) 
	,[shpban] [DECIMAL](38, 4) 
	,[shitan] [DECIMAL](38, 4) 
	,[shftan] [DECIMAL](38, 4) 
	,[shdvan] [DECIMAL](38, 4) 
	,[shdoc1] [DECIMAL](38, 4) 
	,[shdct4] [NVARCHAR](2) 
	,[shcord] [DECIMAL](38, 4) 
	,[shbsc] [NVARCHAR](10) 
	,[shbcrc] [NVARCHAR](3) 
	,[shauft] [NVARCHAR](1) 
	,[shaufi] [NVARCHAR](1) 
	,[shopbo] [NVARCHAR](30) 
	,[shoptc] [DECIMAL](38, 4) 
	,[shopld] [VARCHAR](20) 
	,[shopbk] [DECIMAL](38, 4) 
	,[shopsb] [DECIMAL](38, 4) 
	,[shopps] [NVARCHAR](30) 
	,[shoppl] [NVARCHAR](1) 
	,[shopms] [NVARCHAR](1) 
	,[shopss] [NVARCHAR](1) 
	,[shopba] [NVARCHAR](1) 
	,[shopll] [NVARCHAR](1) 
	,[shpran8] [DECIMAL](38, 4) 
	,[shoppid] [DECIMAL](38, 4) 
	,[shsdattn] [NVARCHAR](50) 
	,[shspattn] [NVARCHAR](50) 
	,[shotind] [NVARCHAR](1) 
	,[shprcidln] [DECIMAL](38, 4) 
	,[shccidln] [DECIMAL](38, 4) 
	,[shshccidln] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f42019/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
