--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f060116_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f060116_init

CREATE EXTERNAL TABLE stg_jde.ext_f060116_init
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
	,[yaan8] [DECIMAL](38, 4) 
	,[yaalph] [NVARCHAR](40) 
	,[yassn] [NVARCHAR](20) 
	,[yaoemp] [NVARCHAR](8) 
	,[yasex] [NVARCHAR](1) 
	,[yamstx] [NVARCHAR](1) 
	,[yamsti] [NVARCHAR](1) 
	,[yawsps] [NVARCHAR](1) 
	,[yaeic] [NVARCHAR](1) 
	,[yandep] [DECIMAL](38, 4) 
	,[yaest] [NVARCHAR](1) 
	,[yaecnt] [NVARCHAR](1) 
	,[yatarr] [NVARCHAR](10) 
	,[yatara] [NVARCHAR](10) 
	,[yascdc] [DECIMAL](38, 4) 
	,[yahmst] [NVARCHAR](3) 
	,[yawkse] [NVARCHAR](3) 
	,[yahmlc] [NVARCHAR](3) 
	,[yalwk1] [NVARCHAR](3) 
	,[yalwk2] [NVARCHAR](3) 
	,[yahmco] [NVARCHAR](5) 
	,[yamcu] [NVARCHAR](12) 
	,[yahmcu] [NVARCHAR](12) 
	,[yasg] [NVARCHAR](12) 
	,[yamail] [NVARCHAR](10) 
	,[yapast] [NVARCHAR](1) 
	,[yapfrq] [NVARCHAR](1) 
	,[yasaly] [NVARCHAR](1) 
	,[yapycb] [DECIMAL](38, 4) 
	,[yabcb] [DECIMAL](38, 4) 
	,[yaun] [NVARCHAR](6) 
	,[yajbcd] [NVARCHAR](6) 
	,[yajbst] [NVARCHAR](4) 
	,[yaeeoj] [NVARCHAR](3) 
	,[yaeeom] [NVARCHAR](2) 
	,[yatrs] [NVARCHAR](3) 
	,[yawcmp] [NVARCHAR](4) 
	,[yaflsa] [NVARCHAR](1) 
	,[yaws] [NVARCHAR](1) 
	,[yashft] [NVARCHAR](1) 
	,[yalmth] [NVARCHAR](1) 
	,[yapbrt] [DECIMAL](38, 4) 
	,[yalf] [DECIMAL](38, 4) 
	,[yasal] [DECIMAL](38, 4) 
	,[yaphrt] [DECIMAL](38, 4) 
	,[yapprt] [DECIMAL](38, 4) 
	,[yapwrn] [DECIMAL](38, 4) 
	,[yahrtn] [DECIMAL](38, 4) 
	,[yabrtn] [DECIMAL](38, 4) 
	,[yasaln] [DECIMAL](38, 4) 
	,[yastdh] [DECIMAL](38, 4) 
	,[yastdd] [DECIMAL](38, 4) 
	,[yasdyy] [DECIMAL](38, 4) 
	,[yausr] [NVARCHAR](18) 
	,[yauflg] [NVARCHAR](1) 
	,[yans] [NVARCHAR](1) 
	,[yaifn] [NVARCHAR](1) 
	,[yaimn] [NVARCHAR](1) 
	,[yadob] [VARCHAR](20) 
	,[yadsi] [VARCHAR](20) 
	,[yadt] [VARCHAR](20) 
	,[yadst] [VARCHAR](20) 
	,[yapsdt] [VARCHAR](20) 
	,[yaptdt] [VARCHAR](20) 
	,[yanrdt] [VARCHAR](20) 
	,[yanbdt] [VARCHAR](20) 
	,[yanpdt] [VARCHAR](20) 
	,[yalcdt] [VARCHAR](20) 
	,[yadr] [VARCHAR](20) 
	,[yactdt] [VARCHAR](20) 
	,[yaladt] [VARCHAR](20) 
	,[yabsdt] [VARCHAR](20) 
	,[yacpdt] [VARCHAR](20) 
	,[yaficm] [NVARCHAR](1) 
	,[yap001] [NVARCHAR](3) 
	,[yap002] [NVARCHAR](3) 
	,[yap003] [NVARCHAR](3) 
	,[yap004] [NVARCHAR](3) 
	,[yap005] [NVARCHAR](3) 
	,[yap006] [NVARCHAR](3) 
	,[yap007] [NVARCHAR](3) 
	,[yap008] [NVARCHAR](3) 
	,[yap009] [NVARCHAR](3) 
	,[yap010] [NVARCHAR](3) 
	,[yap011] [NVARCHAR](3) 
	,[yap012] [NVARCHAR](3) 
	,[yap013] [NVARCHAR](3) 
	,[yap014] [NVARCHAR](3) 
	,[yap015] [NVARCHAR](3) 
	,[yap016] [NVARCHAR](3) 
	,[yap017] [NVARCHAR](3) 
	,[yap018] [NVARCHAR](3) 
	,[yap019] [NVARCHAR](3) 
	,[yap020] [NVARCHAR](3) 
	,[yae001] [NVARCHAR](1) 
	,[yae002] [NVARCHAR](1) 
	,[yae003] [NVARCHAR](1) 
	,[yae004] [NVARCHAR](1) 
	,[yae005] [NVARCHAR](1) 
	,[yae006] [NVARCHAR](1) 
	,[yae007] [NVARCHAR](1) 
	,[yae008] [NVARCHAR](1) 
	,[yae009] [NVARCHAR](1) 
	,[yae010] [NVARCHAR](1) 
	,[yaicc] [NVARCHAR](1) 
	,[yanmax] [DECIMAL](38, 4) 
	,[yasocc] [NVARCHAR](4) 
	,[yaiusr] [NVARCHAR](10) 
	,[yaitrm] [NVARCHAR](10) 
	,[yainbt] [NVARCHAR](1) 
	,[yaregn] [NVARCHAR](3) 
	,[yapstp] [NVARCHAR](4) 
	,[yaih] [DECIMAL](38, 4) 
	,[yaccpr] [NVARCHAR](3) 
	,[yaadpn] [NVARCHAR](1) 
	,[yatcnf] [NVARCHAR](1) 
	,[yaraf] [NVARCHAR](1) 
	,[yatipe] [NVARCHAR](1) 
	,[yarccd] [NVARCHAR](1) 
	,[yabdrt] [DECIMAL](38, 4) 
	,[yaborn] [DECIMAL](38, 4) 
	,[yanbdr] [VARCHAR](20) 
	,[yawet] [NVARCHAR](1) 
	,[yaaflg] [NVARCHAR](1) 
	,[yarmst] [NVARCHAR](1) 
	,[yalmst] [NVARCHAR](1) 
	,[yasmkr] [NVARCHAR](2) 
	,[yacbac] [DECIMAL](38, 4) 
	,[yacbaf] [NVARCHAR](1) 
	,[yapccd] [NVARCHAR](5) 
	,[yarcdt] [VARCHAR](20) 
	,[yalsdt] [VARCHAR](20) 
	,[yapadt] [VARCHAR](20) 
	,[yapymh] [NVARCHAR](1) 
	,[yauyst] [NVARCHAR](1) 
	,[yadtsp] [DECIMAL](38, 4) 
	,[yaann8] [DECIMAL](38, 4) 
	,[yappnb] [NVARCHAR](3) 
	,[yapydt] [VARCHAR](20) 
	,[yacben] [NVARCHAR](1) 
	,[yadobm] [DECIMAL](38, 4) 
	,[yadstm] [DECIMAL](38, 4) 
	,[yapsal] [DECIMAL](38, 4) 
	,[yahipn] [NVARCHAR](11) 
	,[yacm] [NVARCHAR](2) 
	,[yalsal] [DECIMAL](38, 4) 
	,[yadivc] [NVARCHAR](6) 
	,[yavshf] [NVARCHAR](1) 
	,[yapyrv] [DECIMAL](38, 4) 
	,[yaanpa] [DECIMAL](38, 4) 
	,[yapgrd] [NVARCHAR](6) 
	,[yapgrs] [NVARCHAR](4) 
	,[yasloc] [NVARCHAR](8) 
	,[yanrvw] [VARCHAR](20) 
	,[yatinc] [NVARCHAR](1) 
	,[yahm01] [NVARCHAR](1) 
	,[yahm02] [NVARCHAR](1) 
	,[yahm03] [NVARCHAR](1) 
	,[yahm04] [NVARCHAR](1) 
	,[yapos] [NVARCHAR](8) 
	,[yaed01] [VARCHAR](20) 
	,[yaed02] [VARCHAR](20) 
	,[yaed03] [VARCHAR](20) 
	,[yaed04] [VARCHAR](20) 
	,[yaed05] [VARCHAR](20) 
	,[yaed06] [VARCHAR](20) 
	,[yaed07] [VARCHAR](20) 
	,[yaed08] [VARCHAR](20) 
	,[yaed09] [VARCHAR](20) 
	,[yaed10] [VARCHAR](20) 
	,[yaed11] [VARCHAR](20) 
	,[yaed12] [VARCHAR](20) 
	,[yaed13] [VARCHAR](20) 
	,[yaed14] [VARCHAR](20) 
	,[yaed15] [VARCHAR](20) 
	,[yaed16] [VARCHAR](20) 
	,[yaed17] [VARCHAR](20) 
	,[yaed18] [VARCHAR](20) 
	,[yaed19] [VARCHAR](20) 
	,[yaed20] [VARCHAR](20) 
	,[yadept] [NVARCHAR](12) 
	,[yafage] [DECIMAL](38, 4) 
	,[yafsal] [DECIMAL](38, 4) 
	,[yaadsd] [VARCHAR](20) 
	,[yacmpa] [DECIMAL](38, 4) 
	,[yaepnt] [DECIMAL](38, 4) 
	,[yajobn] [NVARCHAR](10) 
	,[yauser] [NVARCHAR](10) 
	,[yapid] [NVARCHAR](10) 
	,[yaupmj] [VARCHAR](20) 
	,[yak001] [NVARCHAR](1) 
	,[yak002] [NVARCHAR](1) 
	,[yak003] [NVARCHAR](1) 
	,[yak004] [NVARCHAR](1) 
	,[yak005] [NVARCHAR](1) 
	,[yak006] [NVARCHAR](1) 
	,[yak007] [NVARCHAR](1) 
	,[yak008] [NVARCHAR](1) 
	,[yak009] [NVARCHAR](1) 
	,[yak010] [NVARCHAR](1) 
	,[yaatpy] [DECIMAL](38, 4) 
	,[yapens] [NVARCHAR](1) 
	,[yaorg] [NVARCHAR](1) 
	,[yabens] [NVARCHAR](1) 
	,[yafte] [DECIMAL](38, 4) 
	,[yaaaf] [NVARCHAR](1) 
	,[yasui] [NVARCHAR](1) 
	,[yadtsf] [VARCHAR](20) 
	,[yasmoy] [DECIMAL](38, 4) 
	,[yaldid] [NVARCHAR](5) 
	,[yattar] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f060116/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
