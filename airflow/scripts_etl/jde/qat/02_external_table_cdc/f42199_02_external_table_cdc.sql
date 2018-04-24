--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f42199_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f42199_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f42199_cdc
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
	,[slkcoo] [NVARCHAR](5) 
	,[sldoco] [DECIMAL](38, 4) 
	,[sldcto] [NVARCHAR](2) 
	,[sllnid] [DECIMAL](38, 4) 
	,[slsfxo] [NVARCHAR](3) 
	,[slmcu] [NVARCHAR](12) 
	,[slco] [NVARCHAR](5) 
	,[slokco] [NVARCHAR](5) 
	,[sloorn] [NVARCHAR](8) 
	,[slocto] [NVARCHAR](2) 
	,[slogno] [DECIMAL](38, 4) 
	,[slrkco] [NVARCHAR](5) 
	,[slrorn] [NVARCHAR](8) 
	,[slrcto] [NVARCHAR](2) 
	,[slrlln] [DECIMAL](38, 4) 
	,[sldmct] [NVARCHAR](12) 
	,[sldmcs] [DECIMAL](38, 4) 
	,[slan8] [DECIMAL](38, 4) 
	,[slshan] [DECIMAL](38, 4) 
	,[slpa8] [DECIMAL](38, 4) 
	,[sldrqj] [VARCHAR](20) 
	,[sltrdj] [VARCHAR](20) 
	,[slpddj] [VARCHAR](20) 
	,[sladdj] [VARCHAR](20) 
	,[slivd] [VARCHAR](20) 
	,[slcndj] [VARCHAR](20) 
	,[sldgl] [VARCHAR](20) 
	,[slrsdj] [VARCHAR](20) 
	,[slpefj] [VARCHAR](20) 
	,[slppdj] [VARCHAR](20) 
	,[slvr01] [NVARCHAR](25) 
	,[slvr02] [NVARCHAR](25) 
	,[slitm] [DECIMAL](38, 4) 
	,[sllitm] [NVARCHAR](25) 
	,[slaitm] [NVARCHAR](25) 
	,[sllocn] [NVARCHAR](20) 
	,[sllotn] [NVARCHAR](30) 
	,[slfrgd] [NVARCHAR](3) 
	,[slthgd] [NVARCHAR](3) 
	,[slfrmp] [DECIMAL](38, 4) 
	,[slthrp] [DECIMAL](38, 4) 
	,[slexdp] [DECIMAL](38, 4) 
	,[sldsc1] [NVARCHAR](30) 
	,[sldsc2] [NVARCHAR](30) 
	,[sllnty] [NVARCHAR](2) 
	,[slnxtr] [NVARCHAR](3) 
	,[sllttr] [NVARCHAR](3) 
	,[slemcu] [NVARCHAR](12) 
	,[slrlit] [NVARCHAR](8) 
	,[slktln] [DECIMAL](38, 4) 
	,[slcpnt] [DECIMAL](38, 4) 
	,[slrkit] [DECIMAL](38, 4) 
	,[slktp] [DECIMAL](38, 4) 
	,[slsrp1] [NVARCHAR](3) 
	,[slsrp2] [NVARCHAR](3) 
	,[slsrp3] [NVARCHAR](3) 
	,[slsrp4] [NVARCHAR](3) 
	,[slsrp5] [NVARCHAR](3) 
	,[slprp1] [NVARCHAR](3) 
	,[slprp2] [NVARCHAR](3) 
	,[slprp3] [NVARCHAR](3) 
	,[slprp4] [NVARCHAR](3) 
	,[slprp5] [NVARCHAR](3) 
	,[sluom] [NVARCHAR](2) 
	,[sluorg] [DECIMAL](38, 4) 
	,[slsoqs] [DECIMAL](38, 4) 
	,[slsobk] [DECIMAL](38, 4) 
	,[slsocn] [DECIMAL](38, 4) 
	,[slsone] [DECIMAL](38, 4) 
	,[sluopn] [DECIMAL](38, 4) 
	,[slqtyt] [DECIMAL](38, 4) 
	,[slqrlv] [DECIMAL](38, 4) 
	,[slcomm] [NVARCHAR](1) 
	,[slotqy] [NVARCHAR](1) 
	,[sluprc] [DECIMAL](38, 4) 
	,[slaexp] [DECIMAL](38, 4) 
	,[slaopn] [DECIMAL](38, 4) 
	,[slprov] [NVARCHAR](1) 
	,[sltpc] [NVARCHAR](1) 
	,[slapum] [NVARCHAR](2) 
	,[sllprc] [DECIMAL](38, 4) 
	,[sluncs] [DECIMAL](38, 4) 
	,[slecst] [DECIMAL](38, 4) 
	,[slcsto] [NVARCHAR](1) 
	,[sltcst] [DECIMAL](38, 4) 
	,[slinmg] [NVARCHAR](10) 
	,[slptc] [NVARCHAR](3) 
	,[slryin] [NVARCHAR](1) 
	,[sldtbs] [NVARCHAR](1) 
	,[sltrdc] [DECIMAL](38, 4) 
	,[slfun2] [DECIMAL](38, 4) 
	,[slasn] [NVARCHAR](8) 
	,[slprgr] [NVARCHAR](8) 
	,[slclvl] [NVARCHAR](3) 
	,[slcadc] [DECIMAL](38, 4) 
	,[slkco] [NVARCHAR](5) 
	,[sldoc] [DECIMAL](38, 4) 
	,[sldct] [NVARCHAR](2) 
	,[slodoc] [DECIMAL](38, 4) 
	,[slodct] [NVARCHAR](2) 
	,[slokc] [NVARCHAR](5) 
	,[slpsn] [DECIMAL](38, 4) 
	,[sldeln] [DECIMAL](38, 4) 
	,[sltax1] [NVARCHAR](1) 
	,[sltxa1] [NVARCHAR](10) 
	,[slexr1] [NVARCHAR](2) 
	,[slatxt] [NVARCHAR](1) 
	,[slprio] [NVARCHAR](1) 
	,[slresl] [NVARCHAR](1) 
	,[slback] [NVARCHAR](1) 
	,[slsbal] [NVARCHAR](1) 
	,[slapts] [NVARCHAR](1) 
	,[sllob] [NVARCHAR](3) 
	,[sleuse] [NVARCHAR](3) 
	,[sldtys] [NVARCHAR](2) 
	,[slntr] [NVARCHAR](2) 
	,[slvend] [DECIMAL](38, 4) 
	,[slcars] [DECIMAL](38, 4) 
	,[slmot] [NVARCHAR](3) 
	,[slrout] [NVARCHAR](3) 
	,[slstop] [NVARCHAR](3) 
	,[slzon] [NVARCHAR](3) 
	,[slcnid] [NVARCHAR](20) 
	,[slfrth] [NVARCHAR](3) 
	,[slshcm] [NVARCHAR](3) 
	,[slshcn] [NVARCHAR](3) 
	,[slsern] [NVARCHAR](30) 
	,[sluom1] [NVARCHAR](2) 
	,[slpqor] [DECIMAL](38, 4) 
	,[sluom2] [NVARCHAR](2) 
	,[slsqor] [DECIMAL](38, 4) 
	,[sluom4] [NVARCHAR](2) 
	,[slitwt] [DECIMAL](38, 4) 
	,[slwtum] [NVARCHAR](2) 
	,[slitvl] [DECIMAL](38, 4) 
	,[slvlum] [NVARCHAR](2) 
	,[slrprc] [NVARCHAR](8) 
	,[slorpr] [NVARCHAR](8) 
	,[slorp] [NVARCHAR](1) 
	,[slcmgp] [NVARCHAR](2) 
	,[slglc] [NVARCHAR](4) 
	,[slctry] [DECIMAL](38, 4) 
	,[slfy] [DECIMAL](38, 4) 
	,[slso01] [NVARCHAR](1) 
	,[slso02] [NVARCHAR](1) 
	,[slso03] [NVARCHAR](1) 
	,[slso04] [NVARCHAR](1) 
	,[slso05] [NVARCHAR](1) 
	,[slso06] [NVARCHAR](1) 
	,[slso07] [NVARCHAR](1) 
	,[slso08] [NVARCHAR](1) 
	,[slso09] [NVARCHAR](1) 
	,[slso10] [NVARCHAR](1) 
	,[slso11] [NVARCHAR](1) 
	,[slso12] [NVARCHAR](1) 
	,[slso13] [NVARCHAR](1) 
	,[slso14] [NVARCHAR](1) 
	,[slso15] [NVARCHAR](1) 
	,[slacom] [NVARCHAR](1) 
	,[slcmcg] [NVARCHAR](8) 
	,[slrcd] [NVARCHAR](3) 
	,[slgrwt] [DECIMAL](38, 4) 
	,[slgwum] [NVARCHAR](2) 
	,[slsbl] [NVARCHAR](8) 
	,[slsblt] [NVARCHAR](1) 
	,[sllcod] [NVARCHAR](2) 
	,[slupc1] [NVARCHAR](2) 
	,[slupc2] [NVARCHAR](2) 
	,[slupc3] [NVARCHAR](2) 
	,[slswms] [NVARCHAR](1) 
	,[sluncd] [NVARCHAR](1) 
	,[slcrmd] [NVARCHAR](1) 
	,[slcrcd] [NVARCHAR](3) 
	,[slcrr] [DECIMAL](38, 4) 
	,[slfprc] [DECIMAL](38, 4) 
	,[slfup] [DECIMAL](38, 4) 
	,[slfea] [DECIMAL](38, 4) 
	,[slfuc] [DECIMAL](38, 4) 
	,[slfec] [DECIMAL](38, 4) 
	,[slurcd] [NVARCHAR](2) 
	,[slurdt] [VARCHAR](20) 
	,[slurat] [DECIMAL](38, 4) 
	,[slurab] [DECIMAL](38, 4) 
	,[slurrf] [NVARCHAR](15) 
	,[sltorg] [NVARCHAR](10) 
	,[sluser] [NVARCHAR](10) 
	,[slpid] [NVARCHAR](10) 
	,[sljobn] [NVARCHAR](10) 
	,[slupmj] [VARCHAR](20) 
	,[sltday] [DECIMAL](38, 4) 
	,[slso16] [NVARCHAR](1) 
	,[slso17] [NVARCHAR](1) 
	,[slso18] [NVARCHAR](1) 
	,[slso19] [NVARCHAR](1) 
	,[slso20] [NVARCHAR](1) 
	,[slir01] [NVARCHAR](30) 
	,[slir02] [NVARCHAR](30) 
	,[slir03] [NVARCHAR](30) 
	,[slir04] [NVARCHAR](30) 
	,[slir05] [NVARCHAR](30) 
	,[slsoor] [VARCHAR](20) 
	,[slvr03] [NVARCHAR](25) 
	,[sldeid] [DECIMAL](38, 4) 
	,[slpsig] [NVARCHAR](30) 
	,[slrlnu] [NVARCHAR](10) 
	,[slpmdt] [DECIMAL](38, 4) 
	,[slrltm] [DECIMAL](38, 4) 
	,[slrldj] [VARCHAR](20) 
	,[sldrqt] [DECIMAL](38, 4) 
	,[sladtm] [DECIMAL](38, 4) 
	,[sloptt] [DECIMAL](38, 4) 
	,[slpdtt] [DECIMAL](38, 4) 
	,[slpstm] [DECIMAL](38, 4) 
	,[slxdck] [NVARCHAR](1) 
	,[slxpty] [DECIMAL](38, 4) 
	,[sldual] [NVARCHAR](1) 
	,[slbsc] [NVARCHAR](10) 
	,[slcbsc] [NVARCHAR](10) 
	,[slcord] [DECIMAL](38, 4) 
	,[sldvan] [DECIMAL](38, 4) 
	,[slpend] [NVARCHAR](1) 
	,[slrfrv] [NVARCHAR](3) 
	,[slmcln] [DECIMAL](38, 4) 
	,[slshpn] [DECIMAL](38, 4) 
	,[slrsdt] [DECIMAL](38, 4) 
	,[slprjm] [DECIMAL](38, 4) 
	,[sloseq] [DECIMAL](38, 4) 
	,[slmerl] [NVARCHAR](3) 
	,[slhold] [NVARCHAR](2) 
	,[slhdbu] [NVARCHAR](12) 
	,[sldmbu] [NVARCHAR](12) 
	,[slbcrc] [NVARCHAR](3) 
	,[slodln] [DECIMAL](38, 4) 
	,[slopdj] [VARCHAR](20) 
	,[slxkco] [NVARCHAR](5) 
	,[slxorn] [DECIMAL](38, 4) 
	,[slxcto] [NVARCHAR](2) 
	,[slxlln] [DECIMAL](38, 4) 
	,[slxsfx] [NVARCHAR](3) 
	,[slpoe] [NVARCHAR](6) 
	,[slpmto] [NVARCHAR](1) 
	,[slanby] [DECIMAL](38, 4) 
	,[slpmtn] [NVARCHAR](12) 
	,[slnumb] [DECIMAL](38, 4) 
	,[slaaid] [DECIMAL](38, 4) 
	,[slpran8] [DECIMAL](38, 4) 
	,[slspattn] [NVARCHAR](50) 
	,[slprcidln] [DECIMAL](38, 4) 
	,[slccidln] [DECIMAL](38, 4) 
	,[slshccidln] [DECIMAL](38, 4) 
	,[sloppid] [DECIMAL](38, 4) 
	,[slostp] [NVARCHAR](3) 
	,[slukid] [DECIMAL](38, 4) 
	,[slcatnm] [NVARCHAR](30) 
	,[slalloc] [NVARCHAR](1) 
	,[slfulpid] [VARCHAR](20) 
	,[slallsts] [NVARCHAR](30) 
	,[sloscore] [DECIMAL](38, 4) 
	,[sloscoreo] [NVARCHAR](1) 
	,[slcmco] [NVARCHAR](5) 
	,[slkitid] [DECIMAL](38, 4) 
	,[slkitamtdom] [DECIMAL](38, 4) 
	,[slkitamtfor] [DECIMAL](38, 4) 
	,[slkitdirty] [NVARCHAR](1) 
	,[slocitt] [NVARCHAR](1) 
	,[sloccardno] [DECIMAL](38, 4) 
	,[slpmpn] [NVARCHAR](30) 
	,[slpns] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f42199/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
