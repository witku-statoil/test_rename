--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f42119_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f42119_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f42119_cdc
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
	,[sdkcoo] [NVARCHAR](5) 
	,[sddoco] [DECIMAL](38, 4) 
	,[sddcto] [NVARCHAR](2) 
	,[sdlnid] [DECIMAL](38, 4) 
	,[sdsfxo] [NVARCHAR](3) 
	,[sdmcu] [NVARCHAR](12) 
	,[sdco] [NVARCHAR](5) 
	,[sdokco] [NVARCHAR](5) 
	,[sdoorn] [NVARCHAR](8) 
	,[sdocto] [NVARCHAR](2) 
	,[sdogno] [DECIMAL](38, 4) 
	,[sdrkco] [NVARCHAR](5) 
	,[sdrorn] [NVARCHAR](8) 
	,[sdrcto] [NVARCHAR](2) 
	,[sdrlln] [DECIMAL](38, 4) 
	,[sddmct] [NVARCHAR](12) 
	,[sddmcs] [DECIMAL](38, 4) 
	,[sdan8] [DECIMAL](38, 4) 
	,[sdshan] [DECIMAL](38, 4) 
	,[sdpa8] [DECIMAL](38, 4) 
	,[sddrqj] [VARCHAR](20) 
	,[sdtrdj] [VARCHAR](20) 
	,[sdpddj] [VARCHAR](20) 
	,[sdaddj] [VARCHAR](20) 
	,[sdivd] [VARCHAR](20) 
	,[sdcndj] [VARCHAR](20) 
	,[sddgl] [VARCHAR](20) 
	,[sdrsdj] [VARCHAR](20) 
	,[sdpefj] [VARCHAR](20) 
	,[sdppdj] [VARCHAR](20) 
	,[sdvr01] [NVARCHAR](25) 
	,[sdvr02] [NVARCHAR](25) 
	,[sditm] [DECIMAL](38, 4) 
	,[sdlitm] [NVARCHAR](25) 
	,[sdaitm] [NVARCHAR](25) 
	,[sdlocn] [NVARCHAR](20) 
	,[sdlotn] [NVARCHAR](30) 
	,[sdfrgd] [NVARCHAR](3) 
	,[sdthgd] [NVARCHAR](3) 
	,[sdfrmp] [DECIMAL](38, 4) 
	,[sdthrp] [DECIMAL](38, 4) 
	,[sdexdp] [DECIMAL](38, 4) 
	,[sddsc1] [NVARCHAR](30) 
	,[sddsc2] [NVARCHAR](30) 
	,[sdlnty] [NVARCHAR](2) 
	,[sdnxtr] [NVARCHAR](3) 
	,[sdlttr] [NVARCHAR](3) 
	,[sdemcu] [NVARCHAR](12) 
	,[sdrlit] [NVARCHAR](8) 
	,[sdktln] [DECIMAL](38, 4) 
	,[sdcpnt] [DECIMAL](38, 4) 
	,[sdrkit] [DECIMAL](38, 4) 
	,[sdktp] [DECIMAL](38, 4) 
	,[sdsrp1] [NVARCHAR](3) 
	,[sdsrp2] [NVARCHAR](3) 
	,[sdsrp3] [NVARCHAR](3) 
	,[sdsrp4] [NVARCHAR](3) 
	,[sdsrp5] [NVARCHAR](3) 
	,[sdprp1] [NVARCHAR](3) 
	,[sdprp2] [NVARCHAR](3) 
	,[sdprp3] [NVARCHAR](3) 
	,[sdprp4] [NVARCHAR](3) 
	,[sdprp5] [NVARCHAR](3) 
	,[sduom] [NVARCHAR](2) 
	,[sduorg] [DECIMAL](38, 4) 
	,[sdsoqs] [DECIMAL](38, 4) 
	,[sdsobk] [DECIMAL](38, 4) 
	,[sdsocn] [DECIMAL](38, 4) 
	,[sdsone] [DECIMAL](38, 4) 
	,[sduopn] [DECIMAL](38, 4) 
	,[sdqtyt] [DECIMAL](38, 4) 
	,[sdqrlv] [DECIMAL](38, 4) 
	,[sdcomm] [NVARCHAR](1) 
	,[sdotqy] [NVARCHAR](1) 
	,[sduprc] [DECIMAL](38, 4) 
	,[sdaexp] [DECIMAL](38, 4) 
	,[sdaopn] [DECIMAL](38, 4) 
	,[sdprov] [NVARCHAR](1) 
	,[sdtpc] [NVARCHAR](1) 
	,[sdapum] [NVARCHAR](2) 
	,[sdlprc] [DECIMAL](38, 4) 
	,[sduncs] [DECIMAL](38, 4) 
	,[sdecst] [DECIMAL](38, 4) 
	,[sdcsto] [NVARCHAR](1) 
	,[sdtcst] [DECIMAL](38, 4) 
	,[sdinmg] [NVARCHAR](10) 
	,[sdptc] [NVARCHAR](3) 
	,[sdryin] [NVARCHAR](1) 
	,[sddtbs] [NVARCHAR](1) 
	,[sdtrdc] [DECIMAL](38, 4) 
	,[sdfun2] [DECIMAL](38, 4) 
	,[sdasn] [NVARCHAR](8) 
	,[sdprgr] [NVARCHAR](8) 
	,[sdclvl] [NVARCHAR](3) 
	,[sdcadc] [DECIMAL](38, 4) 
	,[sdkco] [NVARCHAR](5) 
	,[sddoc] [DECIMAL](38, 4) 
	,[sddct] [NVARCHAR](2) 
	,[sdodoc] [DECIMAL](38, 4) 
	,[sdodct] [NVARCHAR](2) 
	,[sdokc] [NVARCHAR](5) 
	,[sdpsn] [DECIMAL](38, 4) 
	,[sddeln] [DECIMAL](38, 4) 
	,[sdtax1] [NVARCHAR](1) 
	,[sdtxa1] [NVARCHAR](10) 
	,[sdexr1] [NVARCHAR](2) 
	,[sdatxt] [NVARCHAR](1) 
	,[sdprio] [NVARCHAR](1) 
	,[sdresl] [NVARCHAR](1) 
	,[sdback] [NVARCHAR](1) 
	,[sdsbal] [NVARCHAR](1) 
	,[sdapts] [NVARCHAR](1) 
	,[sdlob] [NVARCHAR](3) 
	,[sdeuse] [NVARCHAR](3) 
	,[sddtys] [NVARCHAR](2) 
	,[sdntr] [NVARCHAR](2) 
	,[sdvend] [DECIMAL](38, 4) 
	,[sdcars] [DECIMAL](38, 4) 
	,[sdmot] [NVARCHAR](3) 
	,[sdrout] [NVARCHAR](3) 
	,[sdstop] [NVARCHAR](3) 
	,[sdzon] [NVARCHAR](3) 
	,[sdcnid] [NVARCHAR](20) 
	,[sdfrth] [NVARCHAR](3) 
	,[sdshcm] [NVARCHAR](3) 
	,[sdshcn] [NVARCHAR](3) 
	,[sdsern] [NVARCHAR](30) 
	,[sduom1] [NVARCHAR](2) 
	,[sdpqor] [DECIMAL](38, 4) 
	,[sduom2] [NVARCHAR](2) 
	,[sdsqor] [DECIMAL](38, 4) 
	,[sduom4] [NVARCHAR](2) 
	,[sditwt] [DECIMAL](38, 4) 
	,[sdwtum] [NVARCHAR](2) 
	,[sditvl] [DECIMAL](38, 4) 
	,[sdvlum] [NVARCHAR](2) 
	,[sdrprc] [NVARCHAR](8) 
	,[sdorpr] [NVARCHAR](8) 
	,[sdorp] [NVARCHAR](1) 
	,[sdcmgp] [NVARCHAR](2) 
	,[sdglc] [NVARCHAR](4) 
	,[sdctry] [DECIMAL](38, 4) 
	,[sdfy] [DECIMAL](38, 4) 
	,[sdso01] [NVARCHAR](1) 
	,[sdso02] [NVARCHAR](1) 
	,[sdso03] [NVARCHAR](1) 
	,[sdso04] [NVARCHAR](1) 
	,[sdso05] [NVARCHAR](1) 
	,[sdso06] [NVARCHAR](1) 
	,[sdso07] [NVARCHAR](1) 
	,[sdso08] [NVARCHAR](1) 
	,[sdso09] [NVARCHAR](1) 
	,[sdso10] [NVARCHAR](1) 
	,[sdso11] [NVARCHAR](1) 
	,[sdso12] [NVARCHAR](1) 
	,[sdso13] [NVARCHAR](1) 
	,[sdso14] [NVARCHAR](1) 
	,[sdso15] [NVARCHAR](1) 
	,[sdacom] [NVARCHAR](1) 
	,[sdcmcg] [NVARCHAR](8) 
	,[sdrcd] [NVARCHAR](3) 
	,[sdgrwt] [DECIMAL](38, 4) 
	,[sdgwum] [NVARCHAR](2) 
	,[sdsbl] [NVARCHAR](8) 
	,[sdsblt] [NVARCHAR](1) 
	,[sdlcod] [NVARCHAR](2) 
	,[sdupc1] [NVARCHAR](2) 
	,[sdupc2] [NVARCHAR](2) 
	,[sdupc3] [NVARCHAR](2) 
	,[sdswms] [NVARCHAR](1) 
	,[sduncd] [NVARCHAR](1) 
	,[sdcrmd] [NVARCHAR](1) 
	,[sdcrcd] [NVARCHAR](3) 
	,[sdcrr] [DECIMAL](38, 4) 
	,[sdfprc] [DECIMAL](38, 4) 
	,[sdfup] [DECIMAL](38, 4) 
	,[sdfea] [DECIMAL](38, 4) 
	,[sdfuc] [DECIMAL](38, 4) 
	,[sdfec] [DECIMAL](38, 4) 
	,[sdurcd] [NVARCHAR](2) 
	,[sdurdt] [VARCHAR](20) 
	,[sdurat] [DECIMAL](38, 4) 
	,[sdurab] [DECIMAL](38, 4) 
	,[sdurrf] [NVARCHAR](15) 
	,[sdtorg] [NVARCHAR](10) 
	,[sduser] [NVARCHAR](10) 
	,[sdpid] [NVARCHAR](10) 
	,[sdjobn] [NVARCHAR](10) 
	,[sdupmj] [VARCHAR](20) 
	,[sdtday] [DECIMAL](38, 4) 
	,[sdso16] [NVARCHAR](1) 
	,[sdso17] [NVARCHAR](1) 
	,[sdso18] [NVARCHAR](1) 
	,[sdso19] [NVARCHAR](1) 
	,[sdso20] [NVARCHAR](1) 
	,[sdir01] [NVARCHAR](30) 
	,[sdir02] [NVARCHAR](30) 
	,[sdir03] [NVARCHAR](30) 
	,[sdir04] [NVARCHAR](30) 
	,[sdir05] [NVARCHAR](30) 
	,[sdsoor] [VARCHAR](20) 
	,[sdvr03] [NVARCHAR](25) 
	,[sddeid] [DECIMAL](38, 4) 
	,[sdpsig] [NVARCHAR](30) 
	,[sdrlnu] [NVARCHAR](10) 
	,[sdpmdt] [DECIMAL](38, 4) 
	,[sdrltm] [DECIMAL](38, 4) 
	,[sdrldj] [VARCHAR](20) 
	,[sddrqt] [DECIMAL](38, 4) 
	,[sdadtm] [DECIMAL](38, 4) 
	,[sdoptt] [DECIMAL](38, 4) 
	,[sdpdtt] [DECIMAL](38, 4) 
	,[sdpstm] [DECIMAL](38, 4) 
	,[sdxdck] [NVARCHAR](1) 
	,[sdxpty] [DECIMAL](38, 4) 
	,[sddual] [NVARCHAR](1) 
	,[sdbsc] [NVARCHAR](10) 
	,[sdcbsc] [NVARCHAR](10) 
	,[sdcord] [DECIMAL](38, 4) 
	,[sddvan] [DECIMAL](38, 4) 
	,[sdpend] [NVARCHAR](1) 
	,[sdrfrv] [NVARCHAR](3) 
	,[sdmcln] [DECIMAL](38, 4) 
	,[sdshpn] [DECIMAL](38, 4) 
	,[sdrsdt] [DECIMAL](38, 4) 
	,[sdprjm] [DECIMAL](38, 4) 
	,[sdoseq] [DECIMAL](38, 4) 
	,[sdmerl] [NVARCHAR](3) 
	,[sdhold] [NVARCHAR](2) 
	,[sdhdbu] [NVARCHAR](12) 
	,[sddmbu] [NVARCHAR](12) 
	,[sdbcrc] [NVARCHAR](3) 
	,[sdodln] [DECIMAL](38, 4) 
	,[sdopdj] [VARCHAR](20) 
	,[sdxkco] [NVARCHAR](5) 
	,[sdxorn] [DECIMAL](38, 4) 
	,[sdxcto] [NVARCHAR](2) 
	,[sdxlln] [DECIMAL](38, 4) 
	,[sdxsfx] [NVARCHAR](3) 
	,[sdpoe] [NVARCHAR](6) 
	,[sdpmto] [NVARCHAR](1) 
	,[sdanby] [DECIMAL](38, 4) 
	,[sdpmtn] [NVARCHAR](12) 
	,[sdnumb] [DECIMAL](38, 4) 
	,[sdaaid] [DECIMAL](38, 4) 
	,[sdspattn] [NVARCHAR](50) 
	,[sdpran8] [DECIMAL](38, 4) 
	,[sdprcidln] [DECIMAL](38, 4) 
	,[sdccidln] [DECIMAL](38, 4) 
	,[sdshccidln] [DECIMAL](38, 4) 
	,[sdoppid] [DECIMAL](38, 4) 
	,[sdostp] [NVARCHAR](3) 
	,[sdukid] [DECIMAL](38, 4) 
	,[sdcatnm] [NVARCHAR](30) 
	,[sdalloc] [NVARCHAR](1) 
	,[sdfulpid] [VARCHAR](20) 
	,[sdallsts] [NVARCHAR](30) 
	,[sdoscore] [DECIMAL](38, 4) 
	,[sdoscoreo] [NVARCHAR](1) 
	,[sdcmco] [NVARCHAR](5) 
	,[sdkitid] [DECIMAL](38, 4) 
	,[sdkitamtdom] [DECIMAL](38, 4) 
	,[sdkitamtfor] [DECIMAL](38, 4) 
	,[sdkitdirty] [NVARCHAR](1) 
	,[sdocitt] [NVARCHAR](1) 
	,[sdoccardno] [DECIMAL](38, 4) 
	,[sdpmpn] [NVARCHAR](30) 
	,[sdpns] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f42119/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
