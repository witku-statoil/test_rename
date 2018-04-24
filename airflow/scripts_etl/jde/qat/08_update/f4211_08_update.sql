-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f4211_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[sdkcoo] = cdc.[sdkcoo]
	,[sdkcoo_conv] = cdc.[sdkcoo_conv]
	,[sddoco] = cdc.[sddoco]
	,[sddcto] = cdc.[sddcto]
	,[sdlnid] = cdc.[sdlnid]
	,[sdlnid_conv] = cdc.[sdlnid_conv]
	,[sdsfxo] = cdc.[sdsfxo]
	,[sdsfxo_conv] = cdc.[sdsfxo_conv]
	,[sdmcu] = cdc.[sdmcu]
	,[sdmcu_conv] = cdc.[sdmcu_conv]
	,[sdco] = cdc.[sdco]
	,[sdco_conv] = cdc.[sdco_conv]
	,[sdokco] = cdc.[sdokco]
	,[sdokco_conv] = cdc.[sdokco_conv]
	,[sdoorn] = cdc.[sdoorn]
	,[sdoorn_conv] = cdc.[sdoorn_conv]
	,[sdocto] = cdc.[sdocto]
	,[sdogno] = cdc.[sdogno]
	,[sdogno_conv] = cdc.[sdogno_conv]
	,[sdrkco] = cdc.[sdrkco]
	,[sdrkco_conv] = cdc.[sdrkco_conv]
	,[sdrorn] = cdc.[sdrorn]
	,[sdrorn_conv] = cdc.[sdrorn_conv]
	,[sdrcto] = cdc.[sdrcto]
	,[sdrlln] = cdc.[sdrlln]
	,[sdrlln_conv] = cdc.[sdrlln_conv]
	,[sddmct] = cdc.[sddmct]
	,[sddmct_conv] = cdc.[sddmct_conv]
	,[sddmcs] = cdc.[sddmcs]
	,[sdan8] = cdc.[sdan8]
	,[sdshan] = cdc.[sdshan]
	,[sdpa8] = cdc.[sdpa8]
	,[sddrqj] = cdc.[sddrqj]
	,[sddrqj_conv] = cdc.[sddrqj_conv]
	,[sdtrdj] = cdc.[sdtrdj]
	,[sdtrdj_conv] = cdc.[sdtrdj_conv]
	,[sdpddj] = cdc.[sdpddj]
	,[sdpddj_conv] = cdc.[sdpddj_conv]
	,[sdaddj] = cdc.[sdaddj]
	,[sdaddj_conv] = cdc.[sdaddj_conv]
	,[sdivd] = cdc.[sdivd]
	,[sdivd_conv] = cdc.[sdivd_conv]
	,[sdcndj] = cdc.[sdcndj]
	,[sdcndj_conv] = cdc.[sdcndj_conv]
	,[sddgl] = cdc.[sddgl]
	,[sddgl_conv] = cdc.[sddgl_conv]
	,[sdrsdj] = cdc.[sdrsdj]
	,[sdrsdj_conv] = cdc.[sdrsdj_conv]
	,[sdpefj] = cdc.[sdpefj]
	,[sdpefj_conv] = cdc.[sdpefj_conv]
	,[sdppdj] = cdc.[sdppdj]
	,[sdppdj_conv] = cdc.[sdppdj_conv]
	,[sdvr01] = cdc.[sdvr01]
	,[sdvr01_conv] = cdc.[sdvr01_conv]
	,[sdvr02] = cdc.[sdvr02]
	,[sdvr02_conv] = cdc.[sdvr02_conv]
	,[sditm] = cdc.[sditm]
	,[sdlitm] = cdc.[sdlitm]
	,[sdlitm_conv] = cdc.[sdlitm_conv]
	,[sdaitm] = cdc.[sdaitm]
	,[sdaitm_conv] = cdc.[sdaitm_conv]
	,[sdlocn] = cdc.[sdlocn]
	,[sdlocn_conv] = cdc.[sdlocn_conv]
	,[sdlotn] = cdc.[sdlotn]
	,[sdlotn_conv] = cdc.[sdlotn_conv]
	,[sdfrgd] = cdc.[sdfrgd]
	,[sdthgd] = cdc.[sdthgd]
	,[sdfrmp] = cdc.[sdfrmp]
	,[sdfrmp_conv] = cdc.[sdfrmp_conv]
	,[sdthrp] = cdc.[sdthrp]
	,[sdthrp_conv] = cdc.[sdthrp_conv]
	,[sdexdp] = cdc.[sdexdp]
	,[sdexdp_conv] = cdc.[sdexdp_conv]
	,[sddsc1] = cdc.[sddsc1]
	,[sddsc1_conv] = cdc.[sddsc1_conv]
	,[sddsc2] = cdc.[sddsc2]
	,[sddsc2_conv] = cdc.[sddsc2_conv]
	,[sdlnty] = cdc.[sdlnty]
	,[sdlnty_conv] = cdc.[sdlnty_conv]
	,[sdnxtr] = cdc.[sdnxtr]
	,[sdlttr] = cdc.[sdlttr]
	,[sdemcu] = cdc.[sdemcu]
	,[sdemcu_conv] = cdc.[sdemcu_conv]
	,[sdrlit] = cdc.[sdrlit]
	,[sdrlit_conv] = cdc.[sdrlit_conv]
	,[sdktln] = cdc.[sdktln]
	,[sdktln_conv] = cdc.[sdktln_conv]
	,[sdcpnt] = cdc.[sdcpnt]
	,[sdrkit] = cdc.[sdrkit]
	,[sdrkit_conv] = cdc.[sdrkit_conv]
	,[sdktp] = cdc.[sdktp]
	,[sdktp_conv] = cdc.[sdktp_conv]
	,[sdsrp1] = cdc.[sdsrp1]
	,[sdsrp2] = cdc.[sdsrp2]
	,[sdsrp3] = cdc.[sdsrp3]
	,[sdsrp4] = cdc.[sdsrp4]
	,[sdsrp5] = cdc.[sdsrp5]
	,[sdprp1] = cdc.[sdprp1]
	,[sdprp2] = cdc.[sdprp2]
	,[sdprp3] = cdc.[sdprp3]
	,[sdprp4] = cdc.[sdprp4]
	,[sdprp5] = cdc.[sdprp5]
	,[sduom] = cdc.[sduom]
	,[sduorg] = cdc.[sduorg]
	,[sduorg_conv] = cdc.[sduorg_conv]
	,[sdsoqs] = cdc.[sdsoqs]
	,[sdsoqs_conv] = cdc.[sdsoqs_conv]
	,[sdsobk] = cdc.[sdsobk]
	,[sdsobk_conv] = cdc.[sdsobk_conv]
	,[sdsocn] = cdc.[sdsocn]
	,[sdsocn_conv] = cdc.[sdsocn_conv]
	,[sdsone] = cdc.[sdsone]
	,[sdsone_conv] = cdc.[sdsone_conv]
	,[sduopn] = cdc.[sduopn]
	,[sduopn_conv] = cdc.[sduopn_conv]
	,[sdqtyt] = cdc.[sdqtyt]
	,[sdqtyt_conv] = cdc.[sdqtyt_conv]
	,[sdqrlv] = cdc.[sdqrlv]
	,[sdqrlv_conv] = cdc.[sdqrlv_conv]
	,[sdcomm] = cdc.[sdcomm]
	,[sdotqy] = cdc.[sdotqy]
	,[sduprc] = cdc.[sduprc]
	,[sduprc_conv] = cdc.[sduprc_conv]
	,[sdaexp] = cdc.[sdaexp]
	,[sdaexp_conv] = cdc.[sdaexp_conv]
	,[sdaopn] = cdc.[sdaopn]
	,[sdaopn_conv] = cdc.[sdaopn_conv]
	,[sdprov] = cdc.[sdprov]
	,[sdprov_conv] = cdc.[sdprov_conv]
	,[sdtpc] = cdc.[sdtpc]
	,[sdtpc_conv] = cdc.[sdtpc_conv]
	,[sdapum] = cdc.[sdapum]
	,[sdlprc] = cdc.[sdlprc]
	,[sdlprc_conv] = cdc.[sdlprc_conv]
	,[sduncs] = cdc.[sduncs]
	,[sduncs_conv] = cdc.[sduncs_conv]
	,[sdecst] = cdc.[sdecst]
	,[sdecst_conv] = cdc.[sdecst_conv]
	,[sdcsto] = cdc.[sdcsto]
	,[sdcsto_conv] = cdc.[sdcsto_conv]
	,[sdtcst] = cdc.[sdtcst]
	,[sdtcst_conv] = cdc.[sdtcst_conv]
	,[sdinmg] = cdc.[sdinmg]
	,[sdptc] = cdc.[sdptc]
	,[sdptc_conv] = cdc.[sdptc_conv]
	,[sdryin] = cdc.[sdryin]
	,[sddtbs] = cdc.[sddtbs]
	,[sdtrdc] = cdc.[sdtrdc]
	,[sdtrdc_conv] = cdc.[sdtrdc_conv]
	,[sdfun2] = cdc.[sdfun2]
	,[sdfun2_conv] = cdc.[sdfun2_conv]
	,[sdasn] = cdc.[sdasn]
	,[sdprgr] = cdc.[sdprgr]
	,[sdclvl] = cdc.[sdclvl]
	,[sdclvl_conv] = cdc.[sdclvl_conv]
	,[sdcadc] = cdc.[sdcadc]
	,[sdcadc_conv] = cdc.[sdcadc_conv]
	,[sdkco] = cdc.[sdkco]
	,[sdkco_conv] = cdc.[sdkco_conv]
	,[sddoc] = cdc.[sddoc]
	,[sddct] = cdc.[sddct]
	,[sdodoc] = cdc.[sdodoc]
	,[sdodct] = cdc.[sdodct]
	,[sdokc] = cdc.[sdokc]
	,[sdokc_conv] = cdc.[sdokc_conv]
	,[sdpsn] = cdc.[sdpsn]
	,[sddeln] = cdc.[sddeln]
	,[sdtax1] = cdc.[sdtax1]
	,[sdtxa1] = cdc.[sdtxa1]
	,[sdtxa1_conv] = cdc.[sdtxa1_conv]
	,[sdexr1] = cdc.[sdexr1]
	,[sdatxt] = cdc.[sdatxt]
	,[sdatxt_conv] = cdc.[sdatxt_conv]
	,[sdprio] = cdc.[sdprio]
	,[sdresl] = cdc.[sdresl]
	,[sdresl_conv] = cdc.[sdresl_conv]
	,[sdback] = cdc.[sdback]
	,[sdback_conv] = cdc.[sdback_conv]
	,[sdsbal] = cdc.[sdsbal]
	,[sdsbal_conv] = cdc.[sdsbal_conv]
	,[sdapts] = cdc.[sdapts]
	,[sdapts_conv] = cdc.[sdapts_conv]
	,[sdlob] = cdc.[sdlob]
	,[sdeuse] = cdc.[sdeuse]
	,[sddtys] = cdc.[sddtys]
	,[sdntr] = cdc.[sdntr]
	,[sdvend] = cdc.[sdvend]
	,[sdvend_conv] = cdc.[sdvend_conv]
	,[sdcars] = cdc.[sdcars]
	,[sdcars_conv] = cdc.[sdcars_conv]
	,[sdmot] = cdc.[sdmot]
	,[sdrout] = cdc.[sdrout]
	,[sdstop] = cdc.[sdstop]
	,[sdzon] = cdc.[sdzon]
	,[sdcnid] = cdc.[sdcnid]
	,[sdcnid_conv] = cdc.[sdcnid_conv]
	,[sdfrth] = cdc.[sdfrth]
	,[sdshcm] = cdc.[sdshcm]
	,[sdshcn] = cdc.[sdshcn]
	,[sdsern] = cdc.[sdsern]
	,[sdsern_conv] = cdc.[sdsern_conv]
	,[sduom1] = cdc.[sduom1]
	,[sdpqor] = cdc.[sdpqor]
	,[sdpqor_conv] = cdc.[sdpqor_conv]
	,[sduom2] = cdc.[sduom2]
	,[sdsqor] = cdc.[sdsqor]
	,[sdsqor_conv] = cdc.[sdsqor_conv]
	,[sduom4] = cdc.[sduom4]
	,[sditwt] = cdc.[sditwt]
	,[sditwt_conv] = cdc.[sditwt_conv]
	,[sdwtum] = cdc.[sdwtum]
	,[sditvl] = cdc.[sditvl]
	,[sditvl_conv] = cdc.[sditvl_conv]
	,[sdvlum] = cdc.[sdvlum]
	,[sdrprc] = cdc.[sdrprc]
	,[sdorpr] = cdc.[sdorpr]
	,[sdorp] = cdc.[sdorp]
	,[sdorp_conv] = cdc.[sdorp_conv]
	,[sdcmgp] = cdc.[sdcmgp]
	,[sdcmgp_conv] = cdc.[sdcmgp_conv]
	,[sdglc] = cdc.[sdglc]
	,[sdglc_conv] = cdc.[sdglc_conv]
	,[sdctry] = cdc.[sdctry]
	,[sdfy] = cdc.[sdfy]
	,[sdfy_conv] = cdc.[sdfy_conv]
	,[sdso01] = cdc.[sdso01]
	,[sdso01_conv] = cdc.[sdso01_conv]
	,[sdso02] = cdc.[sdso02]
	,[sdso02_conv] = cdc.[sdso02_conv]
	,[sdso03] = cdc.[sdso03]
	,[sdso03_conv] = cdc.[sdso03_conv]
	,[sdso04] = cdc.[sdso04]
	,[sdso04_conv] = cdc.[sdso04_conv]
	,[sdso05] = cdc.[sdso05]
	,[sdso05_conv] = cdc.[sdso05_conv]
	,[sdso06] = cdc.[sdso06]
	,[sdso06_conv] = cdc.[sdso06_conv]
	,[sdso07] = cdc.[sdso07]
	,[sdso07_conv] = cdc.[sdso07_conv]
	,[sdso08] = cdc.[sdso08]
	,[sdso08_conv] = cdc.[sdso08_conv]
	,[sdso09] = cdc.[sdso09]
	,[sdso09_conv] = cdc.[sdso09_conv]
	,[sdso10] = cdc.[sdso10]
	,[sdso10_conv] = cdc.[sdso10_conv]
	,[sdso11] = cdc.[sdso11]
	,[sdso11_conv] = cdc.[sdso11_conv]
	,[sdso12] = cdc.[sdso12]
	,[sdso12_conv] = cdc.[sdso12_conv]
	,[sdso13] = cdc.[sdso13]
	,[sdso13_conv] = cdc.[sdso13_conv]
	,[sdso14] = cdc.[sdso14]
	,[sdso14_conv] = cdc.[sdso14_conv]
	,[sdso15] = cdc.[sdso15]
	,[sdso15_conv] = cdc.[sdso15_conv]
	,[sdacom] = cdc.[sdacom]
	,[sdacom_conv] = cdc.[sdacom_conv]
	,[sdcmcg] = cdc.[sdcmcg]
	,[sdcmcg_conv] = cdc.[sdcmcg_conv]
	,[sdrcd] = cdc.[sdrcd]
	,[sdgrwt] = cdc.[sdgrwt]
	,[sdgrwt_conv] = cdc.[sdgrwt_conv]
	,[sdgwum] = cdc.[sdgwum]
	,[sdsbl] = cdc.[sdsbl]
	,[sdsbl_conv] = cdc.[sdsbl_conv]
	,[sdsblt] = cdc.[sdsblt]
	,[sdlcod] = cdc.[sdlcod]
	,[sdupc1] = cdc.[sdupc1]
	,[sdupc2] = cdc.[sdupc2]
	,[sdupc3] = cdc.[sdupc3]
	,[sdswms] = cdc.[sdswms]
	,[sdswms_conv] = cdc.[sdswms_conv]
	,[sduncd] = cdc.[sduncd]
	,[sdcrmd] = cdc.[sdcrmd]
	,[sdcrcd] = cdc.[sdcrcd]
	,[sdcrcd_conv] = cdc.[sdcrcd_conv]
	,[sdcrr] = cdc.[sdcrr]
	,[sdfprc] = cdc.[sdfprc]
	,[sdfprc_conv] = cdc.[sdfprc_conv]
	,[sdfup] = cdc.[sdfup]
	,[sdfup_conv] = cdc.[sdfup_conv]
	,[sdfea] = cdc.[sdfea]
	,[sdfea_conv] = cdc.[sdfea_conv]
	,[sdfuc] = cdc.[sdfuc]
	,[sdfuc_conv] = cdc.[sdfuc_conv]
	,[sdfec] = cdc.[sdfec]
	,[sdfec_conv] = cdc.[sdfec_conv]
	,[sdurcd] = cdc.[sdurcd]
	,[sdurcd_conv] = cdc.[sdurcd_conv]
	,[sdurdt] = cdc.[sdurdt]
	,[sdurdt_conv] = cdc.[sdurdt_conv]
	,[sdurat] = cdc.[sdurat]
	,[sdurat_conv] = cdc.[sdurat_conv]
	,[sdurab] = cdc.[sdurab]
	,[sdurab_conv] = cdc.[sdurab_conv]
	,[sdurrf] = cdc.[sdurrf]
	,[sdurrf_conv] = cdc.[sdurrf_conv]
	,[sdtorg] = cdc.[sdtorg]
	,[sdtorg_conv] = cdc.[sdtorg_conv]
	,[sduser] = cdc.[sduser]
	,[sduser_conv] = cdc.[sduser_conv]
	,[sdpid] = cdc.[sdpid]
	,[sdpid_conv] = cdc.[sdpid_conv]
	,[sdjobn] = cdc.[sdjobn]
	,[sdjobn_conv] = cdc.[sdjobn_conv]
	,[sdupmj] = cdc.[sdupmj]
	,[sdupmj_conv] = cdc.[sdupmj_conv]
	,[sdtday] = cdc.[sdtday]
	,[sdso16] = cdc.[sdso16]
	,[sdso16_conv] = cdc.[sdso16_conv]
	,[sdso17] = cdc.[sdso17]
	,[sdso17_conv] = cdc.[sdso17_conv]
	,[sdso18] = cdc.[sdso18]
	,[sdso18_conv] = cdc.[sdso18_conv]
	,[sdso19] = cdc.[sdso19]
	,[sdso19_conv] = cdc.[sdso19_conv]
	,[sdso20] = cdc.[sdso20]
	,[sdso20_conv] = cdc.[sdso20_conv]
	,[sdir01] = cdc.[sdir01]
	,[sdir01_conv] = cdc.[sdir01_conv]
	,[sdir02] = cdc.[sdir02]
	,[sdir02_conv] = cdc.[sdir02_conv]
	,[sdir03] = cdc.[sdir03]
	,[sdir03_conv] = cdc.[sdir03_conv]
	,[sdir04] = cdc.[sdir04]
	,[sdir04_conv] = cdc.[sdir04_conv]
	,[sdir05] = cdc.[sdir05]
	,[sdir05_conv] = cdc.[sdir05_conv]
	,[sdsoor] = cdc.[sdsoor]
	,[sdsoor_conv] = cdc.[sdsoor_conv]
	,[sdvr03] = cdc.[sdvr03]
	,[sdvr03_conv] = cdc.[sdvr03_conv]
	,[sddeid] = cdc.[sddeid]
	,[sddeid_conv] = cdc.[sddeid_conv]
	,[sdpsig] = cdc.[sdpsig]
	,[sdpsig_conv] = cdc.[sdpsig_conv]
	,[sdrlnu] = cdc.[sdrlnu]
	,[sdrlnu_conv] = cdc.[sdrlnu_conv]
	,[sdpmdt] = cdc.[sdpmdt]
	,[sdrltm] = cdc.[sdrltm]
	,[sdrldj] = cdc.[sdrldj]
	,[sdrldj_conv] = cdc.[sdrldj_conv]
	,[sddrqt] = cdc.[sddrqt]
	,[sdadtm] = cdc.[sdadtm]
	,[sdoptt] = cdc.[sdoptt]
	,[sdpdtt] = cdc.[sdpdtt]
	,[sdpstm] = cdc.[sdpstm]
	,[sdxdck] = cdc.[sdxdck]
	,[sdxdck_conv] = cdc.[sdxdck_conv]
	,[sdxpty] = cdc.[sdxpty]
	,[sdxpty_conv] = cdc.[sdxpty_conv]
	,[sddual] = cdc.[sddual]
	,[sddual_conv] = cdc.[sddual_conv]
	,[sdbsc] = cdc.[sdbsc]
	,[sdcbsc] = cdc.[sdcbsc]
	,[sdcord] = cdc.[sdcord]
	,[sddvan] = cdc.[sddvan]
	,[sddvan_conv] = cdc.[sddvan_conv]
	,[sdpend] = cdc.[sdpend]
	,[sdpend_conv] = cdc.[sdpend_conv]
	,[sdrfrv] = cdc.[sdrfrv]
	,[sdmcln] = cdc.[sdmcln]
	,[sdmcln_conv] = cdc.[sdmcln_conv]
	,[sdshpn] = cdc.[sdshpn]
	,[sdrsdt] = cdc.[sdrsdt]
	,[sdprjm] = cdc.[sdprjm]
	,[sdprjm_conv] = cdc.[sdprjm_conv]
	,[sdoseq] = cdc.[sdoseq]
	,[sdoseq_conv] = cdc.[sdoseq_conv]
	,[sdmerl] = cdc.[sdmerl]
	,[sdmerl_conv] = cdc.[sdmerl_conv]
	,[sdhold] = cdc.[sdhold]
	,[sdhdbu] = cdc.[sdhdbu]
	,[sdhdbu_conv] = cdc.[sdhdbu_conv]
	,[sddmbu] = cdc.[sddmbu]
	,[sddmbu_conv] = cdc.[sddmbu_conv]
	,[sdbcrc] = cdc.[sdbcrc]
	,[sdbcrc_conv] = cdc.[sdbcrc_conv]
	,[sdodln] = cdc.[sdodln]
	,[sdodln_conv] = cdc.[sdodln_conv]
	,[sdopdj] = cdc.[sdopdj]
	,[sdopdj_conv] = cdc.[sdopdj_conv]
	,[sdxkco] = cdc.[sdxkco]
	,[sdxkco_conv] = cdc.[sdxkco_conv]
	,[sdxorn] = cdc.[sdxorn]
	,[sdxcto] = cdc.[sdxcto]
	,[sdxlln] = cdc.[sdxlln]
	,[sdxlln_conv] = cdc.[sdxlln_conv]
	,[sdxsfx] = cdc.[sdxsfx]
	,[sdxsfx_conv] = cdc.[sdxsfx_conv]
	,[sdpoe] = cdc.[sdpoe]
	,[sdpmto] = cdc.[sdpmto]
	,[sdpmto_conv] = cdc.[sdpmto_conv]
	,[sdanby] = cdc.[sdanby]
	,[sdpmtn] = cdc.[sdpmtn]
	,[sdpmtn_conv] = cdc.[sdpmtn_conv]
	,[sdnumb] = cdc.[sdnumb]
	,[sdaaid] = cdc.[sdaaid]
	,[sdspattn] = cdc.[sdspattn]
	,[sdspattn_conv] = cdc.[sdspattn_conv]
	,[sdpran8] = cdc.[sdpran8]
	,[sdpran8_conv] = cdc.[sdpran8_conv]
	,[sdprcidln] = cdc.[sdprcidln]
	,[sdprcidln_conv] = cdc.[sdprcidln_conv]
	,[sdccidln] = cdc.[sdccidln]
	,[sdccidln_conv] = cdc.[sdccidln_conv]
	,[sdshccidln] = cdc.[sdshccidln]
	,[sdshccidln_conv] = cdc.[sdshccidln_conv]
	,[sdoppid] = cdc.[sdoppid]
	,[sdoppid_conv] = cdc.[sdoppid_conv]
	,[sdostp] = cdc.[sdostp]
	,[sdukid] = cdc.[sdukid]
	,[sdcatnm] = cdc.[sdcatnm]
	,[sdcatnm_conv] = cdc.[sdcatnm_conv]
	,[sdalloc] = cdc.[sdalloc]
	,[sdalloc_conv] = cdc.[sdalloc_conv]
	,[sdfulpid] = cdc.[sdfulpid]
	,[sdfulpid_conv] = cdc.[sdfulpid_conv]
	,[sdallsts] = cdc.[sdallsts]
	,[sdoscore] = cdc.[sdoscore]
	,[sdoscore_conv] = cdc.[sdoscore_conv]
	,[sdoscoreo] = cdc.[sdoscoreo]
	,[sdoscoreo_conv] = cdc.[sdoscoreo_conv]
	,[sdcmco] = cdc.[sdcmco]
	,[sdcmco_conv] = cdc.[sdcmco_conv]
	,[sdkitid] = cdc.[sdkitid]
	,[sdkitid_conv] = cdc.[sdkitid_conv]
	,[sdkitamtdom] = cdc.[sdkitamtdom]
	,[sdkitamtdom_conv] = cdc.[sdkitamtdom_conv]
	,[sdkitamtfor] = cdc.[sdkitamtfor]
	,[sdkitamtfor_conv] = cdc.[sdkitamtfor_conv]
	,[sdkitdirty] = cdc.[sdkitdirty]
	,[sdkitdirty_conv] = cdc.[sdkitdirty_conv]
	,[sdocitt] = cdc.[sdocitt]
	,[sdocitt_conv] = cdc.[sdocitt_conv]
	,[sdoccardno] = cdc.[sdoccardno]
	,[sdoccardno_conv] = cdc.[sdoccardno_conv]
	,[sdpmpn] = cdc.[sdpmpn]
	,[sdpmpn_conv] = cdc.[sdpmpn_conv]
	,[sdpns] = cdc.[sdpns] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f4211_cdc cdc
WHERE
    rep_jde_qat.f4211_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f4211_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f4211_new AF:{{ task_instance_key_str }}' ) 
