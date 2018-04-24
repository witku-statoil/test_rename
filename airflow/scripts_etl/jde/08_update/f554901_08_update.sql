-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f554901_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[ddvr01] = cdc.[ddvr01]
	,[ddvr01_conv] = cdc.[ddvr01_conv]
	,[dddoco] = cdc.[dddoco]
	,[ddlnid] = cdc.[ddlnid]
	,[ddlnid_conv] = cdc.[ddlnid_conv]
	,[ddlitm] = cdc.[ddlitm]
	,[ddlitm_conv] = cdc.[ddlitm_conv]
	,[ddy55doco] = cdc.[ddy55doco]
	,[ddy55doco_conv] = cdc.[ddy55doco_conv]
	,[ddy55msgno] = cdc.[ddy55msgno]
	,[ddy55msgno_conv] = cdc.[ddy55msgno_conv]
	,[ddy55insdj] = cdc.[ddy55insdj]
	,[ddy55insdj_conv] = cdc.[ddy55insdj_conv]
	,[ddy55insdt] = cdc.[ddy55insdt]
	,[ddshpn] = cdc.[ddshpn]
	,[ddssts] = cdc.[ddssts]
	,[ddkcoo] = cdc.[ddkcoo]
	,[ddkcoo_conv] = cdc.[ddkcoo_conv]
	,[dddcto] = cdc.[dddcto]
	,[ddy55dlqs] = cdc.[ddy55dlqs]
	,[ddy55dlqs_conv] = cdc.[ddy55dlqs_conv]
	,[ddums] = cdc.[ddums]
	,[ddums_conv] = cdc.[ddums_conv]
	,[ddtemp] = cdc.[ddtemp]
	,[ddtemp_conv] = cdc.[ddtemp_conv]
	,[ddy55tnfl] = cdc.[ddy55tnfl]
	,[ddy55tnfl_conv] = cdc.[ddy55tnfl_conv]
	,[ddy55orfg] = cdc.[ddy55orfg]
	,[ddy55orfg_conv] = cdc.[ddy55orfg_conv]
	,[dddcdt] = cdc.[dddcdt]
	,[dddcdt_conv] = cdc.[dddcdt_conv]
	,[dddldl] = cdc.[dddldl]
	,[dddldl_conv] = cdc.[dddldl_conv]
	,[ddy55aclt] = cdc.[ddy55aclt]
	,[dddisn] = cdc.[dddisn]
	,[ddy55xcrd] = cdc.[ddy55xcrd]
	,[ddy55ycrd] = cdc.[ddy55ycrd]
	,[ddcmqt] = cdc.[ddcmqt]
	,[ddcmqt_conv] = cdc.[ddcmqt_conv]
	,[ddy55drcd] = cdc.[ddy55drcd]
	,[ddy55drcd_conv] = cdc.[ddy55drcd_conv]
	,[dddlda] = cdc.[dddlda]
	,[dddlda_conv] = cdc.[dddlda_conv]
	,[ddtdlf] = cdc.[ddtdlf]
	,[ddy55tpdt] = cdc.[ddy55tpdt]
	,[ddy55gpsf] = cdc.[ddy55gpsf]
	,[ddy55pdflg] = cdc.[ddy55pdflg]
	,[ddy55pdflg_conv] = cdc.[ddy55pdflg_conv]
	,[ddy55disn1] = cdc.[ddy55disn1]
	,[ddy55cttm] = cdc.[ddy55cttm]
	,[ddmcux] = cdc.[ddmcux]
	,[ddmcux_conv] = cdc.[ddmcux_conv]
	,[ddshan] = cdc.[ddshan]
	,[ddcrcp] = cdc.[ddcrcp]
	,[ddcrcp_conv] = cdc.[ddcrcp_conv]
	,[ddfrvc] = cdc.[ddfrvc]
	,[ddfrvc_conv] = cdc.[ddfrvc_conv]
	,[dddoc] = cdc.[dddoc]
	,[ddgnrt] = cdc.[ddgnrt]
	,[ddgnrt_conv] = cdc.[ddgnrt_conv]
	,[ddy55gnno] = cdc.[ddy55gnno]
	,[ddy55gnno_conv] = cdc.[ddy55gnno_conv]
	,[ddtkid] = cdc.[ddtkid]
	,[ddtkid_conv] = cdc.[ddtkid_conv]
	,[dddend] = cdc.[dddend]
	,[dddend_conv] = cdc.[dddend_conv]
	,[ddqtyl] = cdc.[ddqtyl]
	,[ddqtyl_conv] = cdc.[ddqtyl_conv]
	,[ddcert] = cdc.[ddcert]
	,[ddy55govid] = cdc.[ddy55govid]
	,[ddy55govid_conv] = cdc.[ddy55govid_conv]
	,[ddedbt] = cdc.[ddedbt]
	,[ddedbt_conv] = cdc.[ddedbt_conv]
	,[ddedtn] = cdc.[ddedtn]
	,[ddedtn_conv] = cdc.[ddedtn_conv]
	,[ddflag] = cdc.[ddflag]
	,[ddflag_conv] = cdc.[ddflag_conv]
	,[dduser] = cdc.[dduser]
	,[dduser_conv] = cdc.[dduser_conv]
	,[ddpid] = cdc.[ddpid]
	,[ddpid_conv] = cdc.[ddpid_conv]
	,[ddjobn] = cdc.[ddjobn]
	,[ddjobn_conv] = cdc.[ddjobn_conv]
	,[ddupmj] = cdc.[ddupmj]
	,[ddupmj_conv] = cdc.[ddupmj_conv]
	,[ddupmt] = cdc.[ddupmt]
	,[ddurcd] = cdc.[ddurcd]
	,[ddurcd_conv] = cdc.[ddurcd_conv]
	,[ddurdt] = cdc.[ddurdt]
	,[ddurdt_conv] = cdc.[ddurdt_conv]
	,[ddurat] = cdc.[ddurat]
	,[ddurat_conv] = cdc.[ddurat_conv]
	,[ddurrf] = cdc.[ddurrf]
	,[ddurrf_conv] = cdc.[ddurrf_conv]
	,[ddurab] = cdc.[ddurab]
	,[ddurab_conv] = cdc.[ddurab_conv]
	,[ddrcd] = cdc.[ddrcd]
	,[ddy55char1] = cdc.[ddy55char1]
	,[ddy55char1_conv] = cdc.[ddy55char1_conv]
	,[ddy55char2] = cdc.[ddy55char2]
	,[ddy55char2_conv] = cdc.[ddy55char2_conv]
	,[ddy55date1] = cdc.[ddy55date1]
	,[ddy55date1_conv] = cdc.[ddy55date1_conv]
	,[ddy55date2] = cdc.[ddy55date2]
	,[ddy55date2_conv] = cdc.[ddy55date2_conv]
	,[ddy55strg1] = cdc.[ddy55strg1]
	,[ddy55strg1_conv] = cdc.[ddy55strg1_conv]
	,[ddy55strg2] = cdc.[ddy55strg2]
	,[ddy55strg2_conv] = cdc.[ddy55strg2_conv]
	,[ddy55strg3] = cdc.[ddy55strg3]
	,[ddy55strg3_conv] = cdc.[ddy55strg3_conv]
	,[ddy55strg4] = cdc.[ddy55strg4]
	,[ddy55strg4_conv] = cdc.[ddy55strg4_conv]
	,[ddy55strg5] = cdc.[ddy55strg5]
	,[ddy55strg5_conv] = cdc.[ddy55strg5_conv]
	,[ddy55strg6] = cdc.[ddy55strg6]
	,[ddy55strg6_conv] = cdc.[ddy55strg6_conv]
	,[ddy55strg7] = cdc.[ddy55strg7]
	,[ddy55strg7_conv] = cdc.[ddy55strg7_conv]
	,[ddy55strg8] = cdc.[ddy55strg8]
	,[ddy55strg8_conv] = cdc.[ddy55strg8_conv]
	,[ddy55strg9] = cdc.[ddy55strg9]
	,[ddy55strg9_conv] = cdc.[ddy55strg9_conv]
	,[ddy55strg10] = cdc.[ddy55strg10]
	,[ddy55strg10_conv] = cdc.[ddy55strg10_conv]
	,[ddy55flg1] = cdc.[ddy55flg1]
	,[ddy55flg1_conv] = cdc.[ddy55flg1_conv]
	,[ddy55flg2] = cdc.[ddy55flg2]
	,[ddy55flg2_conv] = cdc.[ddy55flg2_conv]
	,[ddy55num3] = cdc.[ddy55num3]
	,[ddy55num3_conv] = cdc.[ddy55num3_conv]
	,[ddy55num4] = cdc.[ddy55num4]
	,[ddy55num4_conv] = cdc.[ddy55num4_conv]
	,[ddy55time1] = cdc.[ddy55time1]
	,[ddy55time2] = cdc.[ddy55time2]
	,[ddy55amnt1] = cdc.[ddy55amnt1]
	,[ddy55amnt1_conv] = cdc.[ddy55amnt1_conv]
	,[ddy55amnt2] = cdc.[ddy55amnt2]
	,[ddy55amnt2_conv] = cdc.[ddy55amnt2_conv]
	,[ddordnumid] = cdc.[ddordnumid]
	,[ddordnumid_conv] = cdc.[ddordnumid_conv]
	,[ddrornumid] = cdc.[ddrornumid]
	,[ddrornumid_conv] = cdc.[ddrornumid_conv]
	,[ddvmcu] = cdc.[ddvmcu]
	,[ddvmcu_conv] = cdc.[ddvmcu_conv]
	,[ddflg2] = cdc.[ddflg2]
	,[ddflg2_conv] = cdc.[ddflg2_conv]
	,[ddflg3] = cdc.[ddflg3]
	,[ddflg3_conv] = cdc.[ddflg3_conv]
	,[ddflg4] = cdc.[ddflg4]
	,[ddflg4_conv] = cdc.[ddflg4_conv]
	,[ddukid] = cdc.[ddukid]
	,[ddboolean] = cdc.[ddboolean]
	,[ddppdj] = cdc.[ddppdj]
	,[ddppdj_conv] = cdc.[ddppdj_conv]
	,[ddcflg] = cdc.[ddcflg]
	,[ddgovid1] = cdc.[ddgovid1]
	,[ddgovid1_conv] = cdc.[ddgovid1_conv]
	,[ddot1] = cdc.[ddot1]
	,[ddot1_conv] = cdc.[ddot1_conv]
	,[ddef01] = cdc.[ddef01]
	,[ddflg1] = cdc.[ddflg1]
	,[ddflg1_conv] = cdc.[ddflg1_conv]
	,[ddfmrund] = cdc.[ddfmrund]
	,[ddfmrund_conv] = cdc.[ddfmrund_conv]
	,[ddsclq] = cdc.[ddsclq]
	,[ddsclq_conv] = cdc.[ddsclq_conv]
	,[ddtme0] = cdc.[ddtme0]
	,[dddte] = cdc.[dddte]
	,[dddte_conv] = cdc.[dddte_conv]
	,[ddancc] = cdc.[ddancc]
	,[ddorgn] = cdc.[ddorgn]
	,[ddtdlt] = cdc.[ddtdlt]
	,[dditm] = cdc.[dditm]
	,[ddxpitraid] = cdc.[ddxpitraid]
	,[ddxpitraid_conv] = cdc.[ddxpitraid_conv]
	,[ddy55cert] = cdc.[ddy55cert]
	,[ddy55cert_conv] = cdc.[ddy55cert_conv]
	,[ddy55qlty] = cdc.[ddy55qlty]
	,[ddy55qlty_conv] = cdc.[ddy55qlty_conv]
	,[ddy55zdrcd] = cdc.[ddy55zdrcd] -- exclude distribution column
FROM stg_jde.tmp_upsert_f554901_cdc cdc
WHERE
    rep_jde.f554901_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f554901_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f554901_new AF:{{ task_instance_key_str }}' ) 
