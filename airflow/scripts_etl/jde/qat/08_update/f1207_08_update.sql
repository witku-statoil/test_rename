-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f1207_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[fwnumb] = cdc.[fwnumb]
	,[fwapid] = cdc.[fwapid]
	,[fwapid_conv] = cdc.[fwapid_conv]
	,[fwasid] = cdc.[fwasid]
	,[fwasid_conv] = cdc.[fwasid_conv]
	,[fwsrvt] = cdc.[fwsrvt]
	,[fwmsts] = cdc.[fwmsts]
	,[fwmpri] = cdc.[fwmpri]
	,[fwky] = cdc.[fwky]
	,[fwky_conv] = cdc.[fwky_conv]
	,[fwanp] = cdc.[fwanp]
	,[fwrmk] = cdc.[fwrmk]
	,[fwrmk_conv] = cdc.[fwrmk_conv]
	,[fwsrvd] = cdc.[fwsrvd]
	,[fwsrvd_conv] = cdc.[fwsrvd_conv]
	,[fwsrvm] = cdc.[fwsrvm]
	,[fwsrvm_conv] = cdc.[fwsrvm_conv]
	,[fwsrvh] = cdc.[fwsrvh]
	,[fwsrvh_conv] = cdc.[fwsrvh_conv]
	,[fwtdt] = cdc.[fwtdt]
	,[fwtdt_conv] = cdc.[fwtdt_conv]
	,[fwcplm] = cdc.[fwcplm]
	,[fwcplm_conv] = cdc.[fwcplm_conv]
	,[fwcplh] = cdc.[fwcplh]
	,[fwcplh_conv] = cdc.[fwcplh_conv]
	,[fwcpld] = cdc.[fwcpld]
	,[fwcpld_conv] = cdc.[fwcpld_conv]
	,[fwlstm] = cdc.[fwlstm]
	,[fwlstm_conv] = cdc.[fwlstm_conv]
	,[fwlsth] = cdc.[fwlsth]
	,[fwlsth_conv] = cdc.[fwlsth_conv]
	,[fwlcpd] = cdc.[fwlcpd]
	,[fwlcpd_conv] = cdc.[fwlcpd_conv]
	,[fwuser] = cdc.[fwuser]
	,[fwuser_conv] = cdc.[fwuser_conv]
	,[fwpid] = cdc.[fwpid]
	,[fwpid_conv] = cdc.[fwpid_conv]
	,[fwupmj] = cdc.[fwupmj]
	,[fwupmj_conv] = cdc.[fwupmj_conv]
	,[fwjobn] = cdc.[fwjobn]
	,[fwjobn_conv] = cdc.[fwjobn_conv]
	,[fwdoco] = cdc.[fwdoco]
	,[fwwona] = cdc.[fwwona]
	,[fwmpc] = cdc.[fwmpc]
	,[fwsrvf] = cdc.[fwsrvf]
	,[fwsrvf_conv] = cdc.[fwsrvf_conv]
	,[fwcplf] = cdc.[fwcplf]
	,[fwcplf_conv] = cdc.[fwcplf_conv]
	,[fwlstf] = cdc.[fwlstf]
	,[fwlstf_conv] = cdc.[fwlstf_conv]
	,[fwmltw] = cdc.[fwmltw]
	,[fworgm] = cdc.[fworgm]
	,[fworgm_conv] = cdc.[fworgm_conv]
	,[fworgh] = cdc.[fworgh]
	,[fworgh_conv] = cdc.[fworgh_conv]
	,[fworgf] = cdc.[fworgf]
	,[fworgf_conv] = cdc.[fworgf_conv]
	,[fwoccu] = cdc.[fwoccu]
	,[fwoccu_conv] = cdc.[fwoccu_conv]
	,[fwfrin] = cdc.[fwfrin]
	,[fwupmt] = cdc.[fwupmt]
	,[fwmcu] = cdc.[fwmcu]
	,[fwmcu_conv] = cdc.[fwmcu_conv]
	,[fwaaid] = cdc.[fwaaid]
	,[fwcrtl] = cdc.[fwcrtl]
	,[fwcrtl_conv] = cdc.[fwcrtl_conv]
	,[fwpnst] = cdc.[fwpnst]
	,[fwpnst_conv] = cdc.[fwpnst_conv]
	,[fwpmc1] = cdc.[fwpmc1]
	,[fwpmc2] = cdc.[fwpmc2]
	,[fwdnhr] = cdc.[fwdnhr]
	,[fwdnhr_conv] = cdc.[fwdnhr_conv]
	,[fwpdfl] = cdc.[fwpdfl]
	,[fwukid] = cdc.[fwukid]
	,[fwtolu] = cdc.[fwtolu]
	,[fwtolu_conv] = cdc.[fwtolu_conv]
	,[fwtoll] = cdc.[fwtoll]
	,[fwtoll_conv] = cdc.[fwtoll_conv]
	,[fwhldd] = cdc.[fwhldd]
	,[fwhldd_conv] = cdc.[fwhldd_conv]
	,[fwsphr] = cdc.[fwsphr]
	,[fwsphr_conv] = cdc.[fwsphr_conv]
	,[fwspwk] = cdc.[fwspwk]
	,[fwspwk_conv] = cdc.[fwspwk_conv]
	,[fwspmn] = cdc.[fwspmn]
	,[fwspmn_conv] = cdc.[fwspmn_conv]
	,[fwwk] = cdc.[fwwk]
	,[fwwk_conv] = cdc.[fwwk_conv]
	,[fwspdw] = cdc.[fwspdw]
	,[fwspdw_conv] = cdc.[fwspdw_conv]
	,[fwpdfg] = cdc.[fwpdfg]
	,[fwsrvm4] = cdc.[fwsrvm4]
	,[fwsrvm4_conv] = cdc.[fwsrvm4_conv]
	,[fwlstm4] = cdc.[fwlstm4]
	,[fwlstm4_conv] = cdc.[fwlstm4_conv]
	,[fwcplm4] = cdc.[fwcplm4]
	,[fwcplm4_conv] = cdc.[fwcplm4_conv]
	,[fworgm4] = cdc.[fworgm4]
	,[fworgm4_conv] = cdc.[fworgm4_conv]
	,[fwsrvm5] = cdc.[fwsrvm5]
	,[fwsrvm5_conv] = cdc.[fwsrvm5_conv]
	,[fwlstm5] = cdc.[fwlstm5]
	,[fwlstm5_conv] = cdc.[fwlstm5_conv]
	,[fwcplm5] = cdc.[fwcplm5]
	,[fwcplm5_conv] = cdc.[fwcplm5_conv]
	,[fworgm5] = cdc.[fworgm5]
	,[fworgm5_conv] = cdc.[fworgm5_conv]
	,[fwsrvm6] = cdc.[fwsrvm6]
	,[fwsrvm6_conv] = cdc.[fwsrvm6_conv]
	,[fwlstm6] = cdc.[fwlstm6]
	,[fwlstm6_conv] = cdc.[fwlstm6_conv]
	,[fwcplm6] = cdc.[fwcplm6]
	,[fwcplm6_conv] = cdc.[fwcplm6_conv]
	,[fworgm6] = cdc.[fworgm6]
	,[fworgm6_conv] = cdc.[fworgm6_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f1207_cdc cdc
WHERE
    rep_jde_qat.f1207_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f1207_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f1207_new AF:{{ task_instance_key_str }}' ) 
