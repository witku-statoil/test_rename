-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f0010_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[ccco] = cdc.[ccco]
	,[ccco_conv] = cdc.[ccco_conv]
	,[ccname] = cdc.[ccname]
	,[ccname_conv] = cdc.[ccname_conv]
	,[ccaltc] = cdc.[ccaltc]
	,[ccaltc_conv] = cdc.[ccaltc_conv]
	,[ccdfyj] = cdc.[ccdfyj]
	,[ccdfyj_conv] = cdc.[ccdfyj_conv]
	,[ccpnc] = cdc.[ccpnc]
	,[ccdot1] = cdc.[ccdot1]
	,[ccdot1_conv] = cdc.[ccdot1_conv]
	,[cccryr] = cdc.[cccryr]
	,[cccryc] = cdc.[cccryc]
	,[cccryc_conv] = cdc.[cccryc_conv]
	,[ccdot2] = cdc.[ccdot2]
	,[ccdot2_conv] = cdc.[ccdot2_conv]
	,[ccmcua] = cdc.[ccmcua]
	,[ccmcua_conv] = cdc.[ccmcua_conv]
	,[ccmcud] = cdc.[ccmcud]
	,[ccmcud_conv] = cdc.[ccmcud_conv]
	,[ccmcuc] = cdc.[ccmcuc]
	,[ccmcuc_conv] = cdc.[ccmcuc_conv]
	,[ccmcub] = cdc.[ccmcub]
	,[ccmcub_conv] = cdc.[ccmcub_conv]
	,[ccdprc] = cdc.[ccdprc]
	,[ccdprc_conv] = cdc.[ccdprc_conv]
	,[ccbktx] = cdc.[ccbktx]
	,[ccbktx_conv] = cdc.[ccbktx_conv]
	,[cctxbm] = cdc.[cctxbm]
	,[cctxbo] = cdc.[cctxbo]
	,[ccnwxj] = cdc.[ccnwxj]
	,[ccnwxj_conv] = cdc.[ccnwxj_conv]
	,[ccglie] = cdc.[ccglie]
	,[ccabin] = cdc.[ccabin]
	,[ccabin_conv] = cdc.[ccabin_conv]
	,[cccald] = cdc.[cccald]
	,[ccdtpn] = cdc.[ccdtpn]
	,[ccpnf] = cdc.[ccpnf]
	,[ccdff] = cdc.[ccdff]
	,[cccrcd] = cdc.[cccrcd]
	,[cccrcd_conv] = cdc.[cccrcd_conv]
	,[ccsmi] = cdc.[ccsmi]
	,[ccsmi_conv] = cdc.[ccsmi_conv]
	,[ccsmu] = cdc.[ccsmu]
	,[ccsmu_conv] = cdc.[ccsmu_conv]
	,[ccsms] = cdc.[ccsms]
	,[ccsms_conv] = cdc.[ccsms_conv]
	,[ccdnlt] = cdc.[ccdnlt]
	,[ccdnlt_conv] = cdc.[ccdnlt_conv]
	,[ccstmt] = cdc.[ccstmt]
	,[ccatcs] = cdc.[ccatcs]
	,[ccatcs_conv] = cdc.[ccatcs_conv]
	,[ccalgm] = cdc.[ccalgm]
	,[ccagem] = cdc.[ccagem]
	,[cccrdy] = cdc.[cccrdy]
	,[ccagr1] = cdc.[ccagr1]
	,[ccagr2] = cdc.[ccagr2]
	,[ccagr3] = cdc.[ccagr3]
	,[ccagr4] = cdc.[ccagr4]
	,[ccagr5] = cdc.[ccagr5]
	,[ccagr6] = cdc.[ccagr6]
	,[ccagr7] = cdc.[ccagr7]
	,[ccfry] = cdc.[ccfry]
	,[ccfrp] = cdc.[ccfrp]
	,[ccnnp] = cdc.[ccnnp]
	,[ccfp] = cdc.[ccfp]
	,[ccfp_conv] = cdc.[ccfp_conv]
	,[ccage] = cdc.[ccage]
	,[ccdag] = cdc.[ccdag]
	,[ccdag_conv] = cdc.[ccdag_conv]
	,[ccarpn] = cdc.[ccarpn]
	,[ccappn] = cdc.[ccappn]
	,[ccarfj] = cdc.[ccarfj]
	,[ccarfj_conv] = cdc.[ccarfj_conv]
	,[ccapfj] = cdc.[ccapfj]
	,[ccapfj_conv] = cdc.[ccapfj_conv]
	,[ccan8] = cdc.[ccan8]
	,[ccsgbk] = cdc.[ccsgbk]
	,[ccsgbk_conv] = cdc.[ccsgbk_conv]
	,[ccptco] = cdc.[ccptco]
	,[ccptco_conv] = cdc.[ccptco_conv]
	,[ccx1] = cdc.[ccx1]
	,[ccx1_conv] = cdc.[ccx1_conv]
	,[ccx2] = cdc.[ccx2]
	,[ccx2_conv] = cdc.[ccx2_conv]
	,[ccuser] = cdc.[ccuser]
	,[ccuser_conv] = cdc.[ccuser_conv]
	,[ccpid] = cdc.[ccpid]
	,[ccpid_conv] = cdc.[ccpid_conv]
	,[ccupmj] = cdc.[ccupmj]
	,[ccupmj_conv] = cdc.[ccupmj_conv]
	,[ccjobn] = cdc.[ccjobn]
	,[ccjobn_conv] = cdc.[ccjobn_conv]
	,[ccupmt] = cdc.[ccupmt]
	,[cctsid] = cdc.[cctsid]
	,[cctsco] = cdc.[cctsco]
	,[cctsco_conv] = cdc.[cctsco_conv]
	,[ccthco] = cdc.[ccthco]
	,[ccan8c] = cdc.[ccan8c] -- exclude distribution column
FROM stg_jde.tmp_upsert_f0010_cdc cdc
WHERE
    rep_jde.f0010_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f0010_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f0010_new AF:{{ task_instance_key_str }}' ) 
