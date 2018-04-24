-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f03b14_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[rzpyid] = cdc.[rzpyid]
	,[rzrc5] = cdc.[rzrc5]
	,[rzcknu] = cdc.[rzcknu]
	,[rzcknu_conv] = cdc.[rzcknu_conv]
	,[rzdoc] = cdc.[rzdoc]
	,[rzdct] = cdc.[rzdct]
	,[rzkco] = cdc.[rzkco]
	,[rzkco_conv] = cdc.[rzkco_conv]
	,[rzsfx] = cdc.[rzsfx]
	,[rzsfx_conv] = cdc.[rzsfx_conv]
	,[rzan8] = cdc.[rzan8]
	,[rzdctm] = cdc.[rzdctm]
	,[rzdmtj] = cdc.[rzdmtj]
	,[rzdmtj_conv] = cdc.[rzdmtj_conv]
	,[rzdgj] = cdc.[rzdgj]
	,[rzdgj_conv] = cdc.[rzdgj_conv]
	,[rzpost] = cdc.[rzpost]
	,[rzpost_conv] = cdc.[rzpost_conv]
	,[rzglc] = cdc.[rzglc]
	,[rzglc_conv] = cdc.[rzglc_conv]
	,[rzaid] = cdc.[rzaid]
	,[rzaid_conv] = cdc.[rzaid_conv]
	,[rzctry] = cdc.[rzctry]
	,[rzfy] = cdc.[rzfy]
	,[rzfy_conv] = cdc.[rzfy_conv]
	,[rzpn] = cdc.[rzpn]
	,[rzco] = cdc.[rzco]
	,[rzco_conv] = cdc.[rzco_conv]
	,[rzicut] = cdc.[rzicut]
	,[rzicu] = cdc.[rzicu]
	,[rzdicj] = cdc.[rzdicj]
	,[rzdicj_conv] = cdc.[rzdicj_conv]
	,[rzpa8] = cdc.[rzpa8]
	,[rzrpid] = cdc.[rzrpid]
	,[rzrlin] = cdc.[rzrlin]
	,[rzpaap] = cdc.[rzpaap]
	,[rzpaap_conv] = cdc.[rzpaap_conv]
	,[rzadsc] = cdc.[rzadsc]
	,[rzadsc_conv] = cdc.[rzadsc_conv]
	,[rzadsa] = cdc.[rzadsa]
	,[rzadsa_conv] = cdc.[rzadsa_conv]
	,[rzaaaj] = cdc.[rzaaaj]
	,[rzaaaj_conv] = cdc.[rzaaaj_conv]
	,[rzecba] = cdc.[rzecba]
	,[rzecba_conv] = cdc.[rzecba_conv]
	,[rzdda] = cdc.[rzdda]
	,[rzdda_conv] = cdc.[rzdda_conv]
	,[rzbcrc] = cdc.[rzbcrc]
	,[rzbcrc_conv] = cdc.[rzbcrc_conv]
	,[rzcrrm] = cdc.[rzcrrm]
	,[rzcrcd] = cdc.[rzcrcd]
	,[rzcrcd_conv] = cdc.[rzcrcd_conv]
	,[rzcrr] = cdc.[rzcrr]
	,[rzpfap] = cdc.[rzpfap]
	,[rzpfap_conv] = cdc.[rzpfap_conv]
	,[rzcds] = cdc.[rzcds]
	,[rzcds_conv] = cdc.[rzcds_conv]
	,[rzcdsa] = cdc.[rzcdsa]
	,[rzcdsa_conv] = cdc.[rzcdsa_conv]
	,[rzfchg] = cdc.[rzfchg]
	,[rzfchg_conv] = cdc.[rzfchg_conv]
	,[rzecbf] = cdc.[rzecbf]
	,[rzecbf_conv] = cdc.[rzecbf_conv]
	,[rzfda] = cdc.[rzfda]
	,[rzfda_conv] = cdc.[rzfda_conv]
	,[rzagl] = cdc.[rzagl]
	,[rzagl_conv] = cdc.[rzagl_conv]
	,[rzaidt] = cdc.[rzaidt]
	,[rzaidt_conv] = cdc.[rzaidt_conv]
	,[rztcrc] = cdc.[rztcrc]
	,[rztcrc_conv] = cdc.[rztcrc_conv]
	,[rztaap] = cdc.[rztaap]
	,[rztaap_conv] = cdc.[rztaap_conv]
	,[rztada] = cdc.[rztada]
	,[rztada_conv] = cdc.[rztada_conv]
	,[rztaaj] = cdc.[rztaaj]
	,[rztaaj_conv] = cdc.[rztaaj_conv]
	,[rztcba] = cdc.[rztcba]
	,[rztcba_conv] = cdc.[rztcba_conv]
	,[rztda] = cdc.[rztda]
	,[rztda_conv] = cdc.[rztda_conv]
	,[rzacrr] = cdc.[rzacrr]
	,[rzacr1] = cdc.[rzacr1]
	,[rzacr2] = cdc.[rzacr2]
	,[rzacrm] = cdc.[rzacrm]
	,[rzacrm_conv] = cdc.[rzacrm_conv]
	,[rzagla] = cdc.[rzagla]
	,[rzagla_conv] = cdc.[rzagla_conv]
	,[rzaida] = cdc.[rzaida]
	,[rzaida_conv] = cdc.[rzaida_conv]
	,[rzaidd] = cdc.[rzaidd]
	,[rzaidd_conv] = cdc.[rzaidd_conv]
	,[rzrsco] = cdc.[rzrsco]
	,[rzaidw] = cdc.[rzaidw]
	,[rzaidw_conv] = cdc.[rzaidw_conv]
	,[rzecbr] = cdc.[rzecbr]
	,[rzglcc] = cdc.[rzglcc]
	,[rzglcc_conv] = cdc.[rzglcc_conv]
	,[rzaidc] = cdc.[rzaidc]
	,[rzaidc_conv] = cdc.[rzaidc_conv]
	,[rzddex] = cdc.[rzddex]
	,[rzdaid] = cdc.[rzdaid]
	,[rzdaid_conv] = cdc.[rzdaid_conv]
	,[rztyin] = cdc.[rztyin]
	,[rzutic] = cdc.[rzutic]
	,[rzuctf] = cdc.[rzuctf]
	,[rzuctf_conv] = cdc.[rzuctf_conv]
	,[rzaid2] = cdc.[rzaid2]
	,[rzaid2_conv] = cdc.[rzaid2_conv]
	,[rzam2] = cdc.[rzam2]
	,[rzmcu] = cdc.[rzmcu]
	,[rzmcu_conv] = cdc.[rzmcu_conv]
	,[rzsbl] = cdc.[rzsbl]
	,[rzsbl_conv] = cdc.[rzsbl_conv]
	,[rzsblt] = cdc.[rzsblt]
	,[rzpkco] = cdc.[rzpkco]
	,[rzpkco_conv] = cdc.[rzpkco_conv]
	,[rzpo] = cdc.[rzpo]
	,[rzpo_conv] = cdc.[rzpo_conv]
	,[rzpdct] = cdc.[rzpdct]
	,[rznumb] = cdc.[rznumb]
	,[rzunit] = cdc.[rzunit]
	,[rzunit_conv] = cdc.[rzunit_conv]
	,[rzmcu2] = cdc.[rzmcu2]
	,[rzmcu2_conv] = cdc.[rzmcu2_conv]
	,[rzrmk] = cdc.[rzrmk]
	,[rzrmk_conv] = cdc.[rzrmk_conv]
	,[rzfnlp] = cdc.[rzfnlp]
	,[rzfnlp_conv] = cdc.[rzfnlp_conv]
	,[rzalt6] = cdc.[rzalt6]
	,[rzalt6_conv] = cdc.[rzalt6_conv]
	,[rzodoc] = cdc.[rzodoc]
	,[rzodct] = cdc.[rzodct]
	,[rzokco] = cdc.[rzokco]
	,[rzokco_conv] = cdc.[rzokco_conv]
	,[rzosfx] = cdc.[rzosfx]
	,[rzosfx_conv] = cdc.[rzosfx_conv]
	,[rzgdoc] = cdc.[rzgdoc]
	,[rzgdct] = cdc.[rzgdct]
	,[rzgkco] = cdc.[rzgkco]
	,[rzgkco_conv] = cdc.[rzgkco_conv]
	,[rzgsfx] = cdc.[rzgsfx]
	,[rzgsfx_conv] = cdc.[rzgsfx_conv]
	,[rzdctg] = cdc.[rzdctg]
	,[rzdocg] = cdc.[rzdocg]
	,[rzkcog] = cdc.[rzkcog]
	,[rzkcog_conv] = cdc.[rzkcog_conv]
	,[rzctl] = cdc.[rzctl]
	,[rzctl_conv] = cdc.[rzctl_conv]
	,[rzsmtj] = cdc.[rzsmtj]
	,[rzsmtj_conv] = cdc.[rzsmtj_conv]
	,[rzvdgj] = cdc.[rzvdgj]
	,[rzvdgj_conv] = cdc.[rzvdgj_conv]
	,[rzvre] = cdc.[rzvre]
	,[rznfvd] = cdc.[rznfvd]
	,[rznfvd_conv] = cdc.[rznfvd_conv]
	,[rzhcrr] = cdc.[rzhcrr]
	,[rzistc] = cdc.[rzistc]
	,[rzistc_conv] = cdc.[rzistc_conv]
	,[rzlfcj] = cdc.[rzlfcj]
	,[rzlfcj_conv] = cdc.[rzlfcj_conv]
	,[rzpdlt] = cdc.[rzpdlt]
	,[rzddj] = cdc.[rzddj]
	,[rzddj_conv] = cdc.[rzddj_conv]
	,[rzddnj] = cdc.[rzddnj]
	,[rzddnj_conv] = cdc.[rzddnj_conv]
	,[rzidgj] = cdc.[rzidgj]
	,[rzidgj_conv] = cdc.[rzidgj_conv]
	,[rzddst] = cdc.[rzddst]
	,[rzvr01] = cdc.[rzvr01]
	,[rzvr01_conv] = cdc.[rzvr01_conv]
	,[rzrfid] = cdc.[rzrfid]
	,[rzridc] = cdc.[rzridc]
	,[rzridc_conv] = cdc.[rzridc_conv]
	,[rzprgf] = cdc.[rzprgf]
	,[rzprgf_conv] = cdc.[rzprgf_conv]
	,[rzgfl1] = cdc.[rzgfl1]
	,[rzgfl1_conv] = cdc.[rzgfl1_conv]
	,[rzrnid] = cdc.[rzrnid]
	,[rztorg] = cdc.[rztorg]
	,[rztorg_conv] = cdc.[rztorg_conv]
	,[rzuser] = cdc.[rzuser]
	,[rzuser_conv] = cdc.[rzuser_conv]
	,[rzpid] = cdc.[rzpid]
	,[rzpid_conv] = cdc.[rzpid_conv]
	,[rzupmj] = cdc.[rzupmj]
	,[rzupmj_conv] = cdc.[rzupmj_conv]
	,[rzupmt] = cdc.[rzupmt]
	,[rzjobn] = cdc.[rzjobn]
	,[rzjobn_conv] = cdc.[rzjobn_conv]
	,[rzurab] = cdc.[rzurab]
	,[rzurab_conv] = cdc.[rzurab_conv]
	,[rzurat] = cdc.[rzurat]
	,[rzurat_conv] = cdc.[rzurat_conv]
	,[rzurc1] = cdc.[rzurc1]
	,[rzurc1_conv] = cdc.[rzurc1_conv]
	,[rzurdt] = cdc.[rzurdt]
	,[rzurdt_conv] = cdc.[rzurdt_conv]
	,[rzurrf] = cdc.[rzurrf]
	,[rzurrf_conv] = cdc.[rzurrf_conv]
	,[rzshpn] = cdc.[rzshpn]
	,[rzomod] = cdc.[rzomod]
	,[rzdoco] = cdc.[rzdoco]
	,[rzrasi] = cdc.[rzrasi]
	,[rzrasi_conv] = cdc.[rzrasi_conv]
	,[rzsfxo] = cdc.[rzsfxo]
	,[rzsfxo_conv] = cdc.[rzsfxo_conv]
	,[rzkcoo] = cdc.[rzkcoo]
	,[rzkcoo_conv] = cdc.[rzkcoo_conv]
	,[rzdcto] = cdc.[rzdcto]
	,[rzramt] = cdc.[rzramt]
	,[rzramt_conv] = cdc.[rzramt_conv]
	,[rzlrfl] = cdc.[rzlrfl]
	,[rzlrfl_conv] = cdc.[rzlrfl_conv]
	,[rzgfl2] = cdc.[rzgfl2]
	,[rzgfl2_conv] = cdc.[rzgfl2_conv]
	,[rzdrco] = cdc.[rzdrco]
	,[rznettcid] = cdc.[rznettcid]
	,[rznettcid_conv] = cdc.[rznettcid_conv]
	,[rznetdoc] = cdc.[rznetdoc]
	,[rznetdoc_conv] = cdc.[rznetdoc_conv]
	,[rznetrc5] = cdc.[rznetrc5]
	,[rznetrc5_conv] = cdc.[rznetrc5_conv]
	,[rzadgj] = cdc.[rzadgj]
	,[rzadgj_conv] = cdc.[rzadgj_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f03b14_cdc cdc
WHERE
    rep_jde_qat.f03b14_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f03b14_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f03b14_new AF:{{ task_instance_key_str }}' ) 
