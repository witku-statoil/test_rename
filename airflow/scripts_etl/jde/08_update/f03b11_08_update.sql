-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f03b11_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[rpdoc] = cdc.[rpdoc]
	,[rpdct] = cdc.[rpdct]
	,[rpkco] = cdc.[rpkco]
	,[rpkco_conv] = cdc.[rpkco_conv]
	,[rpsfx] = cdc.[rpsfx]
	,[rpsfx_conv] = cdc.[rpsfx_conv]
	,[rpan8] = cdc.[rpan8]
	,[rpdgj] = cdc.[rpdgj]
	,[rpdgj_conv] = cdc.[rpdgj_conv]
	,[rpdivj] = cdc.[rpdivj]
	,[rpdivj_conv] = cdc.[rpdivj_conv]
	,[rpicut] = cdc.[rpicut]
	,[rpicu] = cdc.[rpicu]
	,[rpdicj] = cdc.[rpdicj]
	,[rpdicj_conv] = cdc.[rpdicj_conv]
	,[rpfy] = cdc.[rpfy]
	,[rpfy_conv] = cdc.[rpfy_conv]
	,[rpctry] = cdc.[rpctry]
	,[rppn] = cdc.[rppn]
	,[rpco] = cdc.[rpco]
	,[rpco_conv] = cdc.[rpco_conv]
	,[rpglc] = cdc.[rpglc]
	,[rpglc_conv] = cdc.[rpglc_conv]
	,[rpaid] = cdc.[rpaid]
	,[rpaid_conv] = cdc.[rpaid_conv]
	,[rppa8] = cdc.[rppa8]
	,[rpan8j] = cdc.[rpan8j]
	,[rppyr] = cdc.[rppyr]
	,[rppost] = cdc.[rppost]
	,[rppost_conv] = cdc.[rppost_conv]
	,[rpistr] = cdc.[rpistr]
	,[rpistr_conv] = cdc.[rpistr_conv]
	,[rpbalj] = cdc.[rpbalj]
	,[rppst] = cdc.[rppst]
	,[rpag] = cdc.[rpag]
	,[rpag_conv] = cdc.[rpag_conv]
	,[rpaap] = cdc.[rpaap]
	,[rpaap_conv] = cdc.[rpaap_conv]
	,[rpadsc] = cdc.[rpadsc]
	,[rpadsc_conv] = cdc.[rpadsc_conv]
	,[rpadsa] = cdc.[rpadsa]
	,[rpadsa_conv] = cdc.[rpadsa_conv]
	,[rpatxa] = cdc.[rpatxa]
	,[rpatxa_conv] = cdc.[rpatxa_conv]
	,[rpatxn] = cdc.[rpatxn]
	,[rpatxn_conv] = cdc.[rpatxn_conv]
	,[rpstam] = cdc.[rpstam]
	,[rpstam_conv] = cdc.[rpstam_conv]
	,[rpbcrc] = cdc.[rpbcrc]
	,[rpbcrc_conv] = cdc.[rpbcrc_conv]
	,[rpcrrm] = cdc.[rpcrrm]
	,[rpcrcd] = cdc.[rpcrcd]
	,[rpcrcd_conv] = cdc.[rpcrcd_conv]
	,[rpcrr] = cdc.[rpcrr]
	,[rpdmcd] = cdc.[rpdmcd]
	,[rpdmcd_conv] = cdc.[rpdmcd_conv]
	,[rpacr] = cdc.[rpacr]
	,[rpacr_conv] = cdc.[rpacr_conv]
	,[rpfap] = cdc.[rpfap]
	,[rpfap_conv] = cdc.[rpfap_conv]
	,[rpcds] = cdc.[rpcds]
	,[rpcds_conv] = cdc.[rpcds_conv]
	,[rpcdsa] = cdc.[rpcdsa]
	,[rpcdsa_conv] = cdc.[rpcdsa_conv]
	,[rpctxa] = cdc.[rpctxa]
	,[rpctxa_conv] = cdc.[rpctxa_conv]
	,[rpctxn] = cdc.[rpctxn]
	,[rpctxn_conv] = cdc.[rpctxn_conv]
	,[rpctam] = cdc.[rpctam]
	,[rpctam_conv] = cdc.[rpctam_conv]
	,[rptxa1] = cdc.[rptxa1]
	,[rptxa1_conv] = cdc.[rptxa1_conv]
	,[rpexr1] = cdc.[rpexr1]
	,[rpdsvj] = cdc.[rpdsvj]
	,[rpdsvj_conv] = cdc.[rpdsvj_conv]
	,[rpglba] = cdc.[rpglba]
	,[rpglba_conv] = cdc.[rpglba_conv]
	,[rpam] = cdc.[rpam]
	,[rpaid2] = cdc.[rpaid2]
	,[rpaid2_conv] = cdc.[rpaid2_conv]
	,[rpam2] = cdc.[rpam2]
	,[rpmcu] = cdc.[rpmcu]
	,[rpmcu_conv] = cdc.[rpmcu_conv]
	,[rpobj] = cdc.[rpobj]
	,[rpobj_conv] = cdc.[rpobj_conv]
	,[rpsub] = cdc.[rpsub]
	,[rpsub_conv] = cdc.[rpsub_conv]
	,[rpsblt] = cdc.[rpsblt]
	,[rpsbl] = cdc.[rpsbl]
	,[rpsbl_conv] = cdc.[rpsbl_conv]
	,[rpptc] = cdc.[rpptc]
	,[rpptc_conv] = cdc.[rpptc_conv]
	,[rpddj] = cdc.[rpddj]
	,[rpddj_conv] = cdc.[rpddj_conv]
	,[rpddnj] = cdc.[rpddnj]
	,[rpddnj_conv] = cdc.[rpddnj_conv]
	,[rprddj] = cdc.[rprddj]
	,[rprddj_conv] = cdc.[rprddj_conv]
	,[rprdsj] = cdc.[rprdsj]
	,[rprdsj_conv] = cdc.[rprdsj_conv]
	,[rplfcj] = cdc.[rplfcj]
	,[rplfcj_conv] = cdc.[rplfcj_conv]
	,[rpsmtj] = cdc.[rpsmtj]
	,[rpsmtj_conv] = cdc.[rpsmtj_conv]
	,[rpnbrr] = cdc.[rpnbrr]
	,[rprdrl] = cdc.[rprdrl]
	,[rprdrl_conv] = cdc.[rprdrl_conv]
	,[rprmds] = cdc.[rprmds]
	,[rpcoll] = cdc.[rpcoll]
	,[rpcorc] = cdc.[rpcorc]
	,[rpafc] = cdc.[rpafc]
	,[rpdnlt] = cdc.[rpdnlt]
	,[rpdnlt_conv] = cdc.[rpdnlt_conv]
	,[rprsco] = cdc.[rprsco]
	,[rpodoc] = cdc.[rpodoc]
	,[rpodct] = cdc.[rpodct]
	,[rpokco] = cdc.[rpokco]
	,[rpokco_conv] = cdc.[rpokco_conv]
	,[rposfx] = cdc.[rposfx]
	,[rposfx_conv] = cdc.[rposfx_conv]
	,[rpvinv] = cdc.[rpvinv]
	,[rpvinv_conv] = cdc.[rpvinv_conv]
	,[rppo] = cdc.[rppo]
	,[rppo_conv] = cdc.[rppo_conv]
	,[rppdct] = cdc.[rppdct]
	,[rppkco] = cdc.[rppkco]
	,[rppkco_conv] = cdc.[rppkco_conv]
	,[rpdcto] = cdc.[rpdcto]
	,[rplnid] = cdc.[rplnid]
	,[rplnid_conv] = cdc.[rplnid_conv]
	,[rpsdoc] = cdc.[rpsdoc]
	,[rpsdct] = cdc.[rpsdct]
	,[rpskco] = cdc.[rpskco]
	,[rpskco_conv] = cdc.[rpskco_conv]
	,[rpsfxo] = cdc.[rpsfxo]
	,[rpsfxo_conv] = cdc.[rpsfxo_conv]
	,[rpvldt] = cdc.[rpvldt]
	,[rpvldt_conv] = cdc.[rpvldt_conv]
	,[rpcmc1] = cdc.[rpcmc1]
	,[rpcmc1_conv] = cdc.[rpcmc1_conv]
	,[rpvr01] = cdc.[rpvr01]
	,[rpvr01_conv] = cdc.[rpvr01_conv]
	,[rpunit] = cdc.[rpunit]
	,[rpunit_conv] = cdc.[rpunit_conv]
	,[rpmcu2] = cdc.[rpmcu2]
	,[rpmcu2_conv] = cdc.[rpmcu2_conv]
	,[rprmk] = cdc.[rprmk]
	,[rprmk_conv] = cdc.[rprmk_conv]
	,[rpalph] = cdc.[rpalph]
	,[rpalph_conv] = cdc.[rpalph_conv]
	,[rprf] = cdc.[rprf]
	,[rpdrf] = cdc.[rpdrf]
	,[rpctl] = cdc.[rpctl]
	,[rpctl_conv] = cdc.[rpctl_conv]
	,[rpfnlp] = cdc.[rpfnlp]
	,[rpfnlp_conv] = cdc.[rpfnlp_conv]
	,[rpitm] = cdc.[rpitm]
	,[rpu] = cdc.[rpu]
	,[rpu_conv] = cdc.[rpu_conv]
	,[rpum] = cdc.[rpum]
	,[rpalt6] = cdc.[rpalt6]
	,[rpalt6_conv] = cdc.[rpalt6_conv]
	,[rpryin] = cdc.[rpryin]
	,[rpvdgj] = cdc.[rpvdgj]
	,[rpvdgj_conv] = cdc.[rpvdgj_conv]
	,[rpvod] = cdc.[rpvod]
	,[rpvod_conv] = cdc.[rpvod_conv]
	,[rprp1] = cdc.[rprp1]
	,[rprp1_conv] = cdc.[rprp1_conv]
	,[rprp2] = cdc.[rprp2]
	,[rprp2_conv] = cdc.[rprp2_conv]
	,[rprp3] = cdc.[rprp3]
	,[rprp3_conv] = cdc.[rprp3_conv]
	,[rpar01] = cdc.[rpar01]
	,[rpar02] = cdc.[rpar02]
	,[rpar03] = cdc.[rpar03]
	,[rpar04] = cdc.[rpar04]
	,[rpar05] = cdc.[rpar05]
	,[rpar06] = cdc.[rpar06]
	,[rpar07] = cdc.[rpar07]
	,[rpar08] = cdc.[rpar08]
	,[rpar09] = cdc.[rpar09]
	,[rpar10] = cdc.[rpar10]
	,[rpistc] = cdc.[rpistc]
	,[rpistc_conv] = cdc.[rpistc_conv]
	,[rppyid] = cdc.[rppyid]
	,[rpurc1] = cdc.[rpurc1]
	,[rpurc1_conv] = cdc.[rpurc1_conv]
	,[rpurdt] = cdc.[rpurdt]
	,[rpurdt_conv] = cdc.[rpurdt_conv]
	,[rpurat] = cdc.[rpurat]
	,[rpurat_conv] = cdc.[rpurat_conv]
	,[rpurab] = cdc.[rpurab]
	,[rpurab_conv] = cdc.[rpurab_conv]
	,[rpurrf] = cdc.[rpurrf]
	,[rpurrf_conv] = cdc.[rpurrf_conv]
	,[rprnid] = cdc.[rprnid]
	,[rpppdi] = cdc.[rpppdi]
	,[rpppdi_conv] = cdc.[rpppdi_conv]
	,[rptorg] = cdc.[rptorg]
	,[rptorg_conv] = cdc.[rptorg_conv]
	,[rpuser] = cdc.[rpuser]
	,[rpuser_conv] = cdc.[rpuser_conv]
	,[rpjcl] = cdc.[rpjcl]
	,[rpjcl_conv] = cdc.[rpjcl_conv]
	,[rppid] = cdc.[rppid]
	,[rppid_conv] = cdc.[rppid_conv]
	,[rpupmj] = cdc.[rpupmj]
	,[rpupmj_conv] = cdc.[rpupmj_conv]
	,[rpupmt] = cdc.[rpupmt]
	,[rpddex] = cdc.[rpddex]
	,[rpjobn] = cdc.[rpjobn]
	,[rpjobn_conv] = cdc.[rpjobn_conv]
	,[rphcrr] = cdc.[rphcrr]
	,[rphdgj] = cdc.[rphdgj]
	,[rphdgj_conv] = cdc.[rphdgj_conv]
	,[rpshpn] = cdc.[rpshpn]
	,[rpdtxs] = cdc.[rpdtxs]
	,[rpdtxs_conv] = cdc.[rpdtxs_conv]
	,[rpomod] = cdc.[rpomod]
	,[rpclmg] = cdc.[rpclmg]
	,[rpcmgr] = cdc.[rpcmgr]
	,[rpatad] = cdc.[rpatad]
	,[rpatad_conv] = cdc.[rpatad_conv]
	,[rpctad] = cdc.[rpctad]
	,[rpctad_conv] = cdc.[rpctad_conv]
	,[rpnrta] = cdc.[rpnrta]
	,[rpnrta_conv] = cdc.[rpnrta_conv]
	,[rpfnrt] = cdc.[rpfnrt]
	,[rpfnrt_conv] = cdc.[rpfnrt_conv]
	,[rpprgf] = cdc.[rpprgf]
	,[rpprgf_conv] = cdc.[rpprgf_conv]
	,[rpgfl1] = cdc.[rpgfl1]
	,[rpgfl1_conv] = cdc.[rpgfl1_conv]
	,[rpgfl2] = cdc.[rpgfl2]
	,[rpgfl2_conv] = cdc.[rpgfl2_conv]
	,[rpdoco] = cdc.[rpdoco]
	,[rpkcoo] = cdc.[rpkcoo]
	,[rpkcoo_conv] = cdc.[rpkcoo_conv]
	,[rpsotf] = cdc.[rpsotf]
	,[rpsotf_conv] = cdc.[rpsotf_conv]
	,[rpdtpb] = cdc.[rpdtpb]
	,[rpdtpb_conv] = cdc.[rpdtpb_conv]
	,[rperdj] = cdc.[rperdj]
	,[rperdj_conv] = cdc.[rperdj_conv]
	,[rppwpg] = cdc.[rppwpg]
	,[rppwpg_conv] = cdc.[rppwpg_conv]
	,[rpnettcid] = cdc.[rpnettcid]
	,[rpnettcid_conv] = cdc.[rpnettcid_conv]
	,[rpnetdoc] = cdc.[rpnetdoc]
	,[rpnetdoc_conv] = cdc.[rpnetdoc_conv]
	,[rpnetrc5] = cdc.[rpnetrc5]
	,[rpnetrc5_conv] = cdc.[rpnetrc5_conv]
	,[rpnetst] = cdc.[rpnetst]
	,[rpajcl] = cdc.[rpajcl]
	,[rpajcl_conv] = cdc.[rpajcl_conv]
	,[rprmr1] = cdc.[rprmr1]
	,[rprmr1_conv] = cdc.[rprmr1_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f03b11_cdc cdc
WHERE
    rep_jde.f03b11_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f03b11_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f03b11_new AF:{{ task_instance_key_str }}' ) 
