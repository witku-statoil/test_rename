-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f0006_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[mcmcu] = cdc.[mcmcu]
	,[mcmcu_conv] = cdc.[mcmcu_conv]
	,[mcstyl] = cdc.[mcstyl]
	,[mcdc] = cdc.[mcdc]
	,[mcdc_conv] = cdc.[mcdc_conv]
	,[mcldm] = cdc.[mcldm]
	,[mcco] = cdc.[mcco]
	,[mcco_conv] = cdc.[mcco_conv]
	,[mcan8] = cdc.[mcan8]
	,[mcan8o] = cdc.[mcan8o]
	,[mccnty] = cdc.[mccnty]
	,[mcadds] = cdc.[mcadds]
	,[mcfmod] = cdc.[mcfmod]
	,[mcdl01] = cdc.[mcdl01]
	,[mcdl01_conv] = cdc.[mcdl01_conv]
	,[mcdl02] = cdc.[mcdl02]
	,[mcdl02_conv] = cdc.[mcdl02_conv]
	,[mcdl03] = cdc.[mcdl03]
	,[mcdl03_conv] = cdc.[mcdl03_conv]
	,[mcdl04] = cdc.[mcdl04]
	,[mcdl04_conv] = cdc.[mcdl04_conv]
	,[mcrp01] = cdc.[mcrp01]
	,[mcrp02] = cdc.[mcrp02]
	,[mcrp03] = cdc.[mcrp03]
	,[mcrp04] = cdc.[mcrp04]
	,[mcrp05] = cdc.[mcrp05]
	,[mcrp06] = cdc.[mcrp06]
	,[mcrp07] = cdc.[mcrp07]
	,[mcrp08] = cdc.[mcrp08]
	,[mcrp09] = cdc.[mcrp09]
	,[mcrp10] = cdc.[mcrp10]
	,[mcrp11] = cdc.[mcrp11]
	,[mcrp12] = cdc.[mcrp12]
	,[mcrp13] = cdc.[mcrp13]
	,[mcrp14] = cdc.[mcrp14]
	,[mcrp15] = cdc.[mcrp15]
	,[mcrp16] = cdc.[mcrp16]
	,[mcrp17] = cdc.[mcrp17]
	,[mcrp18] = cdc.[mcrp18]
	,[mcrp19] = cdc.[mcrp19]
	,[mcrp20] = cdc.[mcrp20]
	,[mcrp21] = cdc.[mcrp21]
	,[mcrp22] = cdc.[mcrp22]
	,[mcrp23] = cdc.[mcrp23]
	,[mcrp24] = cdc.[mcrp24]
	,[mcrp25] = cdc.[mcrp25]
	,[mcrp26] = cdc.[mcrp26]
	,[mcrp27] = cdc.[mcrp27]
	,[mcrp28] = cdc.[mcrp28]
	,[mcrp29] = cdc.[mcrp29]
	,[mcrp30] = cdc.[mcrp30]
	,[mcta] = cdc.[mcta]
	,[mcta_conv] = cdc.[mcta_conv]
	,[mctxjs] = cdc.[mctxjs]
	,[mctxa1] = cdc.[mctxa1]
	,[mctxa1_conv] = cdc.[mctxa1_conv]
	,[mcexr1] = cdc.[mcexr1]
	,[mctc01] = cdc.[mctc01]
	,[mctc01_conv] = cdc.[mctc01_conv]
	,[mctc02] = cdc.[mctc02]
	,[mctc02_conv] = cdc.[mctc02_conv]
	,[mctc03] = cdc.[mctc03]
	,[mctc03_conv] = cdc.[mctc03_conv]
	,[mctc04] = cdc.[mctc04]
	,[mctc04_conv] = cdc.[mctc04_conv]
	,[mctc05] = cdc.[mctc05]
	,[mctc05_conv] = cdc.[mctc05_conv]
	,[mctc06] = cdc.[mctc06]
	,[mctc06_conv] = cdc.[mctc06_conv]
	,[mctc07] = cdc.[mctc07]
	,[mctc07_conv] = cdc.[mctc07_conv]
	,[mctc08] = cdc.[mctc08]
	,[mctc08_conv] = cdc.[mctc08_conv]
	,[mctc09] = cdc.[mctc09]
	,[mctc09_conv] = cdc.[mctc09_conv]
	,[mctc10] = cdc.[mctc10]
	,[mctc10_conv] = cdc.[mctc10_conv]
	,[mcnd01] = cdc.[mcnd01]
	,[mcnd02] = cdc.[mcnd02]
	,[mcnd03] = cdc.[mcnd03]
	,[mcnd04] = cdc.[mcnd04]
	,[mcnd05] = cdc.[mcnd05]
	,[mcnd06] = cdc.[mcnd06]
	,[mcnd07] = cdc.[mcnd07]
	,[mcnd08] = cdc.[mcnd08]
	,[mcnd09] = cdc.[mcnd09]
	,[mcnd10] = cdc.[mcnd10]
	,[mccc01] = cdc.[mccc01]
	,[mccc01_conv] = cdc.[mccc01_conv]
	,[mccc02] = cdc.[mccc02]
	,[mccc02_conv] = cdc.[mccc02_conv]
	,[mccc03] = cdc.[mccc03]
	,[mccc03_conv] = cdc.[mccc03_conv]
	,[mccc04] = cdc.[mccc04]
	,[mccc04_conv] = cdc.[mccc04_conv]
	,[mccc05] = cdc.[mccc05]
	,[mccc05_conv] = cdc.[mccc05_conv]
	,[mccc06] = cdc.[mccc06]
	,[mccc06_conv] = cdc.[mccc06_conv]
	,[mccc07] = cdc.[mccc07]
	,[mccc07_conv] = cdc.[mccc07_conv]
	,[mccc08] = cdc.[mccc08]
	,[mccc08_conv] = cdc.[mccc08_conv]
	,[mccc09] = cdc.[mccc09]
	,[mccc09_conv] = cdc.[mccc09_conv]
	,[mccc10] = cdc.[mccc10]
	,[mccc10_conv] = cdc.[mccc10_conv]
	,[mcpecc] = cdc.[mcpecc]
	,[mcals] = cdc.[mcals]
	,[mciss] = cdc.[mciss]
	,[mcglba] = cdc.[mcglba]
	,[mcglba_conv] = cdc.[mcglba_conv]
	,[mcalcl] = cdc.[mcalcl]
	,[mcalcl_conv] = cdc.[mcalcl_conv]
	,[mclmth] = cdc.[mclmth]
	,[mclf] = cdc.[mclf]
	,[mclf_conv] = cdc.[mclf_conv]
	,[mcobj1] = cdc.[mcobj1]
	,[mcobj1_conv] = cdc.[mcobj1_conv]
	,[mcobj2] = cdc.[mcobj2]
	,[mcobj2_conv] = cdc.[mcobj2_conv]
	,[mcobj3] = cdc.[mcobj3]
	,[mcobj3_conv] = cdc.[mcobj3_conv]
	,[mcsub1] = cdc.[mcsub1]
	,[mcsub1_conv] = cdc.[mcsub1_conv]
	,[mctou] = cdc.[mctou]
	,[mctou_conv] = cdc.[mctou_conv]
	,[mcsbli] = cdc.[mcsbli]
	,[mcanpa] = cdc.[mcanpa]
	,[mcct] = cdc.[mcct]
	,[mccert] = cdc.[mccert]
	,[mcmcus] = cdc.[mcmcus]
	,[mcmcus_conv] = cdc.[mcmcus_conv]
	,[mcbtyp] = cdc.[mcbtyp]
	,[mcbtyp_conv] = cdc.[mcbtyp_conv]
	,[mcpc] = cdc.[mcpc]
	,[mcpca] = cdc.[mcpca]
	,[mcpca_conv] = cdc.[mcpca_conv]
	,[mcpcc] = cdc.[mcpcc]
	,[mcpcc_conv] = cdc.[mcpcc_conv]
	,[mcinta] = cdc.[mcinta]
	,[mcinta_conv] = cdc.[mcinta_conv]
	,[mcintl] = cdc.[mcintl]
	,[mcintl_conv] = cdc.[mcintl_conv]
	,[mcd1j] = cdc.[mcd1j]
	,[mcd1j_conv] = cdc.[mcd1j_conv]
	,[mcd2j] = cdc.[mcd2j]
	,[mcd2j_conv] = cdc.[mcd2j_conv]
	,[mcd3j] = cdc.[mcd3j]
	,[mcd3j_conv] = cdc.[mcd3j_conv]
	,[mcd4j] = cdc.[mcd4j]
	,[mcd4j_conv] = cdc.[mcd4j_conv]
	,[mcd5j] = cdc.[mcd5j]
	,[mcd5j_conv] = cdc.[mcd5j_conv]
	,[mcd6j] = cdc.[mcd6j]
	,[mcd6j_conv] = cdc.[mcd6j_conv]
	,[mcfpdj] = cdc.[mcfpdj]
	,[mcfpdj_conv] = cdc.[mcfpdj_conv]
	,[mccac] = cdc.[mccac]
	,[mccac_conv] = cdc.[mccac_conv]
	,[mcpac] = cdc.[mcpac]
	,[mcpac_conv] = cdc.[mcpac_conv]
	,[mceeo] = cdc.[mceeo]
	,[mcerc] = cdc.[mcerc]
	,[mcuser] = cdc.[mcuser]
	,[mcuser_conv] = cdc.[mcuser_conv]
	,[mcpid] = cdc.[mcpid]
	,[mcpid_conv] = cdc.[mcpid_conv]
	,[mcupmj] = cdc.[mcupmj]
	,[mcupmj_conv] = cdc.[mcupmj_conv]
	,[mcjobn] = cdc.[mcjobn]
	,[mcjobn_conv] = cdc.[mcjobn_conv]
	,[mcupmt] = cdc.[mcupmt]
	,[mcbptp] = cdc.[mcbptp]
	,[mcbptp_conv] = cdc.[mcbptp_conv]
	,[mcapsb] = cdc.[mcapsb]
	,[mcapsb_conv] = cdc.[mcapsb_conv]
	,[mctsbu] = cdc.[mctsbu]
	,[mctsbu_conv] = cdc.[mctsbu_conv]
	,[mcrp31] = cdc.[mcrp31]
	,[mcrp32] = cdc.[mcrp32]
	,[mcrp33] = cdc.[mcrp33]
	,[mcrp34] = cdc.[mcrp34]
	,[mcrp35] = cdc.[mcrp35]
	,[mcrp36] = cdc.[mcrp36]
	,[mcrp37] = cdc.[mcrp37]
	,[mcrp38] = cdc.[mcrp38]
	,[mcrp39] = cdc.[mcrp39]
	,[mcrp40] = cdc.[mcrp40]
	,[mcrp41] = cdc.[mcrp41]
	,[mcrp42] = cdc.[mcrp42]
	,[mcrp43] = cdc.[mcrp43]
	,[mcrp44] = cdc.[mcrp44]
	,[mcrp45] = cdc.[mcrp45]
	,[mcrp46] = cdc.[mcrp46]
	,[mcrp47] = cdc.[mcrp47]
	,[mcrp48] = cdc.[mcrp48]
	,[mcrp49] = cdc.[mcrp49]
	,[mcrp50] = cdc.[mcrp50]
	,[mcan8gca1] = cdc.[mcan8gca1]
	,[mcan8gca2] = cdc.[mcan8gca2]
	,[mcan8gca3] = cdc.[mcan8gca3]
	,[mcan8gca4] = cdc.[mcan8gca4]
	,[mcan8gca5] = cdc.[mcan8gca5]
	,[mcrmcu1] = cdc.[mcrmcu1]
	,[mcrmcu1_conv] = cdc.[mcrmcu1_conv]
	,[mcdoco] = cdc.[mcdoco]
	,[mcpctn] = cdc.[mcpctn]
	,[mcclnu] = cdc.[mcclnu]
	,[mcbuca] = cdc.[mcbuca]
	,[mcadjent] = cdc.[mcadjent]
	,[mcadjent_conv] = cdc.[mcadjent_conv]
	,[mcuafl] = cdc.[mcuafl]
	,[mcuafl_conv] = cdc.[mcuafl_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f0006_cdc cdc
WHERE
    rep_jde.f0006_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f0006_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f0006_new AF:{{ task_instance_key_str }}' ) 