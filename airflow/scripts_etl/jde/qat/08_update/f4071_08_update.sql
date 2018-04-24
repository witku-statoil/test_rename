-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f4071_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[atast] = cdc.[atast]
	,[atprgr] = cdc.[atprgr]
	,[atcpgp] = cdc.[atcpgp]
	,[atsdgr] = cdc.[atsdgr]
	,[atprfr] = cdc.[atprfr]
	,[atlbt] = cdc.[atlbt]
	,[atglc] = cdc.[atglc]
	,[atglc_conv] = cdc.[atglc_conv]
	,[atsbif] = cdc.[atsbif]
	,[atacnt] = cdc.[atacnt]
	,[atlnty] = cdc.[atlnty]
	,[atlnty_conv] = cdc.[atlnty_conv]
	,[atmded] = cdc.[atmded]
	,[atmded_conv] = cdc.[atmded_conv]
	,[atabas] = cdc.[atabas]
	,[atabas_conv] = cdc.[atabas_conv]
	,[atolvl] = cdc.[atolvl]
	,[attxb] = cdc.[attxb]
	,[atpa01] = cdc.[atpa01]
	,[atpa02] = cdc.[atpa02]
	,[atpa02_conv] = cdc.[atpa02_conv]
	,[atpa03] = cdc.[atpa03]
	,[atpa03_conv] = cdc.[atpa03_conv]
	,[atpa04] = cdc.[atpa04]
	,[atpa05] = cdc.[atpa05]
	,[atpa05_conv] = cdc.[atpa05_conv]
	,[aturcd] = cdc.[aturcd]
	,[aturcd_conv] = cdc.[aturcd_conv]
	,[aturdt] = cdc.[aturdt]
	,[aturdt_conv] = cdc.[aturdt_conv]
	,[aturat] = cdc.[aturat]
	,[aturat_conv] = cdc.[aturat_conv]
	,[aturab] = cdc.[aturab]
	,[aturab_conv] = cdc.[aturab_conv]
	,[aturrf] = cdc.[aturrf]
	,[aturrf_conv] = cdc.[aturrf_conv]
	,[atenbm] = cdc.[atenbm]
	,[atenbm_conv] = cdc.[atenbm_conv]
	,[atsrflag] = cdc.[atsrflag]
	,[atsrflag_conv] = cdc.[atsrflag_conv]
	,[atusadj] = cdc.[atusadj]
	,[atusadj_conv] = cdc.[atusadj_conv]
	,[atatier] = cdc.[atatier]
	,[atbtier] = cdc.[atbtier]
	,[atbnad] = cdc.[atbnad]
	,[ataprp1] = cdc.[ataprp1]
	,[ataprp2] = cdc.[ataprp2]
	,[ataprp3] = cdc.[ataprp3]
	,[ataprp4] = cdc.[ataprp4]
	,[ataprp5] = cdc.[ataprp5]
	,[ataprp6] = cdc.[ataprp6]
	,[atadjgrp] = cdc.[atadjgrp]
	,[atmeadj] = cdc.[atmeadj]
	,[atmeadj_conv] = cdc.[atmeadj_conv]
	,[atpdcl] = cdc.[atpdcl]
	,[atuser] = cdc.[atuser]
	,[atuser_conv] = cdc.[atuser_conv]
	,[atpid] = cdc.[atpid]
	,[atpid_conv] = cdc.[atpid_conv]
	,[atjobn] = cdc.[atjobn]
	,[atjobn_conv] = cdc.[atjobn_conv]
	,[atupmj] = cdc.[atupmj]
	,[atupmj_conv] = cdc.[atupmj_conv]
	,[attday] = cdc.[attday]
	,[atdidp] = cdc.[atdidp]
	,[atdidp_conv] = cdc.[atdidp_conv]
	,[atpmtn] = cdc.[atpmtn]
	,[atpmtn_conv] = cdc.[atpmtn_conv]
	,[atphst] = cdc.[atphst]
	,[atphst_conv] = cdc.[atphst_conv]
	,[atpa06] = cdc.[atpa06]
	,[atpa06_conv] = cdc.[atpa06_conv]
	,[atpa07] = cdc.[atpa07]
	,[atpa07_conv] = cdc.[atpa07_conv]
	,[atpa08] = cdc.[atpa08]
	,[atpa08_conv] = cdc.[atpa08_conv]
	,[atpa09] = cdc.[atpa09]
	,[atpa09_conv] = cdc.[atpa09_conv]
	,[atpa10] = cdc.[atpa10]
	,[atpa10_conv] = cdc.[atpa10_conv]
	,[atefcn] = cdc.[atefcn]
	,[atefcn_conv] = cdc.[atefcn_conv]
	,[ataptype] = cdc.[ataptype]
	,[atmoadj] = cdc.[atmoadj]
	,[atmoadj_conv] = cdc.[atmoadj_conv]
	,[atplgrp] = cdc.[atplgrp]
	,[atexcpl] = cdc.[atexcpl]
	,[atexcpl_conv] = cdc.[atexcpl_conv]
	,[atupmx] = cdc.[atupmx]
	,[atupmx_conv] = cdc.[atupmx_conv]
	,[atmnmxaj] = cdc.[atmnmxaj]
	,[atmnmxrl] = cdc.[atmnmxrl]
	,[attstrsnm] = cdc.[attstrsnm]
	,[attstrsnm_conv] = cdc.[attstrsnm_conv]
	,[atadjqty] = cdc.[atadjqty]
	,[atadjqty_conv] = cdc.[atadjqty_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f4071_cdc cdc
WHERE
    rep_jde_qat.f4071_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f4071_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f4071_new AF:{{ task_instance_key_str }}' ) 
