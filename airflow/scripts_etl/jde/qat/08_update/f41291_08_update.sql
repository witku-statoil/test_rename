-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f41291_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[igprp5] = cdc.[igprp5]
	,[igitm] = cdc.[igitm]
	,[iglitm] = cdc.[iglitm]
	,[iglitm_conv] = cdc.[iglitm_conv]
	,[igaitm] = cdc.[igaitm]
	,[igaitm_conv] = cdc.[igaitm_conv]
	,[igmcu] = cdc.[igmcu]
	,[igmcu_conv] = cdc.[igmcu_conv]
	,[iglocn] = cdc.[iglocn]
	,[iglocn_conv] = cdc.[iglocn_conv]
	,[iglotn] = cdc.[iglotn]
	,[iglotn_conv] = cdc.[iglotn_conv]
	,[iglvla] = cdc.[iglvla]
	,[iglvlb] = cdc.[iglvlb]
	,[igpcst] = cdc.[igpcst]
	,[igpcst_conv] = cdc.[igpcst_conv]
	,[igpamt] = cdc.[igpamt]
	,[igpamt_conv] = cdc.[igpamt_conv]
	,[igratf] = cdc.[igratf]
	,[igratf_conv] = cdc.[igratf_conv]
	,[igratv] = cdc.[igratv]
	,[igratv_conv] = cdc.[igratv_conv]
	,[igan8] = cdc.[igan8]
	,[igglc] = cdc.[igglc]
	,[igglc_conv] = cdc.[igglc_conv]
	,[igglpt] = cdc.[igglpt]
	,[igefff] = cdc.[igefff]
	,[igefff_conv] = cdc.[igefff_conv]
	,[igefft] = cdc.[igefft]
	,[igefft_conv] = cdc.[igefft_conv]
	,[igtx] = cdc.[igtx]
	,[iginyn] = cdc.[iginyn]
	,[iginyn_conv] = cdc.[iginyn_conv]
	,[igrcyn] = cdc.[igrcyn]
	,[igrcyn_conv] = cdc.[igrcyn_conv]
	,[igaisl] = cdc.[igaisl]
	,[igaisl_conv] = cdc.[igaisl_conv]
	,[igbin] = cdc.[igbin]
	,[igbin_conv] = cdc.[igbin_conv]
	,[iguser] = cdc.[iguser]
	,[iguser_conv] = cdc.[iguser_conv]
	,[igjobn] = cdc.[igjobn]
	,[igjobn_conv] = cdc.[igjobn_conv]
	,[igpid] = cdc.[igpid]
	,[igpid_conv] = cdc.[igpid_conv]
	,[igupmj] = cdc.[igupmj]
	,[igupmj_conv] = cdc.[igupmj_conv]
	,[igtday] = cdc.[igtday] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f41291_cdc cdc
WHERE
    rep_jde_qat.f41291_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f41291_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f41291_new AF:{{ task_instance_key_str }}' ) 