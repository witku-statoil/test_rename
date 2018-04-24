-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f4075_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[vbvbt] = cdc.[vbvbt]
	,[vbcrcd] = cdc.[vbcrcd]
	,[vbcrcd_conv] = cdc.[vbcrcd_conv]
	,[vbuom] = cdc.[vbuom]
	,[vbuprc] = cdc.[vbuprc]
	,[vbuprc_conv] = cdc.[vbuprc_conv]
	,[vbeftj] = cdc.[vbeftj]
	,[vbeftj_conv] = cdc.[vbeftj_conv]
	,[vbexdj] = cdc.[vbexdj]
	,[vbexdj_conv] = cdc.[vbexdj_conv]
	,[vbaprs] = cdc.[vbaprs]
	,[vbuser] = cdc.[vbuser]
	,[vbuser_conv] = cdc.[vbuser_conv]
	,[vbpid] = cdc.[vbpid]
	,[vbpid_conv] = cdc.[vbpid_conv]
	,[vbjobn] = cdc.[vbjobn]
	,[vbjobn_conv] = cdc.[vbjobn_conv]
	,[vbupmj] = cdc.[vbupmj]
	,[vbupmj_conv] = cdc.[vbupmj_conv]
	,[vbtday] = cdc.[vbtday] -- exclude distribution column
FROM stg_jde.tmp_upsert_f4075_cdc cdc
WHERE
    rep_jde.f4075_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f4075_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f4075_new AF:{{ task_instance_key_str }}' ) 
