-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f41003_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[ucum] = cdc.[ucum]
	,[ucrum] = cdc.[ucrum]
	,[ucconv] = cdc.[ucconv]
	,[ucconv_conv] = cdc.[ucconv_conv]
	,[ucuser] = cdc.[ucuser]
	,[ucuser_conv] = cdc.[ucuser_conv]
	,[ucpid] = cdc.[ucpid]
	,[ucpid_conv] = cdc.[ucpid_conv]
	,[ucjobn] = cdc.[ucjobn]
	,[ucjobn_conv] = cdc.[ucjobn_conv]
	,[ucupmj] = cdc.[ucupmj]
	,[ucupmj_conv] = cdc.[ucupmj_conv]
	,[uctday] = cdc.[uctday]
	,[ucexpo] = cdc.[ucexpo]
	,[ucexpo_conv] = cdc.[ucexpo_conv]
	,[ucexso] = cdc.[ucexso]
	,[ucexso_conv] = cdc.[ucexso_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f41003_cdc cdc
WHERE
    rep_jde_qat.f41003_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f41003_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f41003_new AF:{{ task_instance_key_str }}' ) 
