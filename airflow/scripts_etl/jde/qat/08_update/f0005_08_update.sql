-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f0005_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[drsy] = cdc.[drsy]
	,[drrt] = cdc.[drrt]
	,[drrt_conv] = cdc.[drrt_conv]
	,[drky] = cdc.[drky]
	,[drky_conv] = cdc.[drky_conv]
	,[drdl01] = cdc.[drdl01]
	,[drdl01_conv] = cdc.[drdl01_conv]
	,[drdl02] = cdc.[drdl02]
	,[drdl02_conv] = cdc.[drdl02_conv]
	,[drsphd] = cdc.[drsphd]
	,[drsphd_conv] = cdc.[drsphd_conv]
	,[drudco] = cdc.[drudco]
	,[drhrdc] = cdc.[drhrdc]
	,[drhrdc_conv] = cdc.[drhrdc_conv]
	,[druser] = cdc.[druser]
	,[druser_conv] = cdc.[druser_conv]
	,[drpid] = cdc.[drpid]
	,[drpid_conv] = cdc.[drpid_conv]
	,[drupmj] = cdc.[drupmj]
	,[drupmj_conv] = cdc.[drupmj_conv]
	,[drjobn] = cdc.[drjobn]
	,[drjobn_conv] = cdc.[drjobn_conv]
	,[drupmt] = cdc.[drupmt] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f0005_cdc cdc
WHERE
    rep_jde_qat.f0005_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f0005_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f0005_new AF:{{ task_instance_key_str }}' ) 
