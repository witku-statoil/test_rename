-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f41002_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[ummcu] = cdc.[ummcu]
	,[ummcu_conv] = cdc.[ummcu_conv]
	,[umitm] = cdc.[umitm]
	,[umum] = cdc.[umum]
	,[umrum] = cdc.[umrum]
	,[umustr] = cdc.[umustr]
	,[umconv] = cdc.[umconv]
	,[umconv_conv] = cdc.[umconv_conv]
	,[umcnv1] = cdc.[umcnv1]
	,[umcnv1_conv] = cdc.[umcnv1_conv]
	,[umuser] = cdc.[umuser]
	,[umuser_conv] = cdc.[umuser_conv]
	,[umpid] = cdc.[umpid]
	,[umpid_conv] = cdc.[umpid_conv]
	,[umjobn] = cdc.[umjobn]
	,[umjobn_conv] = cdc.[umjobn_conv]
	,[umupmj] = cdc.[umupmj]
	,[umupmj_conv] = cdc.[umupmj_conv]
	,[umtday] = cdc.[umtday]
	,[umexpo] = cdc.[umexpo]
	,[umexpo_conv] = cdc.[umexpo_conv]
	,[umexso] = cdc.[umexso]
	,[umexso_conv] = cdc.[umexso_conv]
	,[umpupc] = cdc.[umpupc]
	,[umpupc_conv] = cdc.[umpupc_conv]
	,[umsepc] = cdc.[umsepc]
	,[umsepc_conv] = cdc.[umsepc_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f41002_cdc cdc
WHERE
    rep_jde_qat.f41002_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f41002_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f41002_new AF:{{ task_instance_key_str }}' ) 
