-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f4092_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[gpgpty] = cdc.[gpgpty]
	,[gpgpty_conv] = cdc.[gpgpty_conv]
	,[gpgpc] = cdc.[gpgpc]
	,[gpgpc_conv] = cdc.[gpgpc_conv]
	,[gpdl01] = cdc.[gpdl01]
	,[gpdl01_conv] = cdc.[gpdl01_conv]
	,[gpgpk1] = cdc.[gpgpk1]
	,[gpgpk1_conv] = cdc.[gpgpk1_conv]
	,[gpgpk2] = cdc.[gpgpk2]
	,[gpgpk2_conv] = cdc.[gpgpk2_conv]
	,[gpgpk3] = cdc.[gpgpk3]
	,[gpgpk3_conv] = cdc.[gpgpk3_conv]
	,[gpgpk4] = cdc.[gpgpk4]
	,[gpgpk4_conv] = cdc.[gpgpk4_conv]
	,[gpgpk5] = cdc.[gpgpk5]
	,[gpgpk5_conv] = cdc.[gpgpk5_conv]
	,[gpgpk6] = cdc.[gpgpk6]
	,[gpgpk6_conv] = cdc.[gpgpk6_conv]
	,[gpgpk7] = cdc.[gpgpk7]
	,[gpgpk7_conv] = cdc.[gpgpk7_conv]
	,[gpgpk8] = cdc.[gpgpk8]
	,[gpgpk8_conv] = cdc.[gpgpk8_conv]
	,[gpgpk9] = cdc.[gpgpk9]
	,[gpgpk9_conv] = cdc.[gpgpk9_conv]
	,[gpgpk10] = cdc.[gpgpk10]
	,[gpgpk10_conv] = cdc.[gpgpk10_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f4092_cdc cdc
WHERE
    rep_jde_qat.f4092_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f4092_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f4092_new AF:{{ task_instance_key_str }}' ) 
