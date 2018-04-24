-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f40943_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[oksdgr] = cdc.[oksdgr]
	,[oksdv1] = cdc.[oksdv1]
	,[oksdv1_conv] = cdc.[oksdv1_conv]
	,[oksdv2] = cdc.[oksdv2]
	,[oksdv2_conv] = cdc.[oksdv2_conv]
	,[oksdv3] = cdc.[oksdv3]
	,[oksdv3_conv] = cdc.[oksdv3_conv]
	,[oksdv4] = cdc.[oksdv4]
	,[oksdv4_conv] = cdc.[oksdv4_conv]
	,[oksdv5] = cdc.[oksdv5]
	,[oksdv5_conv] = cdc.[oksdv5_conv]
	,[oksdv6] = cdc.[oksdv6]
	,[oksdv6_conv] = cdc.[oksdv6_conv]
	,[oksdv7] = cdc.[oksdv7]
	,[oksdv7_conv] = cdc.[oksdv7_conv]
	,[oksdv8] = cdc.[oksdv8]
	,[oksdv8_conv] = cdc.[oksdv8_conv]
	,[okogid] = cdc.[okogid]
	,[okogid_conv] = cdc.[okogid_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f40943_cdc cdc
WHERE
    rep_jde_qat.f40943_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f40943_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f40943_new AF:{{ task_instance_key_str }}' ) 
