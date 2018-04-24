-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f1113_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[c1rtty] = cdc.[c1rtty]
	,[c1crdc] = cdc.[c1crdc]
	,[c1crdc_conv] = cdc.[c1crdc_conv]
	,[c1crcd] = cdc.[c1crcd]
	,[c1crcd_conv] = cdc.[c1crcd_conv]
	,[c1eft] = cdc.[c1eft]
	,[c1eft_conv] = cdc.[c1eft_conv]
	,[c1crr] = cdc.[c1crr]
	,[c1crrd] = cdc.[c1crrd] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f1113_cdc cdc
WHERE
    rep_jde_qat.f1113_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f1113_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f1113_new AF:{{ task_instance_key_str }}' ) 
