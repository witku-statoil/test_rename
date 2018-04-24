-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f5509176_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[qsmcu] = cdc.[qsmcu]
	,[qsmcu_conv] = cdc.[qsmcu_conv]
	,[qsy55fypn2] = cdc.[qsy55fypn2]
	,[qsy55fypn2_conv] = cdc.[qsy55fypn2_conv]
	,[qsrp04] = cdc.[qsrp04] -- exclude distribution column
FROM stg_jde.tmp_upsert_f5509176_cdc cdc
WHERE
    rep_jde.f5509176_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f5509176_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f5509176_new AF:{{ task_instance_key_str }}' ) 
