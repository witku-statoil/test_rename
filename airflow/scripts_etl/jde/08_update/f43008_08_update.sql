-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f43008_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[apdcto] = cdc.[apdcto]
	,[apartg] = cdc.[apartg]
	,[apartg_conv] = cdc.[apartg_conv]
	,[apdl01] = cdc.[apdl01]
	,[apdl01_conv] = cdc.[apdl01_conv]
	,[apalim] = cdc.[apalim]
	,[apalim_conv] = cdc.[apalim_conv]
	,[aprper] = cdc.[aprper]
	,[aprper_conv] = cdc.[aprper_conv]
	,[apaty] = cdc.[apaty]
	,[apaty_conv] = cdc.[apaty_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f43008_cdc cdc
WHERE
    rep_jde.f43008_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f43008_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f43008_new AF:{{ task_instance_key_str }}' ) 
