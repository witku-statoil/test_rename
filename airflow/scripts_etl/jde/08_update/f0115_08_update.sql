-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f0115_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[wpan8] = cdc.[wpan8]
	,[wpidln] = cdc.[wpidln]
	,[wpidln_conv] = cdc.[wpidln_conv]
	,[wprck7] = cdc.[wprck7]
	,[wpcnln] = cdc.[wpcnln]
	,[wpcnln_conv] = cdc.[wpcnln_conv]
	,[wpphtp] = cdc.[wpphtp]
	,[wpar1] = cdc.[wpar1]
	,[wpar1_conv] = cdc.[wpar1_conv]
	,[wpph1] = cdc.[wpph1]
	,[wpph1_conv] = cdc.[wpph1_conv]
	,[wpuser] = cdc.[wpuser]
	,[wpuser_conv] = cdc.[wpuser_conv]
	,[wppid] = cdc.[wppid]
	,[wppid_conv] = cdc.[wppid_conv]
	,[wpupmj] = cdc.[wpupmj]
	,[wpupmj_conv] = cdc.[wpupmj_conv]
	,[wpjobn] = cdc.[wpjobn]
	,[wpjobn_conv] = cdc.[wpjobn_conv]
	,[wpupmt] = cdc.[wpupmt]
	,[wpcfno1] = cdc.[wpcfno1]
	,[wpcfno1_conv] = cdc.[wpcfno1_conv]
	,[wpgen1] = cdc.[wpgen1]
	,[wpgen1_conv] = cdc.[wpgen1_conv]
	,[wpfalge] = cdc.[wpfalge]
	,[wpfalge_conv] = cdc.[wpfalge_conv]
	,[wpsyncs] = cdc.[wpsyncs]
	,[wpcaad] = cdc.[wpcaad] -- exclude distribution column
FROM stg_jde.tmp_upsert_f0115_cdc cdc
WHERE
    rep_jde.f0115_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f0115_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f0115_new AF:{{ task_instance_key_str }}' ) 
