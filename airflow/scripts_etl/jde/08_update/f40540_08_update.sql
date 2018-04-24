-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f40540_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[icitm] = cdc.[icitm]
	,[iccmdcde] = cdc.[iccmdcde]
	,[iccmdcde_conv] = cdc.[iccmdcde_conv]
	,[icunspsc] = cdc.[icunspsc]
	,[icunspsc_conv] = cdc.[icunspsc_conv]
	,[icuser] = cdc.[icuser]
	,[icuser_conv] = cdc.[icuser_conv]
	,[icpid] = cdc.[icpid]
	,[icpid_conv] = cdc.[icpid_conv]
	,[icjobn] = cdc.[icjobn]
	,[icjobn_conv] = cdc.[icjobn_conv]
	,[icupmt] = cdc.[icupmt]
	,[icupmj] = cdc.[icupmj]
	,[icupmj_conv] = cdc.[icupmj_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f40540_cdc cdc
WHERE
    rep_jde.f40540_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f40540_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f40540_new AF:{{ task_instance_key_str }}' ) 
