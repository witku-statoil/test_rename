-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f0150_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[maostp] = cdc.[maostp]
	,[mapa8] = cdc.[mapa8]
	,[maan8] = cdc.[maan8]
	,[madss7] = cdc.[madss7]
	,[mabefd] = cdc.[mabefd]
	,[mabefd_conv] = cdc.[mabefd_conv]
	,[maeefd] = cdc.[maeefd]
	,[maeefd_conv] = cdc.[maeefd_conv]
	,[marmk] = cdc.[marmk]
	,[marmk_conv] = cdc.[marmk_conv]
	,[mauser] = cdc.[mauser]
	,[mauser_conv] = cdc.[mauser_conv]
	,[maupmj] = cdc.[maupmj]
	,[maupmj_conv] = cdc.[maupmj_conv]
	,[mapid] = cdc.[mapid]
	,[mapid_conv] = cdc.[mapid_conv]
	,[majobn] = cdc.[majobn]
	,[majobn_conv] = cdc.[majobn_conv]
	,[maupmt] = cdc.[maupmt]
	,[masyncs] = cdc.[masyncs] -- exclude distribution column
FROM stg_jde.tmp_upsert_f0150_cdc cdc
WHERE
    rep_jde.f0150_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f0150_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f0150_new AF:{{ task_instance_key_str }}' ) 
