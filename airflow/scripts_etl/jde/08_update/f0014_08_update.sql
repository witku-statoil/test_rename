-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f0014_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[pnptc] = cdc.[pnptc]
	,[pnptc_conv] = cdc.[pnptc_conv]
	,[pnptd] = cdc.[pnptd]
	,[pnptd_conv] = cdc.[pnptd_conv]
	,[pndcp] = cdc.[pndcp]
	,[pndcd] = cdc.[pndcd]
	,[pnndtp] = cdc.[pnndtp]
	,[pnddj] = cdc.[pnddj]
	,[pnddj_conv] = cdc.[pnddj_conv]
	,[pnnsp] = cdc.[pnnsp]
	,[pndtpa] = cdc.[pndtpa]
	,[pneir] = cdc.[pneir]
	,[pnuser] = cdc.[pnuser]
	,[pnuser_conv] = cdc.[pnuser_conv]
	,[pnpid] = cdc.[pnpid]
	,[pnpid_conv] = cdc.[pnpid_conv]
	,[pnupmj] = cdc.[pnupmj]
	,[pnupmj_conv] = cdc.[pnupmj_conv]
	,[pnjobn] = cdc.[pnjobn]
	,[pnjobn_conv] = cdc.[pnjobn_conv]
	,[pnupmt] = cdc.[pnupmt]
	,[pnpxdm] = cdc.[pnpxdm]
	,[pnpxdm_conv] = cdc.[pnpxdm_conv]
	,[pnpxdd] = cdc.[pnpxdd]
	,[pnpxdd_conv] = cdc.[pnpxdd_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f0014_cdc cdc
WHERE
    rep_jde.f0014_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f0014_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f0014_new AF:{{ task_instance_key_str }}' ) 
