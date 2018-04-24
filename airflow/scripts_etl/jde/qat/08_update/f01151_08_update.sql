-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f01151_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[eaan8] = cdc.[eaan8]
	,[eaidln] = cdc.[eaidln]
	,[eaidln_conv] = cdc.[eaidln_conv]
	,[earck7] = cdc.[earck7]
	,[eaetp] = cdc.[eaetp]
	,[eaemal] = cdc.[eaemal]
	,[eaemal_conv] = cdc.[eaemal_conv]
	,[eauser] = cdc.[eauser]
	,[eauser_conv] = cdc.[eauser_conv]
	,[eapid] = cdc.[eapid]
	,[eapid_conv] = cdc.[eapid_conv]
	,[eaupmj] = cdc.[eaupmj]
	,[eaupmj_conv] = cdc.[eaupmj_conv]
	,[eajobn] = cdc.[eajobn]
	,[eajobn_conv] = cdc.[eajobn_conv]
	,[eaupmt] = cdc.[eaupmt]
	,[eaehier] = cdc.[eaehier]
	,[eaefor] = cdc.[eaefor]
	,[eaefor_conv] = cdc.[eaefor_conv]
	,[eaeclass] = cdc.[eaeclass]
	,[eacfno1] = cdc.[eacfno1]
	,[eacfno1_conv] = cdc.[eacfno1_conv]
	,[eagen1] = cdc.[eagen1]
	,[eagen1_conv] = cdc.[eagen1_conv]
	,[eafalge] = cdc.[eafalge]
	,[eafalge_conv] = cdc.[eafalge_conv]
	,[easyncs] = cdc.[easyncs]
	,[eacaad] = cdc.[eacaad] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f01151_cdc cdc
WHERE
    rep_jde_qat.f01151_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f01151_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f01151_new AF:{{ task_instance_key_str }}' ) 
