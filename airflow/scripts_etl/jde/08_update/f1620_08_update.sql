-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f1620_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[ctabt1] = cdc.[ctabt1]
	,[ctabt1_conv] = cdc.[ctabt1_conv]
	,[ctdl01] = cdc.[ctdl01]
	,[ctdl01_conv] = cdc.[ctdl01_conv]
	,[ctcmer] = cdc.[ctcmer]
	,[ctfile] = cdc.[ctfile]
	,[ctfile_conv] = cdc.[ctfile_conv]
	,[ctdtai] = cdc.[ctdtai]
	,[ctdtai_conv] = cdc.[ctdtai_conv]
	,[ctvalr] = cdc.[ctvalr]
	,[ctcmvl] = cdc.[ctcmvl]
	,[ctcmvl_conv] = cdc.[ctcmvl_conv]
	,[ctsy] = cdc.[ctsy]
	,[ctrt] = cdc.[ctrt]
	,[ctrt_conv] = cdc.[ctrt_conv]
	,[ctuser] = cdc.[ctuser]
	,[ctuser_conv] = cdc.[ctuser_conv]
	,[ctpid] = cdc.[ctpid]
	,[ctpid_conv] = cdc.[ctpid_conv]
	,[ctupmj] = cdc.[ctupmj]
	,[ctupmj_conv] = cdc.[ctupmj_conv]
	,[ctupmt] = cdc.[ctupmt]
	,[ctjobn] = cdc.[ctjobn]
	,[ctjobn_conv] = cdc.[ctjobn_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f1620_cdc cdc
WHERE
    rep_jde.f1620_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f1620_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f1620_new AF:{{ task_instance_key_str }}' ) 
