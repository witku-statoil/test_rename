-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f4906_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[cmcars] = cdc.[cmcars]
	,[cmcars_conv] = cdc.[cmcars_conv]
	,[cmscac] = cdc.[cmscac]
	,[cmscac_conv] = cdc.[cmscac_conv]
	,[cmcamd] = cdc.[cmcamd]
	,[cmcamd_conv] = cdc.[cmcamd_conv]
	,[cmstbf] = cdc.[cmstbf]
	,[cmstbf_conv] = cdc.[cmstbf_conv]
	,[cmstft] = cdc.[cmstft]
	,[cmrfq1] = cdc.[cmrfq1]
	,[cmrfq2] = cdc.[cmrfq2]
	,[cmrndn] = cdc.[cmrndn]
	,[cmdwfc] = cdc.[cmdwfc]
	,[cmdwfc_conv] = cdc.[cmdwfc_conv]
	,[cmrsla] = cdc.[cmrsla]
	,[cmrsla_conv] = cdc.[cmrsla_conv]
	,[cmpfsd] = cdc.[cmpfsd]
	,[cmpfsd_conv] = cdc.[cmpfsd_conv]
	,[cmprfm] = cdc.[cmprfm]
	,[cmprfm_conv] = cdc.[cmprfm_conv]
	,[cmuser] = cdc.[cmuser]
	,[cmuser_conv] = cdc.[cmuser_conv]
	,[cmpid] = cdc.[cmpid]
	,[cmpid_conv] = cdc.[cmpid_conv]
	,[cmjobn] = cdc.[cmjobn]
	,[cmjobn_conv] = cdc.[cmjobn_conv]
	,[cmupmj] = cdc.[cmupmj]
	,[cmupmj_conv] = cdc.[cmupmj_conv]
	,[cmtday] = cdc.[cmtday] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f4906_cdc cdc
WHERE
    rep_jde_qat.f4906_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f4906_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f4906_new AF:{{ task_instance_key_str }}' ) 
