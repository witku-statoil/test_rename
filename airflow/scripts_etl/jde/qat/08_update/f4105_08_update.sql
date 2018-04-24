-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f4105_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[coitm] = cdc.[coitm]
	,[colitm] = cdc.[colitm]
	,[colitm_conv] = cdc.[colitm_conv]
	,[coaitm] = cdc.[coaitm]
	,[coaitm_conv] = cdc.[coaitm_conv]
	,[comcu] = cdc.[comcu]
	,[comcu_conv] = cdc.[comcu_conv]
	,[colocn] = cdc.[colocn]
	,[colocn_conv] = cdc.[colocn_conv]
	,[colotn] = cdc.[colotn]
	,[colotn_conv] = cdc.[colotn_conv]
	,[colotg] = cdc.[colotg]
	,[coledg] = cdc.[coledg]
	,[councs] = cdc.[councs]
	,[councs_conv] = cdc.[councs_conv]
	,[cocspo] = cdc.[cocspo]
	,[cocsin] = cdc.[cocsin]
	,[courcd] = cdc.[courcd]
	,[courcd_conv] = cdc.[courcd_conv]
	,[courdt] = cdc.[courdt]
	,[courdt_conv] = cdc.[courdt_conv]
	,[courat] = cdc.[courat]
	,[courat_conv] = cdc.[courat_conv]
	,[courab] = cdc.[courab]
	,[courab_conv] = cdc.[courab_conv]
	,[courrf] = cdc.[courrf]
	,[courrf_conv] = cdc.[courrf_conv]
	,[couser] = cdc.[couser]
	,[couser_conv] = cdc.[couser_conv]
	,[copid] = cdc.[copid]
	,[copid_conv] = cdc.[copid_conv]
	,[cojobn] = cdc.[cojobn]
	,[cojobn_conv] = cdc.[cojobn_conv]
	,[coupmj] = cdc.[coupmj]
	,[coupmj_conv] = cdc.[coupmj_conv]
	,[cotday] = cdc.[cotday]
	,[coccfl] = cdc.[coccfl]
	,[coccfl_conv] = cdc.[coccfl_conv]
	,[cocrcs] = cdc.[cocrcs]
	,[cocrcs_conv] = cdc.[cocrcs_conv]
	,[coostc] = cdc.[coostc]
	,[coostc_conv] = cdc.[coostc_conv]
	,[costoc] = cdc.[costoc]
	,[costoc_conv] = cdc.[costoc_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f4105_cdc cdc
WHERE
    rep_jde_qat.f4105_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f4105_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f4105_new AF:{{ task_instance_key_str }}' ) 