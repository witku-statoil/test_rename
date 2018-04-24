-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f4076_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[fmfrmn] = cdc.[fmfrmn]
	,[fmfml] = cdc.[fmfml]
	,[fmfml_conv] = cdc.[fmfml_conv]
	,[fmaprs] = cdc.[fmaprs]
	,[fmuser] = cdc.[fmuser]
	,[fmuser_conv] = cdc.[fmuser_conv]
	,[fmpid] = cdc.[fmpid]
	,[fmpid_conv] = cdc.[fmpid_conv]
	,[fmjobn] = cdc.[fmjobn]
	,[fmjobn_conv] = cdc.[fmjobn_conv]
	,[fmupmj] = cdc.[fmupmj]
	,[fmupmj_conv] = cdc.[fmupmj_conv]
	,[fmtday] = cdc.[fmtday] -- exclude distribution column
FROM stg_jde.tmp_upsert_f4076_cdc cdc
WHERE
    rep_jde.f4076_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f4076_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f4076_new AF:{{ task_instance_key_str }}' ) 
