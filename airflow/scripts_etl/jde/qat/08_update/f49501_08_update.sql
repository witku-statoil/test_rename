-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f49501_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[rdprnb] = cdc.[rdprnb]
	,[rdlnmb] = cdc.[rdlnmb]
	,[rdorgn] = cdc.[rdorgn]
	,[rdancc] = cdc.[rdancc]
	,[rdcars] = cdc.[rdcars]
	,[rdcars_conv] = cdc.[rdcars_conv]
	,[rdmot] = cdc.[rdmot]
	,[rdfrsc] = cdc.[rdfrsc]
	,[rdfrcg] = cdc.[rdfrcg]
	,[rdfrcg_conv] = cdc.[rdfrcg_conv]
	,[rdcrcd] = cdc.[rdcrcd]
	,[rdcrcd_conv] = cdc.[rdcrcd_conv]
	,[rdfrtp] = cdc.[rdfrtp]
	,[rdrtgb] = cdc.[rdrtgb]
	,[rdrtum] = cdc.[rdrtum]
	,[rddstn] = cdc.[rddstn]
	,[rdumd1] = cdc.[rdumd1]
	,[rdrtn] = cdc.[rdrtn]
	,[rdcnmr] = cdc.[rdcnmr]
	,[rdcnmr_conv] = cdc.[rdcnmr_conv]
	,[rdltdt] = cdc.[rdltdt]
	,[rdltdt_conv] = cdc.[rdltdt_conv]
	,[rdeftj] = cdc.[rdeftj]
	,[rdeftj_conv] = cdc.[rdeftj_conv]
	,[rdexdj] = cdc.[rdexdj]
	,[rdexdj_conv] = cdc.[rdexdj_conv]
	,[rdurcd] = cdc.[rdurcd]
	,[rdurcd_conv] = cdc.[rdurcd_conv]
	,[rdurdt] = cdc.[rdurdt]
	,[rdurdt_conv] = cdc.[rdurdt_conv]
	,[rdurat] = cdc.[rdurat]
	,[rdurat_conv] = cdc.[rdurat_conv]
	,[rdurab] = cdc.[rdurab]
	,[rdurab_conv] = cdc.[rdurab_conv]
	,[rdurrf] = cdc.[rdurrf]
	,[rdurrf_conv] = cdc.[rdurrf_conv]
	,[rduser] = cdc.[rduser]
	,[rduser_conv] = cdc.[rduser_conv]
	,[rdpid] = cdc.[rdpid]
	,[rdpid_conv] = cdc.[rdpid_conv]
	,[rdjobn] = cdc.[rdjobn]
	,[rdjobn_conv] = cdc.[rdjobn_conv]
	,[rdupmj] = cdc.[rdupmj]
	,[rdupmj_conv] = cdc.[rdupmj_conv]
	,[rdtday] = cdc.[rdtday]
	,[rdtx] = cdc.[rdtx]
	,[rdtxa1] = cdc.[rdtxa1]
	,[rdtxa1_conv] = cdc.[rdtxa1_conv]
	,[rdexr1] = cdc.[rdexr1]
	,[rdtax1] = cdc.[rdtax1]
	,[rdtxa2] = cdc.[rdtxa2]
	,[rdtxa2_conv] = cdc.[rdtxa2_conv]
	,[rdexr2] = cdc.[rdexr2] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f49501_cdc cdc
WHERE
    rep_jde_qat.f49501_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f49501_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f49501_new AF:{{ task_instance_key_str }}' ) 
