-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f4070_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[snasn] = cdc.[snasn]
	,[snoseq] = cdc.[snoseq]
	,[snoseq_conv] = cdc.[snoseq_conv]
	,[snanps] = cdc.[snanps]
	,[snanps_conv] = cdc.[snanps_conv]
	,[snast] = cdc.[snast]
	,[snurcd] = cdc.[snurcd]
	,[snurcd_conv] = cdc.[snurcd_conv]
	,[snurdt] = cdc.[snurdt]
	,[snurdt_conv] = cdc.[snurdt_conv]
	,[snurat] = cdc.[snurat]
	,[snurat_conv] = cdc.[snurat_conv]
	,[snurab] = cdc.[snurab]
	,[snurab_conv] = cdc.[snurab_conv]
	,[snurrf] = cdc.[snurrf]
	,[snurrf_conv] = cdc.[snurrf_conv]
	,[sneftj] = cdc.[sneftj]
	,[sneftj_conv] = cdc.[sneftj_conv]
	,[snexdj] = cdc.[snexdj]
	,[snexdj_conv] = cdc.[snexdj_conv]
	,[sninht] = cdc.[sninht]
	,[sninht_conv] = cdc.[sninht_conv]
	,[sntier] = cdc.[sntier]
	,[snuser] = cdc.[snuser]
	,[snuser_conv] = cdc.[snuser_conv]
	,[snpid] = cdc.[snpid]
	,[snpid_conv] = cdc.[snpid_conv]
	,[snjobn] = cdc.[snjobn]
	,[snjobn_conv] = cdc.[snjobn_conv]
	,[snupmj] = cdc.[snupmj]
	,[snupmj_conv] = cdc.[snupmj_conv]
	,[sntday] = cdc.[sntday]
	,[snsctype] = cdc.[snsctype]
	,[snsctype_conv] = cdc.[snsctype_conv]
	,[snstprcf] = cdc.[snstprcf]
	,[snstprcf_conv] = cdc.[snstprcf_conv]
	,[snskipto] = cdc.[snskipto]
	,[snskipto_conv] = cdc.[snskipto_conv]
	,[snskipend] = cdc.[snskipend]
	,[snskipend_conv] = cdc.[snskipend_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f4070_cdc cdc
WHERE
    rep_jde.f4070_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f4070_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f4070_new AF:{{ task_instance_key_str }}' ) 
