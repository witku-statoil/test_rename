-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f550312_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[sglrsegt] = cdc.[sglrsegt]
	,[sgctr] = cdc.[sgctr]
	,[sgac04] = cdc.[sgac04]
	,[sgdsc1] = cdc.[sgdsc1]
	,[sgdsc1_conv] = cdc.[sgdsc1_conv]
	,[sgac05] = cdc.[sgac05]
	,[sgdsc2] = cdc.[sgdsc2]
	,[sgdsc2_conv] = cdc.[sgdsc2_conv]
	,[sgac01] = cdc.[sgac01]
	,[sgclass03] = cdc.[sgclass03]
	,[sgsrp1] = cdc.[sgsrp1]
	,[sgy55char1] = cdc.[sgy55char1]
	,[sgy55char1_conv] = cdc.[sgy55char1_conv]
	,[sgy55char2] = cdc.[sgy55char2]
	,[sgy55char2_conv] = cdc.[sgy55char2_conv]
	,[sgy55strg1] = cdc.[sgy55strg1]
	,[sgy55strg1_conv] = cdc.[sgy55strg1_conv]
	,[sgy55strg2] = cdc.[sgy55strg2]
	,[sgy55strg2_conv] = cdc.[sgy55strg2_conv]
	,[sgy55amnt1] = cdc.[sgy55amnt1]
	,[sgy55amnt1_conv] = cdc.[sgy55amnt1_conv]
	,[sgy55amnt2] = cdc.[sgy55amnt2]
	,[sgy55amnt2_conv] = cdc.[sgy55amnt2_conv]
	,[sgy55time1] = cdc.[sgy55time1]
	,[sgy55time2] = cdc.[sgy55time2]
	,[sgy55date1] = cdc.[sgy55date1]
	,[sgy55date1_conv] = cdc.[sgy55date1_conv]
	,[sgy55date2] = cdc.[sgy55date2]
	,[sgy55date2_conv] = cdc.[sgy55date2_conv]
	,[sguser] = cdc.[sguser]
	,[sguser_conv] = cdc.[sguser_conv]
	,[sgpid] = cdc.[sgpid]
	,[sgpid_conv] = cdc.[sgpid_conv]
	,[sgjobn] = cdc.[sgjobn]
	,[sgjobn_conv] = cdc.[sgjobn_conv]
	,[sgupmj] = cdc.[sgupmj]
	,[sgupmj_conv] = cdc.[sgupmj_conv]
	,[sgupmt] = cdc.[sgupmt]
	,[sgdesc] = cdc.[sgdesc]
	,[sgdesc_conv] = cdc.[sgdesc_conv]
	,[sgan8] = cdc.[sgan8]
	,[sgalph] = cdc.[sgalph]
	,[sgalph_conv] = cdc.[sgalph_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f550312_cdc cdc
WHERE
    rep_jde.f550312_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f550312_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f550312_new AF:{{ task_instance_key_str }}' ) 
