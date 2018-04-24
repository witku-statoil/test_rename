-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f0015_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[cxcrcd] = cdc.[cxcrcd]
	,[cxcrcd_conv] = cdc.[cxcrcd_conv]
	,[cxcrdc] = cdc.[cxcrdc]
	,[cxcrdc_conv] = cdc.[cxcrdc_conv]
	,[cxan8] = cdc.[cxan8]
	,[cxrttyp] = cdc.[cxrttyp]
	,[cxeft] = cdc.[cxeft]
	,[cxeft_conv] = cdc.[cxeft_conv]
	,[cxclmeth] = cdc.[cxclmeth]
	,[cxclmeth_conv] = cdc.[cxclmeth_conv]
	,[cxcrcm] = cdc.[cxcrcm]
	,[cxcrcm_conv] = cdc.[cxcrcm_conv]
	,[cxtrcr] = cdc.[cxtrcr]
	,[cxtrcr_conv] = cdc.[cxtrcr_conv]
	,[cxcrr] = cdc.[cxcrr]
	,[cxcrrd] = cdc.[cxcrrd]
	,[cxcsr] = cdc.[cxcsr]
	,[cxcsr_conv] = cdc.[cxcsr_conv]
	,[cxuser] = cdc.[cxuser]
	,[cxuser_conv] = cdc.[cxuser_conv]
	,[cxpid] = cdc.[cxpid]
	,[cxpid_conv] = cdc.[cxpid_conv]
	,[cxjobn] = cdc.[cxjobn]
	,[cxjobn_conv] = cdc.[cxjobn_conv]
	,[cxupmj] = cdc.[cxupmj]
	,[cxupmj_conv] = cdc.[cxupmj_conv]
	,[cxupmt] = cdc.[cxupmt] -- exclude distribution column
FROM stg_jde.tmp_upsert_f0015_cdc cdc
WHERE
    rep_jde.f0015_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f0015_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f0015_new AF:{{ task_instance_key_str }}' ) 
