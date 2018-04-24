-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f556202_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[emctr] = cdc.[emctr]
	,[emco] = cdc.[emco]
	,[emco_conv] = cdc.[emco_conv]
	,[emmcu] = cdc.[emmcu]
	,[emmcu_conv] = cdc.[emmcu_conv]
	,[emlocn] = cdc.[emlocn]
	,[emlocn_conv] = cdc.[emlocn_conv]
	,[emev01] = cdc.[emev01]
	,[emev01_conv] = cdc.[emev01_conv]
	,[emev02] = cdc.[emev02]
	,[emev02_conv] = cdc.[emev02_conv]
	,[emmath01] = cdc.[emmath01]
	,[emmath01_conv] = cdc.[emmath01_conv]
	,[emmath02] = cdc.[emmath02]
	,[emmath02_conv] = cdc.[emmath02_conv]
	,[emdl01] = cdc.[emdl01]
	,[emdl01_conv] = cdc.[emdl01_conv]
	,[emdl02] = cdc.[emdl02]
	,[emdl02_conv] = cdc.[emdl02_conv]
	,[emtrdj] = cdc.[emtrdj]
	,[emtrdj_conv] = cdc.[emtrdj_conv]
	,[empddj] = cdc.[empddj]
	,[empddj_conv] = cdc.[empddj_conv]
	,[emuser] = cdc.[emuser]
	,[emuser_conv] = cdc.[emuser_conv]
	,[empid] = cdc.[empid]
	,[empid_conv] = cdc.[empid_conv]
	,[emjobn] = cdc.[emjobn]
	,[emjobn_conv] = cdc.[emjobn_conv]
	,[emupmj] = cdc.[emupmj]
	,[emupmj_conv] = cdc.[emupmj_conv]
	,[emtday] = cdc.[emtday] -- exclude distribution column
FROM stg_jde.tmp_upsert_f556202_cdc cdc
WHERE
    rep_jde.f556202_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f556202_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f556202_new AF:{{ task_instance_key_str }}' ) 
