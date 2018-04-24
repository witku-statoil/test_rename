-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f3412_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[mwitm] = cdc.[mwitm]
	,[mwmcu] = cdc.[mwmcu]
	,[mwmcu_conv] = cdc.[mwmcu_conv]
	,[mwdrqj] = cdc.[mwdrqj]
	,[mwdrqj_conv] = cdc.[mwdrqj_conv]
	,[mwlovd] = cdc.[mwlovd]
	,[mwlovd_conv] = cdc.[mwlovd_conv]
	,[mwkit] = cdc.[mwkit]
	,[mwmmcu] = cdc.[mwmmcu]
	,[mwmmcu_conv] = cdc.[mwmmcu_conv]
	,[mwuorg] = cdc.[mwuorg]
	,[mwuorg_conv] = cdc.[mwuorg_conv]
	,[mwdoco] = cdc.[mwdoco]
	,[mwdcto] = cdc.[mwdcto]
	,[mwrkco] = cdc.[mwrkco]
	,[mwrkco_conv] = cdc.[mwrkco_conv]
	,[mwrorn] = cdc.[mwrorn]
	,[mwrorn_conv] = cdc.[mwrorn_conv]
	,[mwrcto] = cdc.[mwrcto]
	,[mwrlln] = cdc.[mwrlln]
	,[mwrlln_conv] = cdc.[mwrlln_conv]
	,[mwukid] = cdc.[mwukid]
	,[mwplnk] = cdc.[mwplnk]
	,[mwprjm] = cdc.[mwprjm]
	,[mwprjm_conv] = cdc.[mwprjm_conv]
	,[mwsrdm] = cdc.[mwsrdm]
	,[mwsrdm_conv] = cdc.[mwsrdm_conv]
	,[mwpns] = cdc.[mwpns] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f3412_cdc cdc
WHERE
    rep_jde_qat.f3412_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f3412_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f3412_new AF:{{ task_instance_key_str }}' ) 
