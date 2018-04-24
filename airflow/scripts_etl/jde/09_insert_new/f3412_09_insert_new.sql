-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f3412_new
(
    	sys_file_name
	,sys_file_ln
	,sys_integration_id
	,sys_extract_dt
	,sys_cdc_dt
	,sys_cdc_scn
	,sys_cdc_operation_type
	,sys_cdc_before_after
	,sys_line_modified_ind
	,mwitm
	,mwmcu
	,mwmcu_conv
	,mwdrqj
	,mwdrqj_conv
	,mwlovd
	,mwlovd_conv
	,mwkit
	,mwmmcu
	,mwmmcu_conv
	,mwuorg
	,mwuorg_conv
	,mwdoco
	,mwdcto
	,mwrkco
	,mwrkco_conv
	,mwrorn
	,mwrorn_conv
	,mwrcto
	,mwrlln
	,mwrlln_conv
	,mwukid
	,mwplnk
	,mwprjm
	,mwprjm_conv
	,mwsrdm
	,mwsrdm_conv
	,mwpns
)
SELECT
    	cdc.sys_file_name
	,cdc.sys_file_ln
	,cdc.sys_integration_id
	,cdc.sys_extract_dt
	,cdc.sys_cdc_dt
	,cdc.sys_cdc_scn
	,cdc.sys_cdc_operation_type
	,cdc.sys_cdc_before_after
	,cdc.sys_line_modified_ind
	,cdc.mwitm
	,cdc.mwmcu
	,cdc.mwmcu_conv
	,cdc.mwdrqj
	,cdc.mwdrqj_conv
	,cdc.mwlovd
	,cdc.mwlovd_conv
	,cdc.mwkit
	,cdc.mwmmcu
	,cdc.mwmmcu_conv
	,cdc.mwuorg
	,cdc.mwuorg_conv
	,cdc.mwdoco
	,cdc.mwdcto
	,cdc.mwrkco
	,cdc.mwrkco_conv
	,cdc.mwrorn
	,cdc.mwrorn_conv
	,cdc.mwrcto
	,cdc.mwrlln
	,cdc.mwrlln_conv
	,cdc.mwukid
	,cdc.mwplnk
	,cdc.mwprjm
	,cdc.mwprjm_conv
	,cdc.mwsrdm
	,cdc.mwsrdm_conv
	,cdc.mwpns
FROM stg_jde.tmp_upsert_f3412_cdc cdc
LEFT OUTER JOIN rep_jde.f3412_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f3412_new AF:{{ task_instance_key_str }}' ) 