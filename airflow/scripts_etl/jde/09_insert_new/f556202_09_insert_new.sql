-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f556202_new
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
	,emctr
	,emco
	,emco_conv
	,emmcu
	,emmcu_conv
	,emlocn
	,emlocn_conv
	,emev01
	,emev01_conv
	,emev02
	,emev02_conv
	,emmath01
	,emmath01_conv
	,emmath02
	,emmath02_conv
	,emdl01
	,emdl01_conv
	,emdl02
	,emdl02_conv
	,emtrdj
	,emtrdj_conv
	,empddj
	,empddj_conv
	,emuser
	,emuser_conv
	,empid
	,empid_conv
	,emjobn
	,emjobn_conv
	,emupmj
	,emupmj_conv
	,emtday
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
	,cdc.emctr
	,cdc.emco
	,cdc.emco_conv
	,cdc.emmcu
	,cdc.emmcu_conv
	,cdc.emlocn
	,cdc.emlocn_conv
	,cdc.emev01
	,cdc.emev01_conv
	,cdc.emev02
	,cdc.emev02_conv
	,cdc.emmath01
	,cdc.emmath01_conv
	,cdc.emmath02
	,cdc.emmath02_conv
	,cdc.emdl01
	,cdc.emdl01_conv
	,cdc.emdl02
	,cdc.emdl02_conv
	,cdc.emtrdj
	,cdc.emtrdj_conv
	,cdc.empddj
	,cdc.empddj_conv
	,cdc.emuser
	,cdc.emuser_conv
	,cdc.empid
	,cdc.empid_conv
	,cdc.emjobn
	,cdc.emjobn_conv
	,cdc.emupmj
	,cdc.emupmj_conv
	,cdc.emtday
FROM stg_jde.tmp_upsert_f556202_cdc cdc
LEFT OUTER JOIN rep_jde.f556202_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f556202_new AF:{{ task_instance_key_str }}' ) 