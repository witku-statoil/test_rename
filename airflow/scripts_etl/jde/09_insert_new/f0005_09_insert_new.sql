-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f0005_new
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
	,drsy
	,drrt
	,drrt_conv
	,drky
	,drky_conv
	,drdl01
	,drdl01_conv
	,drdl02
	,drdl02_conv
	,drsphd
	,drsphd_conv
	,drudco
	,drhrdc
	,drhrdc_conv
	,druser
	,druser_conv
	,drpid
	,drpid_conv
	,drupmj
	,drupmj_conv
	,drjobn
	,drjobn_conv
	,drupmt
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
	,cdc.drsy
	,cdc.drrt
	,cdc.drrt_conv
	,cdc.drky
	,cdc.drky_conv
	,cdc.drdl01
	,cdc.drdl01_conv
	,cdc.drdl02
	,cdc.drdl02_conv
	,cdc.drsphd
	,cdc.drsphd_conv
	,cdc.drudco
	,cdc.drhrdc
	,cdc.drhrdc_conv
	,cdc.druser
	,cdc.druser_conv
	,cdc.drpid
	,cdc.drpid_conv
	,cdc.drupmj
	,cdc.drupmj_conv
	,cdc.drjobn
	,cdc.drjobn_conv
	,cdc.drupmt
FROM stg_jde.tmp_upsert_f0005_cdc cdc
LEFT OUTER JOIN rep_jde.f0005_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f0005_new AF:{{ task_instance_key_str }}' ) 