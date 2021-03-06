-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f38112_new
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
	,dxdoco
	,dxdcto
	,dxkcoo
	,dxkcoo_conv
	,dxlnid
	,dxlnid_conv
	,dxdmct
	,dxdmct_conv
	,dxdmcs
	,dxmcu
	,dxmcu_conv
	,dxseq
	,dxpsr
	,dxpsr_conv
	,dxpsry
	,dxqcom
	,dxqcom_conv
	,dxcnqy
	,dxaa
	,dxaa_conv
	,dxcrcd
	,dxcrcd_conv
	,dxuom
	,dxtrdj
	,dxtrdj_conv
	,dxtv01
	,dxtv01_conv
	,dxtv02
	,dxtv02_conv
	,dxtv03
	,dxtv03_conv
	,dxurcd
	,dxurcd_conv
	,dxurdt
	,dxurdt_conv
	,dxurab
	,dxurab_conv
	,dxurrf
	,dxurrf_conv
	,dxuser
	,dxuser_conv
	,dxpid
	,dxpid_conv
	,dxjobn
	,dxjobn_conv
	,dxupmj
	,dxupmj_conv
	,dxtday
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
	,cdc.dxdoco
	,cdc.dxdcto
	,cdc.dxkcoo
	,cdc.dxkcoo_conv
	,cdc.dxlnid
	,cdc.dxlnid_conv
	,cdc.dxdmct
	,cdc.dxdmct_conv
	,cdc.dxdmcs
	,cdc.dxmcu
	,cdc.dxmcu_conv
	,cdc.dxseq
	,cdc.dxpsr
	,cdc.dxpsr_conv
	,cdc.dxpsry
	,cdc.dxqcom
	,cdc.dxqcom_conv
	,cdc.dxcnqy
	,cdc.dxaa
	,cdc.dxaa_conv
	,cdc.dxcrcd
	,cdc.dxcrcd_conv
	,cdc.dxuom
	,cdc.dxtrdj
	,cdc.dxtrdj_conv
	,cdc.dxtv01
	,cdc.dxtv01_conv
	,cdc.dxtv02
	,cdc.dxtv02_conv
	,cdc.dxtv03
	,cdc.dxtv03_conv
	,cdc.dxurcd
	,cdc.dxurcd_conv
	,cdc.dxurdt
	,cdc.dxurdt_conv
	,cdc.dxurab
	,cdc.dxurab_conv
	,cdc.dxurrf
	,cdc.dxurrf_conv
	,cdc.dxuser
	,cdc.dxuser_conv
	,cdc.dxpid
	,cdc.dxpid_conv
	,cdc.dxjobn
	,cdc.dxjobn_conv
	,cdc.dxupmj
	,cdc.dxupmj_conv
	,cdc.dxtday
FROM stg_jde_qat.tmp_upsert_f38112_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f38112_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f38112_new AF:{{ task_instance_key_str }}' ) 