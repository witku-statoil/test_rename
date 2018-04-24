-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f38012_new
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
	,dpdmct
	,dpdmct_conv
	,dpdmcs
	,dpitm
	,dpdto
	,dpdes
	,dpdes_conv
	,dpdesy
	,dpseq
	,dppsr
	,dppsr_conv
	,dppsry
	,dpcnqy
	,dpcnqt
	,dpcnqt_conv
	,dpaa
	,dpaa_conv
	,dpcrcd
	,dpcrcd_conv
	,dpuom
	,dpminq
	,dpminq_conv
	,dpmaxq
	,dpmaxq_conv
	,dpqbal
	,dpqbal_conv
	,dpqcom
	,dpqcom_conv
	,dpeftj
	,dpeftj_conv
	,dpexdj
	,dpexdj_conv
	,dptv01
	,dptv01_conv
	,dptv02
	,dptv02_conv
	,dptv03
	,dptv03_conv
	,dpqty1
	,dpqty1_conv
	,dptvum
	,dpurcd
	,dpurcd_conv
	,dpurdt
	,dpurdt_conv
	,dpurab
	,dpurab_conv
	,dpurrf
	,dpurrf_conv
	,dpuser
	,dpuser_conv
	,dppid
	,dppid_conv
	,dpjobn
	,dpjobn_conv
	,dpupmj
	,dpupmj_conv
	,dptday
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
	,cdc.dpdmct
	,cdc.dpdmct_conv
	,cdc.dpdmcs
	,cdc.dpitm
	,cdc.dpdto
	,cdc.dpdes
	,cdc.dpdes_conv
	,cdc.dpdesy
	,cdc.dpseq
	,cdc.dppsr
	,cdc.dppsr_conv
	,cdc.dppsry
	,cdc.dpcnqy
	,cdc.dpcnqt
	,cdc.dpcnqt_conv
	,cdc.dpaa
	,cdc.dpaa_conv
	,cdc.dpcrcd
	,cdc.dpcrcd_conv
	,cdc.dpuom
	,cdc.dpminq
	,cdc.dpminq_conv
	,cdc.dpmaxq
	,cdc.dpmaxq_conv
	,cdc.dpqbal
	,cdc.dpqbal_conv
	,cdc.dpqcom
	,cdc.dpqcom_conv
	,cdc.dpeftj
	,cdc.dpeftj_conv
	,cdc.dpexdj
	,cdc.dpexdj_conv
	,cdc.dptv01
	,cdc.dptv01_conv
	,cdc.dptv02
	,cdc.dptv02_conv
	,cdc.dptv03
	,cdc.dptv03_conv
	,cdc.dpqty1
	,cdc.dpqty1_conv
	,cdc.dptvum
	,cdc.dpurcd
	,cdc.dpurcd_conv
	,cdc.dpurdt
	,cdc.dpurdt_conv
	,cdc.dpurab
	,cdc.dpurab_conv
	,cdc.dpurrf
	,cdc.dpurrf_conv
	,cdc.dpuser
	,cdc.dpuser_conv
	,cdc.dppid
	,cdc.dppid_conv
	,cdc.dpjobn
	,cdc.dpjobn_conv
	,cdc.dpupmj
	,cdc.dpupmj_conv
	,cdc.dptday
FROM stg_jde_qat.tmp_upsert_f38012_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f38012_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f38012_new AF:{{ task_instance_key_str }}' ) 