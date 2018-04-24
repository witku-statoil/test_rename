-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f0150_new
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
	,maostp
	,mapa8
	,maan8
	,madss7
	,mabefd
	,mabefd_conv
	,maeefd
	,maeefd_conv
	,marmk
	,marmk_conv
	,mauser
	,mauser_conv
	,maupmj
	,maupmj_conv
	,mapid
	,mapid_conv
	,majobn
	,majobn_conv
	,maupmt
	,masyncs
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
	,cdc.maostp
	,cdc.mapa8
	,cdc.maan8
	,cdc.madss7
	,cdc.mabefd
	,cdc.mabefd_conv
	,cdc.maeefd
	,cdc.maeefd_conv
	,cdc.marmk
	,cdc.marmk_conv
	,cdc.mauser
	,cdc.mauser_conv
	,cdc.maupmj
	,cdc.maupmj_conv
	,cdc.mapid
	,cdc.mapid_conv
	,cdc.majobn
	,cdc.majobn_conv
	,cdc.maupmt
	,cdc.masyncs
FROM stg_jde_qat.tmp_upsert_f0150_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f0150_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f0150_new AF:{{ task_instance_key_str }}' ) 