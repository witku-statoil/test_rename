-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f41514_new
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
	,rhmcu
	,rhmcu_conv
	,rhitm
	,rhtkid
	,rhtkid_conv
	,rhopdt
	,rhopdt_conv
	,rhrttm
	,rhincn
	,rhincn_conv
	,rhbum0
	,rhoutg
	,rhoutg_conv
	,rhbum1
	,rhopns
	,rhopns_conv
	,rhbum2
	,rhclos
	,rhclos_conv
	,rhbum3
	,rhglqt
	,rhglqt_conv
	,rhbum4
	,rhuser
	,rhuser_conv
	,rhpid
	,rhpid_conv
	,rhjobn
	,rhjobn_conv
	,rhupmj
	,rhupmj_conv
	,rhtday
	,rhinam
	,rhinam_conv
	,rhhum1
	,rhinwg
	,rhinwg_conv
	,rhhum2
	,rhogam
	,rhogam_conv
	,rhhum3
	,rhogwg
	,rhogwg_conv
	,rhhum4
	,rhosam
	,rhosam_conv
	,rhhum5
	,rhoswg
	,rhoswg_conv
	,rhhum6
	,rhcsam
	,rhcsam_conv
	,rhhum7
	,rhcswg
	,rhcswg_conv
	,rhhum8
	,rhglam
	,rhglam_conv
	,rhhum9
	,rhglwg
	,rhglwg_conv
	,rhhuma
	,rhurrf
	,rhurrf_conv
	,rhurdt
	,rhurdt_conv
	,rhurcd
	,rhurcd_conv
	,rhurat
	,rhurat_conv
	,rhurab
	,rhurab_conv
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
	,cdc.rhmcu
	,cdc.rhmcu_conv
	,cdc.rhitm
	,cdc.rhtkid
	,cdc.rhtkid_conv
	,cdc.rhopdt
	,cdc.rhopdt_conv
	,cdc.rhrttm
	,cdc.rhincn
	,cdc.rhincn_conv
	,cdc.rhbum0
	,cdc.rhoutg
	,cdc.rhoutg_conv
	,cdc.rhbum1
	,cdc.rhopns
	,cdc.rhopns_conv
	,cdc.rhbum2
	,cdc.rhclos
	,cdc.rhclos_conv
	,cdc.rhbum3
	,cdc.rhglqt
	,cdc.rhglqt_conv
	,cdc.rhbum4
	,cdc.rhuser
	,cdc.rhuser_conv
	,cdc.rhpid
	,cdc.rhpid_conv
	,cdc.rhjobn
	,cdc.rhjobn_conv
	,cdc.rhupmj
	,cdc.rhupmj_conv
	,cdc.rhtday
	,cdc.rhinam
	,cdc.rhinam_conv
	,cdc.rhhum1
	,cdc.rhinwg
	,cdc.rhinwg_conv
	,cdc.rhhum2
	,cdc.rhogam
	,cdc.rhogam_conv
	,cdc.rhhum3
	,cdc.rhogwg
	,cdc.rhogwg_conv
	,cdc.rhhum4
	,cdc.rhosam
	,cdc.rhosam_conv
	,cdc.rhhum5
	,cdc.rhoswg
	,cdc.rhoswg_conv
	,cdc.rhhum6
	,cdc.rhcsam
	,cdc.rhcsam_conv
	,cdc.rhhum7
	,cdc.rhcswg
	,cdc.rhcswg_conv
	,cdc.rhhum8
	,cdc.rhglam
	,cdc.rhglam_conv
	,cdc.rhhum9
	,cdc.rhglwg
	,cdc.rhglwg_conv
	,cdc.rhhuma
	,cdc.rhurrf
	,cdc.rhurrf_conv
	,cdc.rhurdt
	,cdc.rhurdt_conv
	,cdc.rhurcd
	,cdc.rhurcd_conv
	,cdc.rhurat
	,cdc.rhurat_conv
	,cdc.rhurab
	,cdc.rhurab_conv
FROM stg_jde_qat.tmp_upsert_f41514_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f41514_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f41514_new AF:{{ task_instance_key_str }}' ) 