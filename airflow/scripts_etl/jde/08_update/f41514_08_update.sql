-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f41514_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[rhmcu] = cdc.[rhmcu]
	,[rhmcu_conv] = cdc.[rhmcu_conv]
	,[rhitm] = cdc.[rhitm]
	,[rhtkid] = cdc.[rhtkid]
	,[rhtkid_conv] = cdc.[rhtkid_conv]
	,[rhopdt] = cdc.[rhopdt]
	,[rhopdt_conv] = cdc.[rhopdt_conv]
	,[rhrttm] = cdc.[rhrttm]
	,[rhincn] = cdc.[rhincn]
	,[rhincn_conv] = cdc.[rhincn_conv]
	,[rhbum0] = cdc.[rhbum0]
	,[rhoutg] = cdc.[rhoutg]
	,[rhoutg_conv] = cdc.[rhoutg_conv]
	,[rhbum1] = cdc.[rhbum1]
	,[rhopns] = cdc.[rhopns]
	,[rhopns_conv] = cdc.[rhopns_conv]
	,[rhbum2] = cdc.[rhbum2]
	,[rhclos] = cdc.[rhclos]
	,[rhclos_conv] = cdc.[rhclos_conv]
	,[rhbum3] = cdc.[rhbum3]
	,[rhglqt] = cdc.[rhglqt]
	,[rhglqt_conv] = cdc.[rhglqt_conv]
	,[rhbum4] = cdc.[rhbum4]
	,[rhuser] = cdc.[rhuser]
	,[rhuser_conv] = cdc.[rhuser_conv]
	,[rhpid] = cdc.[rhpid]
	,[rhpid_conv] = cdc.[rhpid_conv]
	,[rhjobn] = cdc.[rhjobn]
	,[rhjobn_conv] = cdc.[rhjobn_conv]
	,[rhupmj] = cdc.[rhupmj]
	,[rhupmj_conv] = cdc.[rhupmj_conv]
	,[rhtday] = cdc.[rhtday]
	,[rhinam] = cdc.[rhinam]
	,[rhinam_conv] = cdc.[rhinam_conv]
	,[rhhum1] = cdc.[rhhum1]
	,[rhinwg] = cdc.[rhinwg]
	,[rhinwg_conv] = cdc.[rhinwg_conv]
	,[rhhum2] = cdc.[rhhum2]
	,[rhogam] = cdc.[rhogam]
	,[rhogam_conv] = cdc.[rhogam_conv]
	,[rhhum3] = cdc.[rhhum3]
	,[rhogwg] = cdc.[rhogwg]
	,[rhogwg_conv] = cdc.[rhogwg_conv]
	,[rhhum4] = cdc.[rhhum4]
	,[rhosam] = cdc.[rhosam]
	,[rhosam_conv] = cdc.[rhosam_conv]
	,[rhhum5] = cdc.[rhhum5]
	,[rhoswg] = cdc.[rhoswg]
	,[rhoswg_conv] = cdc.[rhoswg_conv]
	,[rhhum6] = cdc.[rhhum6]
	,[rhcsam] = cdc.[rhcsam]
	,[rhcsam_conv] = cdc.[rhcsam_conv]
	,[rhhum7] = cdc.[rhhum7]
	,[rhcswg] = cdc.[rhcswg]
	,[rhcswg_conv] = cdc.[rhcswg_conv]
	,[rhhum8] = cdc.[rhhum8]
	,[rhglam] = cdc.[rhglam]
	,[rhglam_conv] = cdc.[rhglam_conv]
	,[rhhum9] = cdc.[rhhum9]
	,[rhglwg] = cdc.[rhglwg]
	,[rhglwg_conv] = cdc.[rhglwg_conv]
	,[rhhuma] = cdc.[rhhuma]
	,[rhurrf] = cdc.[rhurrf]
	,[rhurrf_conv] = cdc.[rhurrf_conv]
	,[rhurdt] = cdc.[rhurdt]
	,[rhurdt_conv] = cdc.[rhurdt_conv]
	,[rhurcd] = cdc.[rhurcd]
	,[rhurcd_conv] = cdc.[rhurcd_conv]
	,[rhurat] = cdc.[rhurat]
	,[rhurat_conv] = cdc.[rhurat_conv]
	,[rhurab] = cdc.[rhurab]
	,[rhurab_conv] = cdc.[rhurab_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f41514_cdc cdc
WHERE
    rep_jde.f41514_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f41514_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f41514_new AF:{{ task_instance_key_str }}' ) 
