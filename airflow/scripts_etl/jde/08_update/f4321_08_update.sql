-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f4321_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[pbkcoo] = cdc.[pbkcoo]
	,[pbkcoo_conv] = cdc.[pbkcoo_conv]
	,[pbdoco] = cdc.[pbdoco]
	,[pbdcto] = cdc.[pbdcto]
	,[pbsfxo] = cdc.[pbsfxo]
	,[pbsfxo_conv] = cdc.[pbsfxo_conv]
	,[pblnid] = cdc.[pblnid]
	,[pblnid_conv] = cdc.[pblnid_conv]
	,[pbdlan] = cdc.[pbdlan]
	,[pbdlan_conv] = cdc.[pbdlan_conv]
	,[pbcdf1] = cdc.[pbcdf1]
	,[pbcdf1_conv] = cdc.[pbcdf1_conv]
	,[pbcdf2] = cdc.[pbcdf2]
	,[pbcdf2_conv] = cdc.[pbcdf2_conv]
	,[pbcdf3] = cdc.[pbcdf3]
	,[pbcdf3_conv] = cdc.[pbcdf3_conv]
	,[pbcdf4] = cdc.[pbcdf4]
	,[pbcdf4_conv] = cdc.[pbcdf4_conv]
	,[pbvsd] = cdc.[pbvsd]
	,[pbvsd_conv] = cdc.[pbvsd_conv]
	,[pbvsw] = cdc.[pbvsw]
	,[pbvsw_conv] = cdc.[pbvsw_conv]
	,[pbvsm] = cdc.[pbvsm]
	,[pbvsm_conv] = cdc.[pbvsm_conv]
	,[pbcrec] = cdc.[pbcrec]
	,[pbcrec_conv] = cdc.[pbcrec_conv]
	,[pbcfro] = cdc.[pbcfro]
	,[pbcfro_conv] = cdc.[pbcfro_conv]
	,[pbcfab] = cdc.[pbcfab]
	,[pbcfab_conv] = cdc.[pbcfab_conv]
	,[pbcraw] = cdc.[pbcraw]
	,[pbcraw_conv] = cdc.[pbcraw_conv]
	,[pbdrqj] = cdc.[pbdrqj]
	,[pbdrqj_conv] = cdc.[pbdrqj_conv]
	,[pbfrdj] = cdc.[pbfrdj]
	,[pbfrdj_conv] = cdc.[pbfrdj_conv]
	,[pblrdj] = cdc.[pblrdj]
	,[pblrdj_conv] = cdc.[pblrdj_conv]
	,[pbdoc] = cdc.[pbdoc]
	,[pbdct] = cdc.[pbdct]
	,[pbvrn] = cdc.[pbvrn]
	,[pbvrn_conv] = cdc.[pbvrn_conv]
	,[pbvsst] = cdc.[pbvsst]
	,[pbrjyn] = cdc.[pbrjyn]
	,[pbrjyn_conv] = cdc.[pbrjyn_conv]
	,[pborfd] = cdc.[pborfd]
	,[pborfd_conv] = cdc.[pborfd_conv]
	,[pblrcj] = cdc.[pblrcj]
	,[pblrcj_conv] = cdc.[pblrcj_conv]
	,[pblrqt] = cdc.[pblrqt]
	,[pblrqt_conv] = cdc.[pblrqt_conv]
	,[pbuopn] = cdc.[pbuopn]
	,[pbuopn_conv] = cdc.[pbuopn_conv]
	,[pburec] = cdc.[pburec]
	,[pburec_conv] = cdc.[pburec_conv]
	,[pbvssp] = cdc.[pbvssp]
	,[pbshlt] = cdc.[pbshlt]
	,[pbshlt_conv] = cdc.[pbshlt_conv]
	,[pbshqt] = cdc.[pbshqt]
	,[pbshqt_conv] = cdc.[pbshqt_conv]
	,[pbupc] = cdc.[pbupc]
	,[pbupc_conv] = cdc.[pbupc_conv]
	,[pburcd] = cdc.[pburcd]
	,[pburcd_conv] = cdc.[pburcd_conv]
	,[pburdt] = cdc.[pburdt]
	,[pburdt_conv] = cdc.[pburdt_conv]
	,[pburat] = cdc.[pburat]
	,[pburat_conv] = cdc.[pburat_conv]
	,[pburab] = cdc.[pburab]
	,[pburab_conv] = cdc.[pburab_conv]
	,[pburrf] = cdc.[pburrf]
	,[pburrf_conv] = cdc.[pburrf_conv]
	,[pbuser] = cdc.[pbuser]
	,[pbuser_conv] = cdc.[pbuser_conv]
	,[pbpid] = cdc.[pbpid]
	,[pbpid_conv] = cdc.[pbpid_conv]
	,[pbjobn] = cdc.[pbjobn]
	,[pbjobn_conv] = cdc.[pbjobn_conv]
	,[pbupmj] = cdc.[pbupmj]
	,[pbupmj_conv] = cdc.[pbupmj_conv]
	,[pbtday] = cdc.[pbtday]
	,[pbvlot] = cdc.[pbvlot]
	,[pbvlot_conv] = cdc.[pbvlot_conv]
	,[pbrcpd] = cdc.[pbrcpd]
	,[pbrcpd_conv] = cdc.[pbrcpd_conv]
	,[pbmxqt] = cdc.[pbmxqt]
	,[pbmxqt_conv] = cdc.[pbmxqt_conv]
	,[pbmwdh] = cdc.[pbmwdh]
	,[pbltlv] = cdc.[pbltlv]
	,[pbltlv_conv] = cdc.[pbltlv_conv]
	,[pbdcap] = cdc.[pbdcap]
	,[pbda08] = cdc.[pbda08]
	,[pbda08_conv] = cdc.[pbda08_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f4321_cdc cdc
WHERE
    rep_jde.f4321_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f4321_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f4321_new AF:{{ task_instance_key_str }}' ) 
