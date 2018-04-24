-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f41500_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[ppmcu] = cdc.[ppmcu]
	,[ppmcu_conv] = cdc.[ppmcu_conv]
	,[pptkid] = cdc.[pptkid]
	,[pptkid_conv] = cdc.[pptkid_conv]
	,[ppdl01] = cdc.[ppdl01]
	,[ppdl01_conv] = cdc.[ppdl01_conv]
	,[pptklo] = cdc.[pptklo]
	,[pptuse] = cdc.[pptuse]
	,[pptkty] = cdc.[pptkty]
	,[pptkcp] = cdc.[pptkcp]
	,[pptkcp_conv] = cdc.[pptkcp_conv]
	,[ppbum1] = cdc.[ppbum1]
	,[pphttk] = cdc.[pphttk]
	,[pphttk_conv] = cdc.[pphttk_conv]
	,[ppprez] = cdc.[ppprez]
	,[ppprez_conv] = cdc.[ppprez_conv]
	,[ppidte] = cdc.[ppidte]
	,[ppidte_conv] = cdc.[ppidte_conv]
	,[ppdtcl] = cdc.[ppdtcl]
	,[ppdtcl_conv] = cdc.[ppdtcl_conv]
	,[ppstrp] = cdc.[ppstrp]
	,[ppdipt] = cdc.[ppdipt]
	,[ppgamt] = cdc.[ppgamt]
	,[ppfltr] = cdc.[ppfltr]
	,[pptexp] = cdc.[pptexp]
	,[pptexp_conv] = cdc.[pptexp_conv]
	,[pptstu] = cdc.[pptstu]
	,[ppcutk] = cdc.[ppcutk]
	,[ppcutk_conv] = cdc.[ppcutk_conv]
	,[ppscom] = cdc.[ppscom]
	,[pptdia] = cdc.[pptdia]
	,[pptdia_conv] = cdc.[pptdia_conv]
	,[ppbum2] = cdc.[ppbum2]
	,[ppthgt] = cdc.[ppthgt]
	,[ppthgt_conv] = cdc.[ppthgt_conv]
	,[ppbum3] = cdc.[ppbum3]
	,[ppstem] = cdc.[ppstem]
	,[ppstem_conv] = cdc.[ppstem_conv]
	,[ppstpu] = cdc.[ppstpu]
	,[pprefh] = cdc.[pprefh]
	,[pprefh_conv] = cdc.[pprefh_conv]
	,[ppbum4] = cdc.[ppbum4]
	,[pprwgh] = cdc.[pprwgh]
	,[pprwgh_conv] = cdc.[pprwgh_conv]
	,[ppbum5] = cdc.[ppbum5]
	,[ppflht] = cdc.[ppflht]
	,[ppflht_conv] = cdc.[ppflht_conv]
	,[ppbum6] = cdc.[ppbum6]
	,[ppunpv] = cdc.[ppunpv]
	,[ppunpv_conv] = cdc.[ppunpv_conv]
	,[ppbum7] = cdc.[ppbum7]
	,[pppipv] = cdc.[pppipv]
	,[pppipv_conv] = cdc.[pppipv_conv]
	,[ppbum8] = cdc.[ppbum8]
	,[ppdisv] = cdc.[ppdisv]
	,[ppdisv_conv] = cdc.[ppdisv_conv]
	,[ppbum9] = cdc.[ppbum9]
	,[ppdihr] = cdc.[ppdihr]
	,[ppdihr_conv] = cdc.[ppdihr_conv]
	,[ppdhru] = cdc.[ppdhru]
	,[ppfirh] = cdc.[ppfirh]
	,[ppfirh_conv] = cdc.[ppfirh_conv]
	,[ppfrhu] = cdc.[ppfrhu]
	,[pplswn] = cdc.[pplswn]
	,[pplswn_conv] = cdc.[pplswn_conv]
	,[ppbum0] = cdc.[ppbum0]
	,[pplosc] = cdc.[pplosc]
	,[pplosc_conv] = cdc.[pplosc_conv]
	,[pphisc] = cdc.[pphisc]
	,[pphisc_conv] = cdc.[pphisc_conv]
	,[ppitm] = cdc.[ppitm]
	,[ppitml] = cdc.[ppitml]
	,[pppcsd] = cdc.[pppcsd]
	,[pppcsd_conv] = cdc.[pppcsd_conv]
	,[pprtob] = cdc.[pprtob]
	,[ppurcd] = cdc.[ppurcd]
	,[ppurcd_conv] = cdc.[ppurcd_conv]
	,[ppurat] = cdc.[ppurat]
	,[ppurat_conv] = cdc.[ppurat_conv]
	,[ppurab] = cdc.[ppurab]
	,[ppurab_conv] = cdc.[ppurab_conv]
	,[ppurrf] = cdc.[ppurrf]
	,[ppurrf_conv] = cdc.[ppurrf_conv]
	,[ppurdt] = cdc.[ppurdt]
	,[ppurdt_conv] = cdc.[ppurdt_conv]
	,[ppuser] = cdc.[ppuser]
	,[ppuser_conv] = cdc.[ppuser_conv]
	,[pppid] = cdc.[pppid]
	,[pppid_conv] = cdc.[pppid_conv]
	,[ppjobn] = cdc.[ppjobn]
	,[ppjobn_conv] = cdc.[ppjobn_conv]
	,[ppupmj] = cdc.[ppupmj]
	,[ppupmj_conv] = cdc.[ppupmj_conv]
	,[pptday] = cdc.[pptday]
	,[pptnkn] = cdc.[pptnkn]
	,[pptnkn_conv] = cdc.[pptnkn_conv]
	,[ppexpf] = cdc.[ppexpf]
	,[ppexpf_conv] = cdc.[ppexpf_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f41500_cdc cdc
WHERE
    rep_jde.f41500_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f41500_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f41500_new AF:{{ task_instance_key_str }}' ) 
