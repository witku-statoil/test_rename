-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f41011_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[pkitm] = cdc.[pkitm]
	,[pkpdgr] = cdc.[pkpdgr]
	,[pkdsgp] = cdc.[pkdsgp]
	,[pkdntb] = cdc.[pkdntb]
	,[pkstcn] = cdc.[pkstcn]
	,[pkrptm] = cdc.[pkrptm]
	,[pkrqtc] = cdc.[pkrqtc]
	,[pkrqtc_conv] = cdc.[pkrqtc_conv]
	,[pklpgp] = cdc.[pklpgp]
	,[pklpgp_conv] = cdc.[pklpgp_conv]
	,[pkcavp] = cdc.[pkcavp]
	,[pkcavp_conv] = cdc.[pkcavp_conv]
	,[pkdnty] = cdc.[pkdnty]
	,[pkdnty_conv] = cdc.[pkdnty_conv]
	,[pkdntp] = cdc.[pkdntp]
	,[pkdetp] = cdc.[pkdetp]
	,[pkdetp_conv] = cdc.[pkdetp_conv]
	,[pkdtpu] = cdc.[pkdtpu]
	,[pkcoex] = cdc.[pkcoex]
	,[pkcoex_conv] = cdc.[pkcoex_conv]
	,[pktmmn] = cdc.[pktmmn]
	,[pktmmn_conv] = cdc.[pktmmn_conv]
	,[pktpum] = cdc.[pktpum]
	,[pktmmx] = cdc.[pktmmx]
	,[pktmmx_conv] = cdc.[pktmmx_conv]
	,[pktpux] = cdc.[pktpux]
	,[pkdsmn] = cdc.[pkdsmn]
	,[pkdsmn_conv] = cdc.[pkdsmn_conv]
	,[pkdntm] = cdc.[pkdntm]
	,[pkdnmx] = cdc.[pkdnmx]
	,[pkdnmx_conv] = cdc.[pkdnmx_conv]
	,[pkdntx] = cdc.[pkdntx]
	,[pklpgv] = cdc.[pklpgv]
	,[pklpgv_conv] = cdc.[pklpgv_conv]
	,[pktpu1] = cdc.[pktpu1]
	,[pkmnvc] = cdc.[pkmnvc]
	,[pkmnvc_conv] = cdc.[pkmnvc_conv]
	,[pkmxvc] = cdc.[pkmxvc]
	,[pkmxvc_conv] = cdc.[pkmxvc_conv]
	,[pkurcd] = cdc.[pkurcd]
	,[pkurcd_conv] = cdc.[pkurcd_conv]
	,[pkurat] = cdc.[pkurat]
	,[pkurat_conv] = cdc.[pkurat_conv]
	,[pkurab] = cdc.[pkurab]
	,[pkurab_conv] = cdc.[pkurab_conv]
	,[pkurrf] = cdc.[pkurrf]
	,[pkurrf_conv] = cdc.[pkurrf_conv]
	,[pkurdt] = cdc.[pkurdt]
	,[pkurdt_conv] = cdc.[pkurdt_conv]
	,[pkuser] = cdc.[pkuser]
	,[pkuser_conv] = cdc.[pkuser_conv]
	,[pkpid] = cdc.[pkpid]
	,[pkpid_conv] = cdc.[pkpid_conv]
	,[pkjobn] = cdc.[pkjobn]
	,[pkjobn_conv] = cdc.[pkjobn_conv]
	,[pkupmj] = cdc.[pkupmj]
	,[pkupmj_conv] = cdc.[pkupmj_conv]
	,[pktday] = cdc.[pktday]
	,[pkrtob] = cdc.[pkrtob] -- exclude distribution column
FROM stg_jde.tmp_upsert_f41011_cdc cdc
WHERE
    rep_jde.f41011_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f41011_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f41011_new AF:{{ task_instance_key_str }}' ) 
