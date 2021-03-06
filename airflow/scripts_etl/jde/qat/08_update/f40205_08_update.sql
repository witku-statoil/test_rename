-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f40205_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[lflnty] = cdc.[lflnty]
	,[lflnty_conv] = cdc.[lflnty_conv]
	,[lflnds] = cdc.[lflnds]
	,[lflnds_conv] = cdc.[lflnds_conv]
	,[lflnd2] = cdc.[lflnd2]
	,[lflnd2_conv] = cdc.[lflnd2_conv]
	,[lfgli] = cdc.[lfgli]
	,[lfgli_conv] = cdc.[lfgli_conv]
	,[lfivi] = cdc.[lfivi]
	,[lfari] = cdc.[lfari]
	,[lfari_conv] = cdc.[lfari_conv]
	,[lfapi] = cdc.[lfapi]
	,[lfapi_conv] = cdc.[lfapi_conv]
	,[lfrsgn] = cdc.[lfrsgn]
	,[lfrsgn_conv] = cdc.[lfrsgn_conv]
	,[lftxyn] = cdc.[lftxyn]
	,[lftxyn_conv] = cdc.[lftxyn_conv]
	,[lfprft] = cdc.[lfprft]
	,[lfprft_conv] = cdc.[lfprft_conv]
	,[lfcdsc] = cdc.[lfcdsc]
	,[lfcdsc_conv] = cdc.[lfcdsc_conv]
	,[lftx01] = cdc.[lftx01]
	,[lftx02] = cdc.[lftx02]
	,[lfglc] = cdc.[lfglc]
	,[lfglc_conv] = cdc.[lfglc_conv]
	,[lfpdc1] = cdc.[lfpdc1]
	,[lfpdc1_conv] = cdc.[lfpdc1_conv]
	,[lfpdc2] = cdc.[lfpdc2]
	,[lfpdc2_conv] = cdc.[lfpdc2_conv]
	,[lfpdc3] = cdc.[lfpdc3]
	,[lfpdc3_conv] = cdc.[lfpdc3_conv]
	,[lfpdc4] = cdc.[lfpdc4]
	,[lfpdc4_conv] = cdc.[lfpdc4_conv]
	,[lfidc1] = cdc.[lfidc1]
	,[lfidc1_conv] = cdc.[lfidc1_conv]
	,[lfidc2] = cdc.[lfidc2]
	,[lfidc3] = cdc.[lfidc3]
	,[lfidc3_conv] = cdc.[lfidc3_conv]
	,[lfidc4] = cdc.[lfidc4]
	,[lfidc4_conv] = cdc.[lfidc4_conv]
	,[lfcsj] = cdc.[lfcsj]
	,[lfdcto] = cdc.[lfdcto]
	,[lfart] = cdc.[lfart]
	,[lfart_conv] = cdc.[lfart_conv]
	,[lfaft] = cdc.[lfaft]
	,[lfaft_conv] = cdc.[lfaft_conv]
	,[lfgwo] = cdc.[lfgwo]
	,[lfgwo_conv] = cdc.[lfgwo_conv]
	,[lfexvr] = cdc.[lfexvr]
	,[lfexvr_conv] = cdc.[lfexvr_conv]
	,[lfcmi] = cdc.[lfcmi]
	,[lfcmi_conv] = cdc.[lfcmi_conv]
	,[lfprrq] = cdc.[lfprrq]
	,[lfprrq_conv] = cdc.[lfprrq_conv]
	,[lfev01] = cdc.[lfev01]
	,[lfev01_conv] = cdc.[lfev01_conv]
	,[lfev02] = cdc.[lfev02]
	,[lfev02_conv] = cdc.[lfev02_conv]
	,[lfev03] = cdc.[lfev03]
	,[lfev03_conv] = cdc.[lfev03_conv]
	,[lfev04] = cdc.[lfev04]
	,[lfev04_conv] = cdc.[lfev04_conv]
	,[lfev05] = cdc.[lfev05]
	,[lfev05_conv] = cdc.[lfev05_conv]
	,[lfnewr] = cdc.[lfnewr]
	,[lfsruf] = cdc.[lfsruf]
	,[lfsruf_conv] = cdc.[lfsruf_conv]
	,[lfnbru] = cdc.[lfnbru] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f40205_cdc cdc
WHERE
    rep_jde_qat.f40205_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f40205_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f40205_new AF:{{ task_instance_key_str }}' ) 
