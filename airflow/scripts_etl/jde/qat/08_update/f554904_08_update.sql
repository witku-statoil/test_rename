-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f554904_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[fdvr01] = cdc.[fdvr01]
	,[fdvr01_conv] = cdc.[fdvr01_conv]
	,[fdy55fcode] = cdc.[fdy55fcode]
	,[fdy55msgno] = cdc.[fdy55msgno]
	,[fdy55msgno_conv] = cdc.[fdy55msgno_conv]
	,[fdy55insdj] = cdc.[fdy55insdj]
	,[fdy55insdj_conv] = cdc.[fdy55insdj_conv]
	,[fdy55insdt] = cdc.[fdy55insdt]
	,[fdldnm] = cdc.[fdldnm]
	,[fdlnid] = cdc.[fdlnid]
	,[fdlnid_conv] = cdc.[fdlnid_conv]
	,[fdy55fdst] = cdc.[fdy55fdst]
	,[fdumd1] = cdc.[fdumd1]
	,[fdy55fwait] = cdc.[fdy55fwait]
	,[fdfrcg] = cdc.[fdfrcg]
	,[fdfrcg_conv] = cdc.[fdfrcg_conv]
	,[fdy55fwaitc] = cdc.[fdy55fwaitc]
	,[fdy55fwaitc_conv] = cdc.[fdy55fwaitc_conv]
	,[fdedbt] = cdc.[fdedbt]
	,[fdedbt_conv] = cdc.[fdedbt_conv]
	,[fdedtn] = cdc.[fdedtn]
	,[fdedtn_conv] = cdc.[fdedtn_conv]
	,[fdflag] = cdc.[fdflag]
	,[fdflag_conv] = cdc.[fdflag_conv]
	,[fduser] = cdc.[fduser]
	,[fduser_conv] = cdc.[fduser_conv]
	,[fdpid] = cdc.[fdpid]
	,[fdpid_conv] = cdc.[fdpid_conv]
	,[fdjobn] = cdc.[fdjobn]
	,[fdjobn_conv] = cdc.[fdjobn_conv]
	,[fdupmj] = cdc.[fdupmj]
	,[fdupmj_conv] = cdc.[fdupmj_conv]
	,[fdupmt] = cdc.[fdupmt]
	,[fdurrf] = cdc.[fdurrf]
	,[fdurrf_conv] = cdc.[fdurrf_conv]
	,[fdurcd] = cdc.[fdurcd]
	,[fdurcd_conv] = cdc.[fdurcd_conv]
	,[fdurab] = cdc.[fdurab]
	,[fdurab_conv] = cdc.[fdurab_conv]
	,[fdurat] = cdc.[fdurat]
	,[fdurat_conv] = cdc.[fdurat_conv]
	,[fdurdt] = cdc.[fdurdt]
	,[fdurdt_conv] = cdc.[fdurdt_conv]
	,[fdrcd] = cdc.[fdrcd]
	,[fdy55char1] = cdc.[fdy55char1]
	,[fdy55char1_conv] = cdc.[fdy55char1_conv]
	,[fdy55char2] = cdc.[fdy55char2]
	,[fdy55char2_conv] = cdc.[fdy55char2_conv]
	,[fdy55date1] = cdc.[fdy55date1]
	,[fdy55date1_conv] = cdc.[fdy55date1_conv]
	,[fdy55date2] = cdc.[fdy55date2]
	,[fdy55date2_conv] = cdc.[fdy55date2_conv]
	,[fdy55strg1] = cdc.[fdy55strg1]
	,[fdy55strg1_conv] = cdc.[fdy55strg1_conv]
	,[fdy55strg2] = cdc.[fdy55strg2]
	,[fdy55strg2_conv] = cdc.[fdy55strg2_conv]
	,[fdy55strg3] = cdc.[fdy55strg3]
	,[fdy55strg3_conv] = cdc.[fdy55strg3_conv]
	,[fdy55strg4] = cdc.[fdy55strg4]
	,[fdy55strg4_conv] = cdc.[fdy55strg4_conv]
	,[fdy55strg5] = cdc.[fdy55strg5]
	,[fdy55strg5_conv] = cdc.[fdy55strg5_conv]
	,[fdy55strg6] = cdc.[fdy55strg6]
	,[fdy55strg6_conv] = cdc.[fdy55strg6_conv]
	,[fdy55strg7] = cdc.[fdy55strg7]
	,[fdy55strg7_conv] = cdc.[fdy55strg7_conv]
	,[fdy55strg8] = cdc.[fdy55strg8]
	,[fdy55strg8_conv] = cdc.[fdy55strg8_conv]
	,[fdy55strg9] = cdc.[fdy55strg9]
	,[fdy55strg9_conv] = cdc.[fdy55strg9_conv]
	,[fdy55strg10] = cdc.[fdy55strg10]
	,[fdy55strg10_conv] = cdc.[fdy55strg10_conv]
	,[fdy55flg1] = cdc.[fdy55flg1]
	,[fdy55flg1_conv] = cdc.[fdy55flg1_conv]
	,[fdy55flg2] = cdc.[fdy55flg2]
	,[fdy55flg2_conv] = cdc.[fdy55flg2_conv]
	,[fdy55num3] = cdc.[fdy55num3]
	,[fdy55num3_conv] = cdc.[fdy55num3_conv]
	,[fdy55num4] = cdc.[fdy55num4]
	,[fdy55num4_conv] = cdc.[fdy55num4_conv]
	,[fdy55time1] = cdc.[fdy55time1]
	,[fdy55time2] = cdc.[fdy55time2]
	,[fdy55amnt2] = cdc.[fdy55amnt2]
	,[fdy55amnt2_conv] = cdc.[fdy55amnt2_conv]
	,[fdy55amnt1] = cdc.[fdy55amnt1]
	,[fdy55amnt1_conv] = cdc.[fdy55amnt1_conv]
	,[fdukid] = cdc.[fdukid]
	,[fdflg2] = cdc.[fdflg2]
	,[fdflg2_conv] = cdc.[fdflg2_conv]
	,[fdflg3] = cdc.[fdflg3]
	,[fdflg3_conv] = cdc.[fdflg3_conv]
	,[fdxpitraid] = cdc.[fdxpitraid]
	,[fdxpitraid_conv] = cdc.[fdxpitraid_conv]
	,[fdflg01] = cdc.[fdflg01]
	,[fdflg01_conv] = cdc.[fdflg01_conv]
	,[fdy55fwttm] = cdc.[fdy55fwttm]
	,[fdy55fwttm_conv] = cdc.[fdy55fwttm_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f554904_cdc cdc
WHERE
    rep_jde_qat.f554904_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f554904_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f554904_new AF:{{ task_instance_key_str }}' ) 