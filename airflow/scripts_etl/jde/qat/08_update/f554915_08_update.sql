-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f554915_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[vgy55vg] = cdc.[vgy55vg]
	,[vgvehi] = cdc.[vgvehi]
	,[vgvehi_conv] = cdc.[vgvehi_conv]
	,[vgdsc1] = cdc.[vgdsc1]
	,[vgdsc1_conv] = cdc.[vgdsc1_conv]
	,[vgdsc2] = cdc.[vgdsc2]
	,[vgdsc2_conv] = cdc.[vgdsc2_conv]
	,[vg98iddesc] = cdc.[vg98iddesc]
	,[vg98iddesc_conv] = cdc.[vg98iddesc_conv]
	,[vgdl011] = cdc.[vgdl011]
	,[vgdl011_conv] = cdc.[vgdl011_conv]
	,[vgy55char1] = cdc.[vgy55char1]
	,[vgy55char1_conv] = cdc.[vgy55char1_conv]
	,[vgy55char2] = cdc.[vgy55char2]
	,[vgy55char2_conv] = cdc.[vgy55char2_conv]
	,[vgy55strg1] = cdc.[vgy55strg1]
	,[vgy55strg1_conv] = cdc.[vgy55strg1_conv]
	,[vgy55strg2] = cdc.[vgy55strg2]
	,[vgy55strg2_conv] = cdc.[vgy55strg2_conv]
	,[vgy55strg3] = cdc.[vgy55strg3]
	,[vgy55strg3_conv] = cdc.[vgy55strg3_conv]
	,[vgy55strg4] = cdc.[vgy55strg4]
	,[vgy55strg4_conv] = cdc.[vgy55strg4_conv]
	,[vgy55strg5] = cdc.[vgy55strg5]
	,[vgy55strg5_conv] = cdc.[vgy55strg5_conv]
	,[vgy55strg6] = cdc.[vgy55strg6]
	,[vgy55strg6_conv] = cdc.[vgy55strg6_conv]
	,[vgy55strg7] = cdc.[vgy55strg7]
	,[vgy55strg7_conv] = cdc.[vgy55strg7_conv]
	,[vgy55strg8] = cdc.[vgy55strg8]
	,[vgy55strg8_conv] = cdc.[vgy55strg8_conv]
	,[vgy55strg9] = cdc.[vgy55strg9]
	,[vgy55strg9_conv] = cdc.[vgy55strg9_conv]
	,[vgy55strg10] = cdc.[vgy55strg10]
	,[vgy55strg10_conv] = cdc.[vgy55strg10_conv]
	,[vgy55num1] = cdc.[vgy55num1]
	,[vgy55num1_conv] = cdc.[vgy55num1_conv]
	,[vgy55num2] = cdc.[vgy55num2]
	,[vgy55num2_conv] = cdc.[vgy55num2_conv]
	,[vgy55num3] = cdc.[vgy55num3]
	,[vgy55num3_conv] = cdc.[vgy55num3_conv]
	,[vgy55num4] = cdc.[vgy55num4]
	,[vgy55num4_conv] = cdc.[vgy55num4_conv]
	,[vgy55date1] = cdc.[vgy55date1]
	,[vgy55date1_conv] = cdc.[vgy55date1_conv]
	,[vgy55date2] = cdc.[vgy55date2]
	,[vgy55date2_conv] = cdc.[vgy55date2_conv]
	,[vgy55amnt1] = cdc.[vgy55amnt1]
	,[vgy55amnt1_conv] = cdc.[vgy55amnt1_conv]
	,[vgy55amnt2] = cdc.[vgy55amnt2]
	,[vgy55amnt2_conv] = cdc.[vgy55amnt2_conv]
	,[vgurrf] = cdc.[vgurrf]
	,[vgurrf_conv] = cdc.[vgurrf_conv]
	,[vgurcd] = cdc.[vgurcd]
	,[vgurcd_conv] = cdc.[vgurcd_conv]
	,[vgurat] = cdc.[vgurat]
	,[vgurat_conv] = cdc.[vgurat_conv]
	,[vgurab] = cdc.[vgurab]
	,[vgurab_conv] = cdc.[vgurab_conv]
	,[vguser] = cdc.[vguser]
	,[vguser_conv] = cdc.[vguser_conv]
	,[vgpid] = cdc.[vgpid]
	,[vgpid_conv] = cdc.[vgpid_conv]
	,[vgjobn] = cdc.[vgjobn]
	,[vgjobn_conv] = cdc.[vgjobn_conv]
	,[vgupmj] = cdc.[vgupmj]
	,[vgupmj_conv] = cdc.[vgupmj_conv]
	,[vgupmt] = cdc.[vgupmt] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f554915_cdc cdc
WHERE
    rep_jde_qat.f554915_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f554915_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f554915_new AF:{{ task_instance_key_str }}' ) 
