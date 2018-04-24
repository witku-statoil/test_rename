-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f554237_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[rsaddj] = cdc.[rsaddj]
	,[rsaddj_conv] = cdc.[rsaddj_conv]
	,[rsvr01] = cdc.[rsvr01]
	,[rsvr01_conv] = cdc.[rsvr01_conv]
	,[rsmcu] = cdc.[rsmcu]
	,[rsmcu_conv] = cdc.[rsmcu_conv]
	,[rstpc] = cdc.[rstpc]
	,[rstpc_conv] = cdc.[rstpc_conv]
	,[rstax1] = cdc.[rstax1]
	,[rsback] = cdc.[rsback]
	,[rsback_conv] = cdc.[rsback_conv]
	,[rsurrf] = cdc.[rsurrf]
	,[rsurrf_conv] = cdc.[rsurrf_conv]
	,[rssbal] = cdc.[rssbal]
	,[rssbal_conv] = cdc.[rssbal_conv]
	,[rsaa18] = cdc.[rsaa18]
	,[rsaa18_conv] = cdc.[rsaa18_conv]
	,[rsvr02] = cdc.[rsvr02]
	,[rsvr02_conv] = cdc.[rsvr02_conv]
	,[rsacttime] = cdc.[rsacttime]
	,[rsacttime_conv] = cdc.[rsacttime_conv]
	,[rsobsz] = cdc.[rsobsz]
	,[rsaa30] = cdc.[rsaa30]
	,[rsaa30_conv] = cdc.[rsaa30_conv]
	,[rsctr] = cdc.[rsctr]
	,[rsaa20] = cdc.[rsaa20]
	,[rsaa20_conv] = cdc.[rsaa20_conv]
	,[rssbtm] = cdc.[rssbtm]
	,[rsev01] = cdc.[rsev01]
	,[rsev01_conv] = cdc.[rsev01_conv]
	,[rsev02] = cdc.[rsev02]
	,[rsev02_conv] = cdc.[rsev02_conv]
	,[rsev03] = cdc.[rsev03]
	,[rsev03_conv] = cdc.[rsev03_conv]
	,[rsev04] = cdc.[rsev04]
	,[rsev04_conv] = cdc.[rsev04_conv]
	,[rsev05] = cdc.[rsev05]
	,[rsev05_conv] = cdc.[rsev05_conv]
	,[rsev06] = cdc.[rsev06]
	,[rsev06_conv] = cdc.[rsev06_conv]
	,[rsev07] = cdc.[rsev07]
	,[rsev07_conv] = cdc.[rsev07_conv]
	,[rsev08] = cdc.[rsev08]
	,[rsev08_conv] = cdc.[rsev08_conv]
	,[rsev09] = cdc.[rsev09]
	,[rsev09_conv] = cdc.[rsev09_conv]
	,[rsaurst2] = cdc.[rsaurst2]
	,[rsaurst2_conv] = cdc.[rsaurst2_conv]
	,[rsaurst3] = cdc.[rsaurst3]
	,[rsaurst3_conv] = cdc.[rsaurst3_conv]
	,[rsy55char1] = cdc.[rsy55char1]
	,[rsy55char1_conv] = cdc.[rsy55char1_conv]
	,[rsy55char2] = cdc.[rsy55char2]
	,[rsy55char2_conv] = cdc.[rsy55char2_conv]
	,[rsy55strg1] = cdc.[rsy55strg1]
	,[rsy55strg1_conv] = cdc.[rsy55strg1_conv]
	,[rsy55strg2] = cdc.[rsy55strg2]
	,[rsy55strg2_conv] = cdc.[rsy55strg2_conv]
	,[rsy55amnt1] = cdc.[rsy55amnt1]
	,[rsy55amnt1_conv] = cdc.[rsy55amnt1_conv]
	,[rsy55amnt2] = cdc.[rsy55amnt2]
	,[rsy55amnt2_conv] = cdc.[rsy55amnt2_conv]
	,[rsy55time1] = cdc.[rsy55time1]
	,[rsy55time2] = cdc.[rsy55time2]
	,[rsy55date1] = cdc.[rsy55date1]
	,[rsy55date1_conv] = cdc.[rsy55date1_conv]
	,[rsy55date2] = cdc.[rsy55date2]
	,[rsy55date2_conv] = cdc.[rsy55date2_conv]
	,[rsuser] = cdc.[rsuser]
	,[rsuser_conv] = cdc.[rsuser_conv]
	,[rspid] = cdc.[rspid]
	,[rspid_conv] = cdc.[rspid_conv]
	,[rsjobn] = cdc.[rsjobn]
	,[rsjobn_conv] = cdc.[rsjobn_conv]
	,[rsupmj] = cdc.[rsupmj]
	,[rsupmj_conv] = cdc.[rsupmj_conv]
	,[rsupmt] = cdc.[rsupmt] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f554237_cdc cdc
WHERE
    rep_jde_qat.f554237_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f554237_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f554237_new AF:{{ task_instance_key_str }}' ) 