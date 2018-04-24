-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f554506_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[ismcu] = cdc.[ismcu]
	,[ismcu_conv] = cdc.[ismcu_conv]
	,[isitm] = cdc.[isitm]
	,[isy55qdte] = cdc.[isy55qdte]
	,[isy55qdte_conv] = cdc.[isy55qdte_conv]
	,[istme0] = cdc.[istme0]
	,[islitm] = cdc.[islitm]
	,[islitm_conv] = cdc.[islitm_conv]
	,[isaitm] = cdc.[isaitm]
	,[isaitm_conv] = cdc.[isaitm_conv]
	,[isy55qn] = cdc.[isy55qn]
	,[isy55qn_conv] = cdc.[isy55qn_conv]
	,[isy55jdqn] = cdc.[isy55jdqn]
	,[isy55jdqn_conv] = cdc.[isy55jdqn_conv]
	,[isy55qt] = cdc.[isy55qt]
	,[isuom1] = cdc.[isuom1]
	,[isuncs] = cdc.[isuncs]
	,[isuncs_conv] = cdc.[isuncs_conv]
	,[iscrcd] = cdc.[iscrcd]
	,[iscrcd_conv] = cdc.[iscrcd_conv]
	,[isy55usg] = cdc.[isy55usg]
	,[isy55usg_conv] = cdc.[isy55usg_conv]
	,[isy55crcd1] = cdc.[isy55crcd1]
	,[isy55crcd1_conv] = cdc.[isy55crcd1_conv]
	,[isy55uom1] = cdc.[isy55uom1]
	,[isy55lsg] = cdc.[isy55lsg]
	,[isy55lsg_conv] = cdc.[isy55lsg_conv]
	,[isy55crcd2] = cdc.[isy55crcd2]
	,[isy55crcd2_conv] = cdc.[isy55crcd2_conv]
	,[isy55uom2] = cdc.[isy55uom2]
	,[isy55acs] = cdc.[isy55acs]
	,[isy55acs_conv] = cdc.[isy55acs_conv]
	,[isy55crcd3] = cdc.[isy55crcd3]
	,[isy55crcd3_conv] = cdc.[isy55crcd3_conv]
	,[isy55uom3] = cdc.[isy55uom3]
	,[isy55trc] = cdc.[isy55trc]
	,[isy55trc_conv] = cdc.[isy55trc_conv]
	,[isy55crcd4] = cdc.[isy55crcd4]
	,[isy55crcd4_conv] = cdc.[isy55crcd4_conv]
	,[isy55uom4] = cdc.[isy55uom4]
	,[isy55esg] = cdc.[isy55esg]
	,[isy55esg_conv] = cdc.[isy55esg_conv]
	,[isy55crcd6] = cdc.[isy55crcd6]
	,[isy55crcd6_conv] = cdc.[isy55crcd6_conv]
	,[isy55uom6] = cdc.[isy55uom6]
	,[isy55disc] = cdc.[isy55disc]
	,[isy55disc_conv] = cdc.[isy55disc_conv]
	,[isy55osl] = cdc.[isy55osl]
	,[isy55osl_conv] = cdc.[isy55osl_conv]
	,[isy55tcst] = cdc.[isy55tcst]
	,[isy55tcst_conv] = cdc.[isy55tcst_conv]
	,[isy55crcd5] = cdc.[isy55crcd5]
	,[isy55crcd5_conv] = cdc.[isy55crcd5_conv]
	,[isy55uom5] = cdc.[isy55uom5]
	,[isy55avin] = cdc.[isy55avin]
	,[isdte] = cdc.[isdte]
	,[isdte_conv] = cdc.[isdte_conv]
	,[isflag] = cdc.[isflag]
	,[isflag_conv] = cdc.[isflag_conv]
	,[isev01] = cdc.[isev01]
	,[isev01_conv] = cdc.[isev01_conv]
	,[isev02] = cdc.[isev02]
	,[isev02_conv] = cdc.[isev02_conv]
	,[isev03] = cdc.[isev03]
	,[isev03_conv] = cdc.[isev03_conv]
	,[isev04] = cdc.[isev04]
	,[isev04_conv] = cdc.[isev04_conv]
	,[isev05] = cdc.[isev05]
	,[isev05_conv] = cdc.[isev05_conv]
	,[isurrf] = cdc.[isurrf]
	,[isurrf_conv] = cdc.[isurrf_conv]
	,[isurcd] = cdc.[isurcd]
	,[isurcd_conv] = cdc.[isurcd_conv]
	,[isurab] = cdc.[isurab]
	,[isurab_conv] = cdc.[isurab_conv]
	,[isurat] = cdc.[isurat]
	,[isurat_conv] = cdc.[isurat_conv]
	,[isurdt] = cdc.[isurdt]
	,[isurdt_conv] = cdc.[isurdt_conv]
	,[isuser] = cdc.[isuser]
	,[isuser_conv] = cdc.[isuser_conv]
	,[ispid] = cdc.[ispid]
	,[ispid_conv] = cdc.[ispid_conv]
	,[isjobn] = cdc.[isjobn]
	,[isjobn_conv] = cdc.[isjobn_conv]
	,[isupmt] = cdc.[isupmt]
	,[isupmj] = cdc.[isupmj]
	,[isupmj_conv] = cdc.[isupmj_conv]
	,[isy55char1] = cdc.[isy55char1]
	,[isy55char1_conv] = cdc.[isy55char1_conv]
	,[isy55char2] = cdc.[isy55char2]
	,[isy55char2_conv] = cdc.[isy55char2_conv]
	,[isy55date1] = cdc.[isy55date1]
	,[isy55date1_conv] = cdc.[isy55date1_conv]
	,[isy55date2] = cdc.[isy55date2]
	,[isy55date2_conv] = cdc.[isy55date2_conv]
	,[isy55strg1] = cdc.[isy55strg1]
	,[isy55strg1_conv] = cdc.[isy55strg1_conv]
	,[isy55strg2] = cdc.[isy55strg2]
	,[isy55strg2_conv] = cdc.[isy55strg2_conv]
	,[isy55time1] = cdc.[isy55time1]
	,[isy55time2] = cdc.[isy55time2]
	,[isy55amnt1] = cdc.[isy55amnt1]
	,[isy55amnt1_conv] = cdc.[isy55amnt1_conv]
	,[isy55amnt2] = cdc.[isy55amnt2]
	,[isy55amnt2_conv] = cdc.[isy55amnt2_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f554506_cdc cdc
WHERE
    rep_jde.f554506_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f554506_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f554506_new AF:{{ task_instance_key_str }}' ) 
