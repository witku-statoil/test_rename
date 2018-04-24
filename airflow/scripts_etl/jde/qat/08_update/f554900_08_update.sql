-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f554900_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[dhvr01] = cdc.[dhvr01]
	,[dhvr01_conv] = cdc.[dhvr01_conv]
	,[dhy55msgno] = cdc.[dhy55msgno]
	,[dhy55msgno_conv] = cdc.[dhy55msgno_conv]
	,[dhy55insdj] = cdc.[dhy55insdj]
	,[dhy55insdj_conv] = cdc.[dhy55insdj_conv]
	,[dhy55insdt] = cdc.[dhy55insdt]
	,[dhnstp] = cdc.[dhnstp]
	,[dhnstp_conv] = cdc.[dhnstp_conv]
	,[dhpveh] = cdc.[dhpveh]
	,[dhpveh_conv] = cdc.[dhpveh_conv]
	,[dhnbrord] = cdc.[dhnbrord]
	,[dhnbrord_conv] = cdc.[dhnbrord_conv]
	,[dhy55dstt] = cdc.[dhy55dstt]
	,[dhy55dsnt] = cdc.[dhy55dsnt]
	,[dhumd1] = cdc.[dhumd1]
	,[dhy55eltm] = cdc.[dhy55eltm]
	,[dhy55odrtm] = cdc.[dhy55odrtm]
	,[dhy55odltm] = cdc.[dhy55odltm]
	,[dhy55oldtm] = cdc.[dhy55oldtm]
	,[dhy55tsdj] = cdc.[dhy55tsdj]
	,[dhy55tsdj_conv] = cdc.[dhy55tsdj_conv]
	,[dhy55tsdm] = cdc.[dhy55tsdm]
	,[dhdlda] = cdc.[dhdlda]
	,[dhdlda_conv] = cdc.[dhdlda_conv]
	,[dhtdlf] = cdc.[dhtdlf]
	,[dhy55tdlf] = cdc.[dhy55tdlf]
	,[dhdldl] = cdc.[dhdldl]
	,[dhdldl_conv] = cdc.[dhdldl_conv]
	,[dhtdlt] = cdc.[dhtdlt]
	,[dhy55tdlt] = cdc.[dhy55tdlt]
	,[dhy55tedj] = cdc.[dhy55tedj]
	,[dhy55tedj_conv] = cdc.[dhy55tedj_conv]
	,[dhy55tetm] = cdc.[dhy55tetm]
	,[dhldnm] = cdc.[dhldnm]
	,[dhldls] = cdc.[dhldls]
	,[dhy55ercod] = cdc.[dhy55ercod]
	,[dhy55ercod_conv] = cdc.[dhy55ercod_conv]
	,[dhy55eflg] = cdc.[dhy55eflg]
	,[dhy55eflg_conv] = cdc.[dhy55eflg_conv]
	,[dhy55tdlq] = cdc.[dhy55tdlq]
	,[dhy55tdlq_conv] = cdc.[dhy55tdlq_conv]
	,[dhuom] = cdc.[dhuom]
	,[dhy55ldno] = cdc.[dhy55ldno]
	,[dhy55ldno_conv] = cdc.[dhy55ldno_conv]
	,[dhy55mnor] = cdc.[dhy55mnor]
	,[dhy55mnstp] = cdc.[dhy55mnstp]
	,[dhy55mnstp_conv] = cdc.[dhy55mnstp_conv]
	,[dhy55pdno] = cdc.[dhy55pdno]
	,[dhy55zdno] = cdc.[dhy55zdno]
	,[dhy55rdno] = cdc.[dhy55rdno]
	,[dhy55ntat] = cdc.[dhy55ntat]
	,[dhy55ntat_conv] = cdc.[dhy55ntat_conv]
	,[dhy55wttm] = cdc.[dhy55wttm]
	,[dhy55odstm] = cdc.[dhy55odstm]
	,[dhy55oumds] = cdc.[dhy55oumds]
	,[dhy55disn1] = cdc.[dhy55disn1]
	,[dhy55stmcu] = cdc.[dhy55stmcu]
	,[dhy55etmcu] = cdc.[dhy55etmcu]
	,[dhy55bflg] = cdc.[dhy55bflg]
	,[dhy55drtm] = cdc.[dhy55drtm]
	,[dhy55mctm] = cdc.[dhy55mctm]
	,[dhstfn] = cdc.[dhstfn]
	,[dhy55wktm] = cdc.[dhy55wktm]
	,[dhwactime] = cdc.[dhwactime]
	,[dhwactime_conv] = cdc.[dhwactime_conv]
	,[dhappflg] = cdc.[dhappflg]
	,[dhappflg_conv] = cdc.[dhappflg_conv]
	,[dhbhuser] = cdc.[dhbhuser]
	,[dhbhuser_conv] = cdc.[dhbhuser_conv]
	,[dhapdj] = cdc.[dhapdj]
	,[dhapdj_conv] = cdc.[dhapdj_conv]
	,[dhatim] = cdc.[dhatim]
	,[dhy55ackm] = cdc.[dhy55ackm]
	,[dhy55gpskm] = cdc.[dhy55gpskm]
	,[dhrlno] = cdc.[dhrlno]
	,[dhrlno_conv] = cdc.[dhrlno_conv]
	,[dhtlnum] = cdc.[dhtlnum]
	,[dhtlnum_conv] = cdc.[dhtlnum_conv]
	,[dhy55ldtm] = cdc.[dhy55ldtm]
	,[dhy55drcd] = cdc.[dhy55drcd]
	,[dhy55drcd_conv] = cdc.[dhy55drcd_conv]
	,[dhy55rbflg] = cdc.[dhy55rbflg]
	,[dhy55rbflg_conv] = cdc.[dhy55rbflg_conv]
	,[dhy55glfg] = cdc.[dhy55glfg]
	,[dhy55glfg_conv] = cdc.[dhy55glfg_conv]
	,[dhy55glap] = cdc.[dhy55glap]
	,[dhy55glap_conv] = cdc.[dhy55glap_conv]
	,[dhy55srce] = cdc.[dhy55srce]
	,[dhedbt] = cdc.[dhedbt]
	,[dhedbt_conv] = cdc.[dhedbt_conv]
	,[dhedtn] = cdc.[dhedtn]
	,[dhedtn_conv] = cdc.[dhedtn_conv]
	,[dhy55spdl] = cdc.[dhy55spdl]
	,[dhy55spdl_conv] = cdc.[dhy55spdl_conv]
	,[dhcars] = cdc.[dhcars]
	,[dhcars_conv] = cdc.[dhcars_conv]
	,[dhflag] = cdc.[dhflag]
	,[dhflag_conv] = cdc.[dhflag_conv]
	,[dhuser] = cdc.[dhuser]
	,[dhuser_conv] = cdc.[dhuser_conv]
	,[dhpid] = cdc.[dhpid]
	,[dhpid_conv] = cdc.[dhpid_conv]
	,[dhjobn] = cdc.[dhjobn]
	,[dhjobn_conv] = cdc.[dhjobn_conv]
	,[dhupmj] = cdc.[dhupmj]
	,[dhupmj_conv] = cdc.[dhupmj_conv]
	,[dhupmt] = cdc.[dhupmt]
	,[dhurcd] = cdc.[dhurcd]
	,[dhurcd_conv] = cdc.[dhurcd_conv]
	,[dhurdt] = cdc.[dhurdt]
	,[dhurdt_conv] = cdc.[dhurdt_conv]
	,[dhurat] = cdc.[dhurat]
	,[dhurat_conv] = cdc.[dhurat_conv]
	,[dhurab] = cdc.[dhurab]
	,[dhurab_conv] = cdc.[dhurab_conv]
	,[dhurrf] = cdc.[dhurrf]
	,[dhurrf_conv] = cdc.[dhurrf_conv]
	,[dhrcd] = cdc.[dhrcd]
	,[dhy55char1] = cdc.[dhy55char1]
	,[dhy55char1_conv] = cdc.[dhy55char1_conv]
	,[dhy55char2] = cdc.[dhy55char2]
	,[dhy55char2_conv] = cdc.[dhy55char2_conv]
	,[dhy55date1] = cdc.[dhy55date1]
	,[dhy55date1_conv] = cdc.[dhy55date1_conv]
	,[dhy55date2] = cdc.[dhy55date2]
	,[dhy55date2_conv] = cdc.[dhy55date2_conv]
	,[dhy55strg1] = cdc.[dhy55strg1]
	,[dhy55strg1_conv] = cdc.[dhy55strg1_conv]
	,[dhy55strg2] = cdc.[dhy55strg2]
	,[dhy55strg2_conv] = cdc.[dhy55strg2_conv]
	,[dhy55strg3] = cdc.[dhy55strg3]
	,[dhy55strg3_conv] = cdc.[dhy55strg3_conv]
	,[dhy55strg4] = cdc.[dhy55strg4]
	,[dhy55strg4_conv] = cdc.[dhy55strg4_conv]
	,[dhy55strg5] = cdc.[dhy55strg5]
	,[dhy55strg5_conv] = cdc.[dhy55strg5_conv]
	,[dhy55strg6] = cdc.[dhy55strg6]
	,[dhy55strg6_conv] = cdc.[dhy55strg6_conv]
	,[dhy55strg7] = cdc.[dhy55strg7]
	,[dhy55strg7_conv] = cdc.[dhy55strg7_conv]
	,[dhy55strg8] = cdc.[dhy55strg8]
	,[dhy55strg8_conv] = cdc.[dhy55strg8_conv]
	,[dhy55strg9] = cdc.[dhy55strg9]
	,[dhy55strg9_conv] = cdc.[dhy55strg9_conv]
	,[dhy55strg10] = cdc.[dhy55strg10]
	,[dhy55strg10_conv] = cdc.[dhy55strg10_conv]
	,[dhy55flg1] = cdc.[dhy55flg1]
	,[dhy55flg1_conv] = cdc.[dhy55flg1_conv]
	,[dhy55flg2] = cdc.[dhy55flg2]
	,[dhy55flg2_conv] = cdc.[dhy55flg2_conv]
	,[dhy55num3] = cdc.[dhy55num3]
	,[dhy55num3_conv] = cdc.[dhy55num3_conv]
	,[dhy55num4] = cdc.[dhy55num4]
	,[dhy55num4_conv] = cdc.[dhy55num4_conv]
	,[dhy55time1] = cdc.[dhy55time1]
	,[dhy55time2] = cdc.[dhy55time2]
	,[dhy55amnt1] = cdc.[dhy55amnt1]
	,[dhy55amnt1_conv] = cdc.[dhy55amnt1_conv]
	,[dhy55amnt2] = cdc.[dhy55amnt2]
	,[dhy55amnt2_conv] = cdc.[dhy55amnt2_conv]
	,[dhum] = cdc.[dhum]
	,[dhhrsact] = cdc.[dhhrsact]
	,[dhhrsact_conv] = cdc.[dhhrsact_conv]
	,[dhflg2] = cdc.[dhflg2]
	,[dhflg2_conv] = cdc.[dhflg2_conv]
	,[dhflg3] = cdc.[dhflg3]
	,[dhflg3_conv] = cdc.[dhflg3_conv]
	,[dhflg4] = cdc.[dhflg4]
	,[dhflg4_conv] = cdc.[dhflg4_conv]
	,[dhvmcu] = cdc.[dhvmcu]
	,[dhvmcu_conv] = cdc.[dhvmcu_conv]
	,[dhy55disn] = cdc.[dhy55disn]
	,[dhefr] = cdc.[dhefr]
	,[dhefr_conv] = cdc.[dhefr_conv]
	,[dhhrwt] = cdc.[dhhrwt]
	,[dhhrwt_conv] = cdc.[dhhrwt_conv]
	,[dhaltd] = cdc.[dhaltd]
	,[dhaltd_conv] = cdc.[dhaltd_conv]
	,[dhdrtm] = cdc.[dhdrtm]
	,[dhdrtm_conv] = cdc.[dhdrtm_conv]
	,[dhflg1] = cdc.[dhflg1]
	,[dhflg1_conv] = cdc.[dhflg1_conv]
	,[dhapsfact] = cdc.[dhapsfact]
	,[dhapsfact_conv] = cdc.[dhapsfact_conv]
	,[dhprqu] = cdc.[dhprqu]
	,[dhprqu_conv] = cdc.[dhprqu_conv]
	,[dhuom1] = cdc.[dhuom1]
	,[dhmc1] = cdc.[dhmc1]
	,[dhmc1_conv] = cdc.[dhmc1_conv]
	,[dhtcap] = cdc.[dhtcap]
	,[dhtcap_conv] = cdc.[dhtcap_conv]
	,[dhot1] = cdc.[dhot1]
	,[dhot1_conv] = cdc.[dhot1_conv]
	,[dhcrcp] = cdc.[dhcrcp]
	,[dhcrcp_conv] = cdc.[dhcrcp_conv]
	,[dhpraa] = cdc.[dhpraa]
	,[dhpraa_conv] = cdc.[dhpraa_conv]
	,[dhaoth] = cdc.[dhaoth]
	,[dhaoth_conv] = cdc.[dhaoth_conv]
	,[dhchgamt] = cdc.[dhchgamt]
	,[dhchgamt_conv] = cdc.[dhchgamt_conv]
	,[dhnbr3] = cdc.[dhnbr3]
	,[dhnbr2] = cdc.[dhnbr2]
	,[dhnbr1] = cdc.[dhnbr1]
	,[dhnocm] = cdc.[dhnocm]
	,[dhnocm_conv] = cdc.[dhnocm_conv]
	,[dhnbrsld] = cdc.[dhnbrsld]
	,[dhnbrsld_conv] = cdc.[dhnbrsld_conv]
	,[dhtold] = cdc.[dhtold]
	,[dhtold_conv] = cdc.[dhtold_conv]
	,[dhodst] = cdc.[dhodst]
	,[dhsclq] = cdc.[dhsclq]
	,[dhsclq_conv] = cdc.[dhsclq_conv]
	,[dhy55trtm] = cdc.[dhy55trtm]
	,[dhorgn] = cdc.[dhorgn]
	,[dhtrdt] = cdc.[dhtrdt]
	,[dhtrdt_conv] = cdc.[dhtrdt_conv]
	,[dhsttme] = cdc.[dhsttme]
	,[dhstdate] = cdc.[dhstdate]
	,[dhstdate_conv] = cdc.[dhstdate_conv]
	,[dhdisn] = cdc.[dhdisn]
	,[dhdtai] = cdc.[dhdtai]
	,[dhdtai_conv] = cdc.[dhdtai_conv]
	,[dhy55elts2] = cdc.[dhy55elts2]
	,[dhy55elts1] = cdc.[dhy55elts1]
	,[dhy55elts] = cdc.[dhy55elts]
	,[dhtme0] = cdc.[dhtme0]
	,[dhacttime] = cdc.[dhacttime]
	,[dhacttime_conv] = cdc.[dhacttime_conv]
	,[dhbstn] = cdc.[dhbstn]
	,[dhbstn_conv] = cdc.[dhbstn_conv]
	,[dhancc] = cdc.[dhancc]
	,[dhdte] = cdc.[dhdte]
	,[dhdte_conv] = cdc.[dhdte_conv]
	,[dhukid] = cdc.[dhukid]
	,[dhxpitraid] = cdc.[dhxpitraid]
	,[dhxpitraid_conv] = cdc.[dhxpitraid_conv]
	,[dhy55vg] = cdc.[dhy55vg]
	,[dhvtyp] = cdc.[dhvtyp]
	,[dhvtyp_conv] = cdc.[dhvtyp_conv]
	,[dhy55rm1] = cdc.[dhy55rm1]
	,[dhy55rm1_conv] = cdc.[dhy55rm1_conv]
	,[dhy55rm2] = cdc.[dhy55rm2]
	,[dhy55rm2_conv] = cdc.[dhy55rm2_conv]
	,[dhy55stter] = cdc.[dhy55stter]
	,[dhy55stter_conv] = cdc.[dhy55stter_conv]
	,[dhy55edter] = cdc.[dhy55edter]
	,[dhy55edter_conv] = cdc.[dhy55edter_conv]
	,[dhy55wrcd] = cdc.[dhy55wrcd]
	,[dhy55sapflg] = cdc.[dhy55sapflg]
	,[dhy55sapflg_conv] = cdc.[dhy55sapflg_conv]
	,[dhy55mnobsp] = cdc.[dhy55mnobsp]
	,[dhy55mcth] = cdc.[dhy55mcth]
	,[dhy55mcth_conv] = cdc.[dhy55mcth_conv]
	,[dhy55wttmh] = cdc.[dhy55wttmh]
	,[dhy55wttmh_conv] = cdc.[dhy55wttmh_conv]
	,[dhy55wktmh] = cdc.[dhy55wktmh]
	,[dhy55wktmh_conv] = cdc.[dhy55wktmh_conv]
	,[dhy55nstp] = cdc.[dhy55nstp]
	,[dhy55nstp_conv] = cdc.[dhy55nstp_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f554900_cdc cdc
WHERE
    rep_jde_qat.f554900_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f554900_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f554900_new AF:{{ task_instance_key_str }}' ) 
