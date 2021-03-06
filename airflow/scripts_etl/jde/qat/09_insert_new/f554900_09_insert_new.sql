-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f554900_new
(
    	sys_file_name
	,sys_file_ln
	,sys_integration_id
	,sys_extract_dt
	,sys_cdc_dt
	,sys_cdc_scn
	,sys_cdc_operation_type
	,sys_cdc_before_after
	,sys_line_modified_ind
	,dhvr01
	,dhvr01_conv
	,dhy55msgno
	,dhy55msgno_conv
	,dhy55insdj
	,dhy55insdj_conv
	,dhy55insdt
	,dhnstp
	,dhnstp_conv
	,dhpveh
	,dhpveh_conv
	,dhnbrord
	,dhnbrord_conv
	,dhy55dstt
	,dhy55dsnt
	,dhumd1
	,dhy55eltm
	,dhy55odrtm
	,dhy55odltm
	,dhy55oldtm
	,dhy55tsdj
	,dhy55tsdj_conv
	,dhy55tsdm
	,dhdlda
	,dhdlda_conv
	,dhtdlf
	,dhy55tdlf
	,dhdldl
	,dhdldl_conv
	,dhtdlt
	,dhy55tdlt
	,dhy55tedj
	,dhy55tedj_conv
	,dhy55tetm
	,dhldnm
	,dhldls
	,dhy55ercod
	,dhy55ercod_conv
	,dhy55eflg
	,dhy55eflg_conv
	,dhy55tdlq
	,dhy55tdlq_conv
	,dhuom
	,dhy55ldno
	,dhy55ldno_conv
	,dhy55mnor
	,dhy55mnstp
	,dhy55mnstp_conv
	,dhy55pdno
	,dhy55zdno
	,dhy55rdno
	,dhy55ntat
	,dhy55ntat_conv
	,dhy55wttm
	,dhy55odstm
	,dhy55oumds
	,dhy55disn1
	,dhy55stmcu
	,dhy55etmcu
	,dhy55bflg
	,dhy55drtm
	,dhy55mctm
	,dhstfn
	,dhy55wktm
	,dhwactime
	,dhwactime_conv
	,dhappflg
	,dhappflg_conv
	,dhbhuser
	,dhbhuser_conv
	,dhapdj
	,dhapdj_conv
	,dhatim
	,dhy55ackm
	,dhy55gpskm
	,dhrlno
	,dhrlno_conv
	,dhtlnum
	,dhtlnum_conv
	,dhy55ldtm
	,dhy55drcd
	,dhy55drcd_conv
	,dhy55rbflg
	,dhy55rbflg_conv
	,dhy55glfg
	,dhy55glfg_conv
	,dhy55glap
	,dhy55glap_conv
	,dhy55srce
	,dhedbt
	,dhedbt_conv
	,dhedtn
	,dhedtn_conv
	,dhy55spdl
	,dhy55spdl_conv
	,dhcars
	,dhcars_conv
	,dhflag
	,dhflag_conv
	,dhuser
	,dhuser_conv
	,dhpid
	,dhpid_conv
	,dhjobn
	,dhjobn_conv
	,dhupmj
	,dhupmj_conv
	,dhupmt
	,dhurcd
	,dhurcd_conv
	,dhurdt
	,dhurdt_conv
	,dhurat
	,dhurat_conv
	,dhurab
	,dhurab_conv
	,dhurrf
	,dhurrf_conv
	,dhrcd
	,dhy55char1
	,dhy55char1_conv
	,dhy55char2
	,dhy55char2_conv
	,dhy55date1
	,dhy55date1_conv
	,dhy55date2
	,dhy55date2_conv
	,dhy55strg1
	,dhy55strg1_conv
	,dhy55strg2
	,dhy55strg2_conv
	,dhy55strg3
	,dhy55strg3_conv
	,dhy55strg4
	,dhy55strg4_conv
	,dhy55strg5
	,dhy55strg5_conv
	,dhy55strg6
	,dhy55strg6_conv
	,dhy55strg7
	,dhy55strg7_conv
	,dhy55strg8
	,dhy55strg8_conv
	,dhy55strg9
	,dhy55strg9_conv
	,dhy55strg10
	,dhy55strg10_conv
	,dhy55flg1
	,dhy55flg1_conv
	,dhy55flg2
	,dhy55flg2_conv
	,dhy55num3
	,dhy55num3_conv
	,dhy55num4
	,dhy55num4_conv
	,dhy55time1
	,dhy55time2
	,dhy55amnt1
	,dhy55amnt1_conv
	,dhy55amnt2
	,dhy55amnt2_conv
	,dhum
	,dhhrsact
	,dhhrsact_conv
	,dhflg2
	,dhflg2_conv
	,dhflg3
	,dhflg3_conv
	,dhflg4
	,dhflg4_conv
	,dhvmcu
	,dhvmcu_conv
	,dhy55disn
	,dhefr
	,dhefr_conv
	,dhhrwt
	,dhhrwt_conv
	,dhaltd
	,dhaltd_conv
	,dhdrtm
	,dhdrtm_conv
	,dhflg1
	,dhflg1_conv
	,dhapsfact
	,dhapsfact_conv
	,dhprqu
	,dhprqu_conv
	,dhuom1
	,dhmc1
	,dhmc1_conv
	,dhtcap
	,dhtcap_conv
	,dhot1
	,dhot1_conv
	,dhcrcp
	,dhcrcp_conv
	,dhpraa
	,dhpraa_conv
	,dhaoth
	,dhaoth_conv
	,dhchgamt
	,dhchgamt_conv
	,dhnbr3
	,dhnbr2
	,dhnbr1
	,dhnocm
	,dhnocm_conv
	,dhnbrsld
	,dhnbrsld_conv
	,dhtold
	,dhtold_conv
	,dhodst
	,dhsclq
	,dhsclq_conv
	,dhy55trtm
	,dhorgn
	,dhtrdt
	,dhtrdt_conv
	,dhsttme
	,dhstdate
	,dhstdate_conv
	,dhdisn
	,dhdtai
	,dhdtai_conv
	,dhy55elts2
	,dhy55elts1
	,dhy55elts
	,dhtme0
	,dhacttime
	,dhacttime_conv
	,dhbstn
	,dhbstn_conv
	,dhancc
	,dhdte
	,dhdte_conv
	,dhukid
	,dhxpitraid
	,dhxpitraid_conv
	,dhy55vg
	,dhvtyp
	,dhvtyp_conv
	,dhy55rm1
	,dhy55rm1_conv
	,dhy55rm2
	,dhy55rm2_conv
	,dhy55stter
	,dhy55stter_conv
	,dhy55edter
	,dhy55edter_conv
	,dhy55wrcd
	,dhy55sapflg
	,dhy55sapflg_conv
	,dhy55mnobsp
	,dhy55mcth
	,dhy55mcth_conv
	,dhy55wttmh
	,dhy55wttmh_conv
	,dhy55wktmh
	,dhy55wktmh_conv
	,dhy55nstp
	,dhy55nstp_conv
)
SELECT
    	cdc.sys_file_name
	,cdc.sys_file_ln
	,cdc.sys_integration_id
	,cdc.sys_extract_dt
	,cdc.sys_cdc_dt
	,cdc.sys_cdc_scn
	,cdc.sys_cdc_operation_type
	,cdc.sys_cdc_before_after
	,cdc.sys_line_modified_ind
	,cdc.dhvr01
	,cdc.dhvr01_conv
	,cdc.dhy55msgno
	,cdc.dhy55msgno_conv
	,cdc.dhy55insdj
	,cdc.dhy55insdj_conv
	,cdc.dhy55insdt
	,cdc.dhnstp
	,cdc.dhnstp_conv
	,cdc.dhpveh
	,cdc.dhpveh_conv
	,cdc.dhnbrord
	,cdc.dhnbrord_conv
	,cdc.dhy55dstt
	,cdc.dhy55dsnt
	,cdc.dhumd1
	,cdc.dhy55eltm
	,cdc.dhy55odrtm
	,cdc.dhy55odltm
	,cdc.dhy55oldtm
	,cdc.dhy55tsdj
	,cdc.dhy55tsdj_conv
	,cdc.dhy55tsdm
	,cdc.dhdlda
	,cdc.dhdlda_conv
	,cdc.dhtdlf
	,cdc.dhy55tdlf
	,cdc.dhdldl
	,cdc.dhdldl_conv
	,cdc.dhtdlt
	,cdc.dhy55tdlt
	,cdc.dhy55tedj
	,cdc.dhy55tedj_conv
	,cdc.dhy55tetm
	,cdc.dhldnm
	,cdc.dhldls
	,cdc.dhy55ercod
	,cdc.dhy55ercod_conv
	,cdc.dhy55eflg
	,cdc.dhy55eflg_conv
	,cdc.dhy55tdlq
	,cdc.dhy55tdlq_conv
	,cdc.dhuom
	,cdc.dhy55ldno
	,cdc.dhy55ldno_conv
	,cdc.dhy55mnor
	,cdc.dhy55mnstp
	,cdc.dhy55mnstp_conv
	,cdc.dhy55pdno
	,cdc.dhy55zdno
	,cdc.dhy55rdno
	,cdc.dhy55ntat
	,cdc.dhy55ntat_conv
	,cdc.dhy55wttm
	,cdc.dhy55odstm
	,cdc.dhy55oumds
	,cdc.dhy55disn1
	,cdc.dhy55stmcu
	,cdc.dhy55etmcu
	,cdc.dhy55bflg
	,cdc.dhy55drtm
	,cdc.dhy55mctm
	,cdc.dhstfn
	,cdc.dhy55wktm
	,cdc.dhwactime
	,cdc.dhwactime_conv
	,cdc.dhappflg
	,cdc.dhappflg_conv
	,cdc.dhbhuser
	,cdc.dhbhuser_conv
	,cdc.dhapdj
	,cdc.dhapdj_conv
	,cdc.dhatim
	,cdc.dhy55ackm
	,cdc.dhy55gpskm
	,cdc.dhrlno
	,cdc.dhrlno_conv
	,cdc.dhtlnum
	,cdc.dhtlnum_conv
	,cdc.dhy55ldtm
	,cdc.dhy55drcd
	,cdc.dhy55drcd_conv
	,cdc.dhy55rbflg
	,cdc.dhy55rbflg_conv
	,cdc.dhy55glfg
	,cdc.dhy55glfg_conv
	,cdc.dhy55glap
	,cdc.dhy55glap_conv
	,cdc.dhy55srce
	,cdc.dhedbt
	,cdc.dhedbt_conv
	,cdc.dhedtn
	,cdc.dhedtn_conv
	,cdc.dhy55spdl
	,cdc.dhy55spdl_conv
	,cdc.dhcars
	,cdc.dhcars_conv
	,cdc.dhflag
	,cdc.dhflag_conv
	,cdc.dhuser
	,cdc.dhuser_conv
	,cdc.dhpid
	,cdc.dhpid_conv
	,cdc.dhjobn
	,cdc.dhjobn_conv
	,cdc.dhupmj
	,cdc.dhupmj_conv
	,cdc.dhupmt
	,cdc.dhurcd
	,cdc.dhurcd_conv
	,cdc.dhurdt
	,cdc.dhurdt_conv
	,cdc.dhurat
	,cdc.dhurat_conv
	,cdc.dhurab
	,cdc.dhurab_conv
	,cdc.dhurrf
	,cdc.dhurrf_conv
	,cdc.dhrcd
	,cdc.dhy55char1
	,cdc.dhy55char1_conv
	,cdc.dhy55char2
	,cdc.dhy55char2_conv
	,cdc.dhy55date1
	,cdc.dhy55date1_conv
	,cdc.dhy55date2
	,cdc.dhy55date2_conv
	,cdc.dhy55strg1
	,cdc.dhy55strg1_conv
	,cdc.dhy55strg2
	,cdc.dhy55strg2_conv
	,cdc.dhy55strg3
	,cdc.dhy55strg3_conv
	,cdc.dhy55strg4
	,cdc.dhy55strg4_conv
	,cdc.dhy55strg5
	,cdc.dhy55strg5_conv
	,cdc.dhy55strg6
	,cdc.dhy55strg6_conv
	,cdc.dhy55strg7
	,cdc.dhy55strg7_conv
	,cdc.dhy55strg8
	,cdc.dhy55strg8_conv
	,cdc.dhy55strg9
	,cdc.dhy55strg9_conv
	,cdc.dhy55strg10
	,cdc.dhy55strg10_conv
	,cdc.dhy55flg1
	,cdc.dhy55flg1_conv
	,cdc.dhy55flg2
	,cdc.dhy55flg2_conv
	,cdc.dhy55num3
	,cdc.dhy55num3_conv
	,cdc.dhy55num4
	,cdc.dhy55num4_conv
	,cdc.dhy55time1
	,cdc.dhy55time2
	,cdc.dhy55amnt1
	,cdc.dhy55amnt1_conv
	,cdc.dhy55amnt2
	,cdc.dhy55amnt2_conv
	,cdc.dhum
	,cdc.dhhrsact
	,cdc.dhhrsact_conv
	,cdc.dhflg2
	,cdc.dhflg2_conv
	,cdc.dhflg3
	,cdc.dhflg3_conv
	,cdc.dhflg4
	,cdc.dhflg4_conv
	,cdc.dhvmcu
	,cdc.dhvmcu_conv
	,cdc.dhy55disn
	,cdc.dhefr
	,cdc.dhefr_conv
	,cdc.dhhrwt
	,cdc.dhhrwt_conv
	,cdc.dhaltd
	,cdc.dhaltd_conv
	,cdc.dhdrtm
	,cdc.dhdrtm_conv
	,cdc.dhflg1
	,cdc.dhflg1_conv
	,cdc.dhapsfact
	,cdc.dhapsfact_conv
	,cdc.dhprqu
	,cdc.dhprqu_conv
	,cdc.dhuom1
	,cdc.dhmc1
	,cdc.dhmc1_conv
	,cdc.dhtcap
	,cdc.dhtcap_conv
	,cdc.dhot1
	,cdc.dhot1_conv
	,cdc.dhcrcp
	,cdc.dhcrcp_conv
	,cdc.dhpraa
	,cdc.dhpraa_conv
	,cdc.dhaoth
	,cdc.dhaoth_conv
	,cdc.dhchgamt
	,cdc.dhchgamt_conv
	,cdc.dhnbr3
	,cdc.dhnbr2
	,cdc.dhnbr1
	,cdc.dhnocm
	,cdc.dhnocm_conv
	,cdc.dhnbrsld
	,cdc.dhnbrsld_conv
	,cdc.dhtold
	,cdc.dhtold_conv
	,cdc.dhodst
	,cdc.dhsclq
	,cdc.dhsclq_conv
	,cdc.dhy55trtm
	,cdc.dhorgn
	,cdc.dhtrdt
	,cdc.dhtrdt_conv
	,cdc.dhsttme
	,cdc.dhstdate
	,cdc.dhstdate_conv
	,cdc.dhdisn
	,cdc.dhdtai
	,cdc.dhdtai_conv
	,cdc.dhy55elts2
	,cdc.dhy55elts1
	,cdc.dhy55elts
	,cdc.dhtme0
	,cdc.dhacttime
	,cdc.dhacttime_conv
	,cdc.dhbstn
	,cdc.dhbstn_conv
	,cdc.dhancc
	,cdc.dhdte
	,cdc.dhdte_conv
	,cdc.dhukid
	,cdc.dhxpitraid
	,cdc.dhxpitraid_conv
	,cdc.dhy55vg
	,cdc.dhvtyp
	,cdc.dhvtyp_conv
	,cdc.dhy55rm1
	,cdc.dhy55rm1_conv
	,cdc.dhy55rm2
	,cdc.dhy55rm2_conv
	,cdc.dhy55stter
	,cdc.dhy55stter_conv
	,cdc.dhy55edter
	,cdc.dhy55edter_conv
	,cdc.dhy55wrcd
	,cdc.dhy55sapflg
	,cdc.dhy55sapflg_conv
	,cdc.dhy55mnobsp
	,cdc.dhy55mcth
	,cdc.dhy55mcth_conv
	,cdc.dhy55wttmh
	,cdc.dhy55wttmh_conv
	,cdc.dhy55wktmh
	,cdc.dhy55wktmh_conv
	,cdc.dhy55nstp
	,cdc.dhy55nstp_conv
FROM stg_jde_qat.tmp_upsert_f554900_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f554900_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f554900_new AF:{{ task_instance_key_str }}' ) 