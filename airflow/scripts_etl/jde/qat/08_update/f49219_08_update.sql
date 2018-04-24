-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f49219_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[uddoco] = cdc.[uddoco]
	,[uddcto] = cdc.[uddcto]
	,[udkcoo] = cdc.[udkcoo]
	,[udkcoo_conv] = cdc.[udkcoo_conv]
	,[udlnid] = cdc.[udlnid]
	,[udlnid_conv] = cdc.[udlnid_conv]
	,[udvmcu] = cdc.[udvmcu]
	,[udvmcu_conv] = cdc.[udvmcu_conv]
	,[udtrp] = cdc.[udtrp]
	,[udload] = cdc.[udload]
	,[udload_conv] = cdc.[udload_conv]
	,[uddsgp] = cdc.[uddsgp]
	,[udbpfg] = cdc.[udbpfg]
	,[uddstn] = cdc.[uddstn]
	,[udum] = cdc.[udum]
	,[uddeff] = cdc.[uddeff]
	,[uddeff_conv] = cdc.[uddeff_conv]
	,[uddunc] = cdc.[uddunc]
	,[uddunc_conv] = cdc.[uddunc_conv]
	,[udfduc] = cdc.[udfduc]
	,[udfduc_conv] = cdc.[udfduc_conv]
	,[uddrev] = cdc.[uddrev]
	,[uddrev_conv] = cdc.[uddrev_conv]
	,[udfdrv] = cdc.[udfdrv]
	,[udfdrv_conv] = cdc.[udfdrv_conv]
	,[udanum] = cdc.[udanum]
	,[udanum_conv] = cdc.[udanum_conv]
	,[udsidt] = cdc.[udsidt]
	,[udsidt_conv] = cdc.[udsidt_conv]
	,[udincy] = cdc.[udincy]
	,[udlddt] = cdc.[udlddt]
	,[udlddt_conv] = cdc.[udlddt_conv]
	,[udldtm] = cdc.[udldtm]
	,[uddcdt] = cdc.[uddcdt]
	,[uddcdt_conv] = cdc.[uddcdt_conv]
	,[udpcqy] = cdc.[udpcqy]
	,[udpcqy_conv] = cdc.[udpcqy_conv]
	,[uduomc] = cdc.[uduomc]
	,[udtemp] = cdc.[udtemp]
	,[udtemp_conv] = cdc.[udtemp_conv]
	,[udstpu] = cdc.[udstpu]
	,[uddnty] = cdc.[uddnty]
	,[uddnty_conv] = cdc.[uddnty_conv]
	,[uddntp] = cdc.[uddntp]
	,[uddetp] = cdc.[uddetp]
	,[uddetp_conv] = cdc.[uddetp_conv]
	,[uddtpu] = cdc.[uddtpu]
	,[udvcf] = cdc.[udvcf]
	,[udvcf_conv] = cdc.[udvcf_conv]
	,[udpras] = cdc.[udpras]
	,[udcp01] = cdc.[udcp01]
	,[udiqty] = cdc.[udiqty]
	,[udiqty_conv] = cdc.[udiqty_conv]
	,[udstum] = cdc.[udstum]
	,[udstum_conv] = cdc.[udstum_conv]
	,[udbum6] = cdc.[udbum6]
	,[udambr] = cdc.[udambr]
	,[udambr_conv] = cdc.[udambr_conv]
	,[udbum3] = cdc.[udbum3]
	,[udwgtr] = cdc.[udwgtr]
	,[udwgtr_conv] = cdc.[udwgtr_conv]
	,[udbum5] = cdc.[udbum5]
	,[udfrtv] = cdc.[udfrtv]
	,[udfrtv_conv] = cdc.[udfrtv_conv]
	,[udfrtd] = cdc.[udfrtd]
	,[udfrtd_conv] = cdc.[udfrtd_conv]
	,[udfrcc] = cdc.[udfrcc]
	,[udfrcc_conv] = cdc.[udfrcc_conv]
	,[udfrvc] = cdc.[udfrvc]
	,[udfrvc_conv] = cdc.[udfrvc_conv]
	,[udpveh] = cdc.[udpveh]
	,[udpveh_conv] = cdc.[udpveh_conv]
	,[udrlno] = cdc.[udrlno]
	,[udrlno_conv] = cdc.[udrlno_conv]
	,[udmcur] = cdc.[udmcur]
	,[udmcur_conv] = cdc.[udmcur_conv]
	,[udfltn] = cdc.[udfltn]
	,[udfltn_conv] = cdc.[udfltn_conv]
	,[uddsnn] = cdc.[uddsnn]
	,[udarct] = cdc.[udarct]
	,[udsorg] = cdc.[udsorg]
	,[udeltm] = cdc.[udeltm]
	,[udptnr] = cdc.[udptnr]
	,[udian8] = cdc.[udian8]
	,[udptc] = cdc.[udptc]
	,[udptc_conv] = cdc.[udptc_conv]
	,[uddoc] = cdc.[uddoc]
	,[uddct] = cdc.[uddct]
	,[udkco] = cdc.[udkco]
	,[udkco_conv] = cdc.[udkco_conv]
	,[udcrr] = cdc.[udcrr]
	,[udcrcd] = cdc.[udcrcd]
	,[udcrcd_conv] = cdc.[udcrcd_conv]
	,[udtxa1] = cdc.[udtxa1]
	,[udtxa1_conv] = cdc.[udtxa1_conv]
	,[udexr1] = cdc.[udexr1]
	,[udtv01] = cdc.[udtv01]
	,[udtv01_conv] = cdc.[udtv01_conv]
	,[udtv02] = cdc.[udtv02]
	,[udtv02_conv] = cdc.[udtv02_conv]
	,[udtv03] = cdc.[udtv03]
	,[udtv03_conv] = cdc.[udtv03_conv]
	,[udtv04] = cdc.[udtv04]
	,[udtv04_conv] = cdc.[udtv04_conv]
	,[udtv05] = cdc.[udtv05]
	,[udtv05_conv] = cdc.[udtv05_conv]
	,[udtvcd] = cdc.[udtvcd]
	,[udtvcd_conv] = cdc.[udtvcd_conv]
	,[udtvqt] = cdc.[udtvqt]
	,[udtvdt] = cdc.[udtvdt]
	,[udtvdt_conv] = cdc.[udtvdt_conv]
	,[udtvum] = cdc.[udtvum]
	,[uduser] = cdc.[uduser]
	,[uduser_conv] = cdc.[uduser_conv]
	,[udpid] = cdc.[udpid]
	,[udpid_conv] = cdc.[udpid_conv]
	,[udjobn] = cdc.[udjobn]
	,[udjobn_conv] = cdc.[udjobn_conv]
	,[udupmj] = cdc.[udupmj]
	,[udupmj_conv] = cdc.[udupmj_conv]
	,[udtday] = cdc.[udtday]
	,[udsbck] = cdc.[udsbck]
	,[udsbck_conv] = cdc.[udsbck_conv]
	,[udedck] = cdc.[udedck]
	,[udedck_conv] = cdc.[udedck_conv]
	,[udcmdm] = cdc.[udcmdm]
	,[udbbck] = cdc.[udbbck]
	,[udbbck_conv] = cdc.[udbbck_conv]
	,[udrqsj] = cdc.[udrqsj]
	,[udrqsj_conv] = cdc.[udrqsj_conv]
	,[udpsdj] = cdc.[udpsdj]
	,[udpsdj_conv] = cdc.[udpsdj_conv]
	,[udadlj] = cdc.[udadlj]
	,[udadlj_conv] = cdc.[udadlj_conv]
	,[udsub] = cdc.[udsub]
	,[udsub_conv] = cdc.[udsub_conv]
	,[udstts] = cdc.[udstts]
	,[udstts_conv] = cdc.[udstts_conv]
	,[udratt] = cdc.[udratt]
	,[udratt_conv] = cdc.[udratt_conv]
	,[udfuf1] = cdc.[udfuf1]
	,[udfrtc] = cdc.[udfrtc]
	,[udfrtc_conv] = cdc.[udfrtc_conv]
	,[udfrat] = cdc.[udfrat]
	,[udaft] = cdc.[udaft]
	,[udaft_conv] = cdc.[udaft_conv]
	,[udomcu] = cdc.[udomcu]
	,[udomcu_conv] = cdc.[udomcu_conv]
	,[udobj] = cdc.[udobj]
	,[udobj_conv] = cdc.[udobj_conv]
	,[udlt] = cdc.[udlt]
	,[udfapp] = cdc.[udfapp]
	,[udfapp_conv] = cdc.[udfapp_conv]
	,[uddspr] = cdc.[uddspr]
	,[uddspr_conv] = cdc.[uddspr_conv]
	,[uddsft] = cdc.[uddsft]
	,[uddmt1] = cdc.[uddmt1]
	,[uddmt1_conv] = cdc.[uddmt1_conv]
	,[uddms1] = cdc.[uddms1]
	,[udcot] = cdc.[udcot]
	,[udcmgl] = cdc.[udcmgl]
	,[udbalu] = cdc.[udbalu]
	,[udbalu_conv] = cdc.[udbalu_conv]
	,[udapot] = cdc.[udapot]
	,[udapot_conv] = cdc.[udapot_conv]
	,[udacgd] = cdc.[udacgd]
	,[udani] = cdc.[udani]
	,[udani_conv] = cdc.[udani_conv]
	,[udaid] = cdc.[udaid]
	,[udaid_conv] = cdc.[udaid_conv]
	,[udopol] = cdc.[udopol]
	,[udopol_conv] = cdc.[udopol_conv]
	,[udopbo] = cdc.[udopbo]
	,[udopid] = cdc.[udopid]
	,[udopid_conv] = cdc.[udopid_conv]
	,[udopcs] = cdc.[udopcs]
	,[udopcs_conv] = cdc.[udopcs_conv]
	,[udopll] = cdc.[udopll]
	,[udopll_conv] = cdc.[udopll_conv]
	,[udopms] = cdc.[udopms]
	,[udopms_conv] = cdc.[udopms_conv]
	,[udopss] = cdc.[udopss]
	,[udopss_conv] = cdc.[udopss_conv]
	,[udopba] = cdc.[udopba]
	,[udopba_conv] = cdc.[udopba_conv]
	,[udfxsr] = cdc.[udfxsr]
	,[udfxsr_conv] = cdc.[udfxsr_conv]
	,[udpsjobno] = cdc.[udpsjobno]
	,[udpsjobno_conv] = cdc.[udpsjobno_conv]
	,[udjobsq] = cdc.[udjobsq]
	,[udjobsq_conv] = cdc.[udjobsq_conv]
	,[udcardno] = cdc.[udcardno]
	,[uddelbatch] = cdc.[uddelbatch]
	,[uddelbatch_conv] = cdc.[uddelbatch_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f49219_cdc cdc
WHERE
    rep_jde_qat.f49219_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f49219_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f49219_new AF:{{ task_instance_key_str }}' ) 