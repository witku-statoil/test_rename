-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f49211_new
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
	,uddoco
	,uddcto
	,udkcoo
	,udkcoo_conv
	,udlnid
	,udlnid_conv
	,udvmcu
	,udvmcu_conv
	,udtrp
	,udload
	,udload_conv
	,uddsgp
	,udbpfg
	,uddstn
	,udum
	,uddeff
	,uddeff_conv
	,uddunc
	,uddunc_conv
	,udfduc
	,udfduc_conv
	,uddrev
	,uddrev_conv
	,udfdrv
	,udfdrv_conv
	,udanum
	,udanum_conv
	,udsidt
	,udsidt_conv
	,udincy
	,udlddt
	,udlddt_conv
	,udldtm
	,uddcdt
	,uddcdt_conv
	,udpcqy
	,udpcqy_conv
	,uduomc
	,udtemp
	,udtemp_conv
	,udstpu
	,uddnty
	,uddnty_conv
	,uddntp
	,uddetp
	,uddetp_conv
	,uddtpu
	,udvcf
	,udvcf_conv
	,udpras
	,udcp01
	,udiqty
	,udiqty_conv
	,udstum
	,udstum_conv
	,udbum6
	,udambr
	,udambr_conv
	,udbum3
	,udwgtr
	,udwgtr_conv
	,udbum5
	,udfrtv
	,udfrtv_conv
	,udfrtd
	,udfrtd_conv
	,udfrcc
	,udfrcc_conv
	,udfrvc
	,udfrvc_conv
	,udpveh
	,udpveh_conv
	,udrlno
	,udrlno_conv
	,udmcur
	,udmcur_conv
	,udfltn
	,udfltn_conv
	,uddsnn
	,udarct
	,udsorg
	,udeltm
	,udptnr
	,udian8
	,udptc
	,udptc_conv
	,uddoc
	,uddct
	,udkco
	,udkco_conv
	,udcrr
	,udcrcd
	,udcrcd_conv
	,udtxa1
	,udtxa1_conv
	,udexr1
	,udfrdm
	,udfupt
	,udrino
	,udrino_conv
	,udgloc
	,udgloc_conv
	,udauta
	,udauta_conv
	,udalph
	,udalph_conv
	,udmet1
	,udmet1_conv
	,udopn1
	,udopn1_conv
	,udpnr1
	,udpnr1_conv
	,udmet2
	,udmet2_conv
	,udopn2
	,udopn2_conv
	,udpnr2
	,udpnr2_conv
	,udmet3
	,udmet3_conv
	,udopn3
	,udopn3_conv
	,udpnr3
	,udpnr3_conv
	,udardt
	,udardt_conv
	,udartm
	,uddpdt
	,uddpdt_conv
	,uddetm
	,uddstj
	,uddstj_conv
	,udstm
	,udend
	,udend_conv
	,udetm
	,udtv01
	,udtv01_conv
	,udtv02
	,udtv02_conv
	,udtv03
	,udtv03_conv
	,udtv04
	,udtv04_conv
	,udtv05
	,udtv05_conv
	,udtvcd
	,udtvcd_conv
	,udtvqt
	,udtvdt
	,udtvdt_conv
	,udtvum
	,uduser
	,uduser_conv
	,udpid
	,udpid_conv
	,udjobn
	,udjobn_conv
	,udupmj
	,udupmj_conv
	,udtday
	,udsbck
	,udsbck_conv
	,udedck
	,udedck_conv
	,udcmdm
	,udbbck
	,udbbck_conv
	,udrqsj
	,udrqsj_conv
	,udpsdj
	,udpsdj_conv
	,udadlj
	,udadlj_conv
	,udsub
	,udsub_conv
	,udstts
	,udstts_conv
	,udratt
	,udratt_conv
	,udfuf1
	,udfrtc
	,udfrtc_conv
	,udfrat
	,udaft
	,udaft_conv
	,udomcu
	,udomcu_conv
	,udobj
	,udobj_conv
	,udlt
	,udfapp
	,udfapp_conv
	,uddspr
	,uddspr_conv
	,uddsft
	,uddmt1
	,uddmt1_conv
	,uddms1
	,udcot
	,udcmgl
	,udbalu
	,udbalu_conv
	,udapot
	,udapot_conv
	,udacgd
	,udani
	,udani_conv
	,udaid
	,udaid_conv
	,udopol
	,udopol_conv
	,udopbo
	,udopid
	,udopid_conv
	,udopcs
	,udopcs_conv
	,udopll
	,udopll_conv
	,udopms
	,udopms_conv
	,udopss
	,udopss_conv
	,udopba
	,udopba_conv
	,udfxsr
	,udfxsr_conv
	,udopmg
	,udopmg_conv
	,udopsg
	,udopsg_conv
	,udopyn
	,udopyn_conv
	,udopsu
	,udopsu_conv
	,udpsjobno
	,udpsjobno_conv
	,udjobsq
	,udjobsq_conv
	,udcardno
	,uddelbatch
	,uddelbatch_conv
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
	,cdc.uddoco
	,cdc.uddcto
	,cdc.udkcoo
	,cdc.udkcoo_conv
	,cdc.udlnid
	,cdc.udlnid_conv
	,cdc.udvmcu
	,cdc.udvmcu_conv
	,cdc.udtrp
	,cdc.udload
	,cdc.udload_conv
	,cdc.uddsgp
	,cdc.udbpfg
	,cdc.uddstn
	,cdc.udum
	,cdc.uddeff
	,cdc.uddeff_conv
	,cdc.uddunc
	,cdc.uddunc_conv
	,cdc.udfduc
	,cdc.udfduc_conv
	,cdc.uddrev
	,cdc.uddrev_conv
	,cdc.udfdrv
	,cdc.udfdrv_conv
	,cdc.udanum
	,cdc.udanum_conv
	,cdc.udsidt
	,cdc.udsidt_conv
	,cdc.udincy
	,cdc.udlddt
	,cdc.udlddt_conv
	,cdc.udldtm
	,cdc.uddcdt
	,cdc.uddcdt_conv
	,cdc.udpcqy
	,cdc.udpcqy_conv
	,cdc.uduomc
	,cdc.udtemp
	,cdc.udtemp_conv
	,cdc.udstpu
	,cdc.uddnty
	,cdc.uddnty_conv
	,cdc.uddntp
	,cdc.uddetp
	,cdc.uddetp_conv
	,cdc.uddtpu
	,cdc.udvcf
	,cdc.udvcf_conv
	,cdc.udpras
	,cdc.udcp01
	,cdc.udiqty
	,cdc.udiqty_conv
	,cdc.udstum
	,cdc.udstum_conv
	,cdc.udbum6
	,cdc.udambr
	,cdc.udambr_conv
	,cdc.udbum3
	,cdc.udwgtr
	,cdc.udwgtr_conv
	,cdc.udbum5
	,cdc.udfrtv
	,cdc.udfrtv_conv
	,cdc.udfrtd
	,cdc.udfrtd_conv
	,cdc.udfrcc
	,cdc.udfrcc_conv
	,cdc.udfrvc
	,cdc.udfrvc_conv
	,cdc.udpveh
	,cdc.udpveh_conv
	,cdc.udrlno
	,cdc.udrlno_conv
	,cdc.udmcur
	,cdc.udmcur_conv
	,cdc.udfltn
	,cdc.udfltn_conv
	,cdc.uddsnn
	,cdc.udarct
	,cdc.udsorg
	,cdc.udeltm
	,cdc.udptnr
	,cdc.udian8
	,cdc.udptc
	,cdc.udptc_conv
	,cdc.uddoc
	,cdc.uddct
	,cdc.udkco
	,cdc.udkco_conv
	,cdc.udcrr
	,cdc.udcrcd
	,cdc.udcrcd_conv
	,cdc.udtxa1
	,cdc.udtxa1_conv
	,cdc.udexr1
	,cdc.udfrdm
	,cdc.udfupt
	,cdc.udrino
	,cdc.udrino_conv
	,cdc.udgloc
	,cdc.udgloc_conv
	,cdc.udauta
	,cdc.udauta_conv
	,cdc.udalph
	,cdc.udalph_conv
	,cdc.udmet1
	,cdc.udmet1_conv
	,cdc.udopn1
	,cdc.udopn1_conv
	,cdc.udpnr1
	,cdc.udpnr1_conv
	,cdc.udmet2
	,cdc.udmet2_conv
	,cdc.udopn2
	,cdc.udopn2_conv
	,cdc.udpnr2
	,cdc.udpnr2_conv
	,cdc.udmet3
	,cdc.udmet3_conv
	,cdc.udopn3
	,cdc.udopn3_conv
	,cdc.udpnr3
	,cdc.udpnr3_conv
	,cdc.udardt
	,cdc.udardt_conv
	,cdc.udartm
	,cdc.uddpdt
	,cdc.uddpdt_conv
	,cdc.uddetm
	,cdc.uddstj
	,cdc.uddstj_conv
	,cdc.udstm
	,cdc.udend
	,cdc.udend_conv
	,cdc.udetm
	,cdc.udtv01
	,cdc.udtv01_conv
	,cdc.udtv02
	,cdc.udtv02_conv
	,cdc.udtv03
	,cdc.udtv03_conv
	,cdc.udtv04
	,cdc.udtv04_conv
	,cdc.udtv05
	,cdc.udtv05_conv
	,cdc.udtvcd
	,cdc.udtvcd_conv
	,cdc.udtvqt
	,cdc.udtvdt
	,cdc.udtvdt_conv
	,cdc.udtvum
	,cdc.uduser
	,cdc.uduser_conv
	,cdc.udpid
	,cdc.udpid_conv
	,cdc.udjobn
	,cdc.udjobn_conv
	,cdc.udupmj
	,cdc.udupmj_conv
	,cdc.udtday
	,cdc.udsbck
	,cdc.udsbck_conv
	,cdc.udedck
	,cdc.udedck_conv
	,cdc.udcmdm
	,cdc.udbbck
	,cdc.udbbck_conv
	,cdc.udrqsj
	,cdc.udrqsj_conv
	,cdc.udpsdj
	,cdc.udpsdj_conv
	,cdc.udadlj
	,cdc.udadlj_conv
	,cdc.udsub
	,cdc.udsub_conv
	,cdc.udstts
	,cdc.udstts_conv
	,cdc.udratt
	,cdc.udratt_conv
	,cdc.udfuf1
	,cdc.udfrtc
	,cdc.udfrtc_conv
	,cdc.udfrat
	,cdc.udaft
	,cdc.udaft_conv
	,cdc.udomcu
	,cdc.udomcu_conv
	,cdc.udobj
	,cdc.udobj_conv
	,cdc.udlt
	,cdc.udfapp
	,cdc.udfapp_conv
	,cdc.uddspr
	,cdc.uddspr_conv
	,cdc.uddsft
	,cdc.uddmt1
	,cdc.uddmt1_conv
	,cdc.uddms1
	,cdc.udcot
	,cdc.udcmgl
	,cdc.udbalu
	,cdc.udbalu_conv
	,cdc.udapot
	,cdc.udapot_conv
	,cdc.udacgd
	,cdc.udani
	,cdc.udani_conv
	,cdc.udaid
	,cdc.udaid_conv
	,cdc.udopol
	,cdc.udopol_conv
	,cdc.udopbo
	,cdc.udopid
	,cdc.udopid_conv
	,cdc.udopcs
	,cdc.udopcs_conv
	,cdc.udopll
	,cdc.udopll_conv
	,cdc.udopms
	,cdc.udopms_conv
	,cdc.udopss
	,cdc.udopss_conv
	,cdc.udopba
	,cdc.udopba_conv
	,cdc.udfxsr
	,cdc.udfxsr_conv
	,cdc.udopmg
	,cdc.udopmg_conv
	,cdc.udopsg
	,cdc.udopsg_conv
	,cdc.udopyn
	,cdc.udopyn_conv
	,cdc.udopsu
	,cdc.udopsu_conv
	,cdc.udpsjobno
	,cdc.udpsjobno_conv
	,cdc.udjobsq
	,cdc.udjobsq_conv
	,cdc.udcardno
	,cdc.uddelbatch
	,cdc.uddelbatch_conv
FROM stg_jde.tmp_upsert_f49211_cdc cdc
LEFT OUTER JOIN rep_jde.f49211_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f49211_new AF:{{ task_instance_key_str }}' ) 