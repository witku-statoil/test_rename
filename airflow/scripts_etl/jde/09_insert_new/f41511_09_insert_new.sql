-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f41511_new
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
	,padoc
	,pajeln
	,padct
	,pakco
	,pakco_conv
	,padoco
	,padcto
	,pakcoo
	,pakcoo_conv
	,palnid
	,palnid_conv
	,panlin
	,padgl
	,padgl_conv
	,patrdj
	,patrdj_conv
	,paicu
	,patrcd
	,patrcd_conv
	,pamcu
	,pamcu_conv
	,patkid
	,patkid_conv
	,paitm
	,patrno
	,patemp
	,patemp_conv
	,pastpu
	,padnty
	,padnty_conv
	,padntp
	,padetp
	,padetp_conv
	,padtpu
	,paambr
	,paambr_conv
	,pabum3
	,paambi
	,paambi_conv
	,paambu
	,pavcf
	,pavcf_conv
	,pastok
	,pastok_conv
	,pabum4
	,pawgtr
	,pawgtr_conv
	,pabum5
	,pastum
	,pastum_conv
	,pabum6
	,parcd
	,paupcf
	,paupcf_conv
	,palotn
	,palotn_conv
	,palots
	,pammej
	,pammej_conv
	,pauncs
	,pauncs_conv
	,paecst
	,paecst_conv
	,patrex
	,patrex_conv
	,paan8
	,padmct
	,padmct_conv
	,padmcs
	,paacor
	,paacor_conv
	,pahcor
	,pahcor_conv
	,padipt
	,pagdip
	,pagdip_conv
	,pawdip
	,pawdip_conv
	,pavapp
	,pavapp_conv
	,papreu
	,palpgv
	,palpgv_conv
	,patpu1
	,paslip
	,pavapw
	,pavapw_conv
	,pabum0
	,pavapv
	,pavapv_conv
	,pabum9
	,paliqw
	,paliqw_conv
	,pabum7
	,paliqv
	,paliqv_conv
	,pabum8
	,paovol
	,paovol_conv
	,pabum2
	,pardtm
	,padte
	,padte_conv
	,pametn
	,pametn_conv
	,paopnr
	,paopnr_conv
	,papncr
	,papncr_conv
	,pavehi
	,pavehi_conv
	,pavmcu
	,pavmcu_conv
	,patrp
	,pacmpn
	,pacmpn_conv
	,pabfwt
	,pabfwt_conv
	,pabwtu
	,paafwt
	,paafwt_conv
	,paawtu
	,pathdt
	,pathdt_conv
	,paopdt
	,paopdt_conv
	,palrst
	,parecs
	,papgms
	,paglrn
	,paglrn_conv
	,paptnr
	,paurcd
	,paurcd_conv
	,paurat
	,paurat_conv
	,paurab
	,paurab_conv
	,paurrf
	,paurrf_conv
	,paurdt
	,paurdt_conv
	,pauser
	,pauser_conv
	,papid
	,papid_conv
	,pajobn
	,pajobn_conv
	,paupmj
	,paupmj_conv
	,patday
	,paukid
	,pabpas
	,pabpas_conv
	,palotc
	,palotc_conv
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
	,cdc.padoc
	,cdc.pajeln
	,cdc.padct
	,cdc.pakco
	,cdc.pakco_conv
	,cdc.padoco
	,cdc.padcto
	,cdc.pakcoo
	,cdc.pakcoo_conv
	,cdc.palnid
	,cdc.palnid_conv
	,cdc.panlin
	,cdc.padgl
	,cdc.padgl_conv
	,cdc.patrdj
	,cdc.patrdj_conv
	,cdc.paicu
	,cdc.patrcd
	,cdc.patrcd_conv
	,cdc.pamcu
	,cdc.pamcu_conv
	,cdc.patkid
	,cdc.patkid_conv
	,cdc.paitm
	,cdc.patrno
	,cdc.patemp
	,cdc.patemp_conv
	,cdc.pastpu
	,cdc.padnty
	,cdc.padnty_conv
	,cdc.padntp
	,cdc.padetp
	,cdc.padetp_conv
	,cdc.padtpu
	,cdc.paambr
	,cdc.paambr_conv
	,cdc.pabum3
	,cdc.paambi
	,cdc.paambi_conv
	,cdc.paambu
	,cdc.pavcf
	,cdc.pavcf_conv
	,cdc.pastok
	,cdc.pastok_conv
	,cdc.pabum4
	,cdc.pawgtr
	,cdc.pawgtr_conv
	,cdc.pabum5
	,cdc.pastum
	,cdc.pastum_conv
	,cdc.pabum6
	,cdc.parcd
	,cdc.paupcf
	,cdc.paupcf_conv
	,cdc.palotn
	,cdc.palotn_conv
	,cdc.palots
	,cdc.pammej
	,cdc.pammej_conv
	,cdc.pauncs
	,cdc.pauncs_conv
	,cdc.paecst
	,cdc.paecst_conv
	,cdc.patrex
	,cdc.patrex_conv
	,cdc.paan8
	,cdc.padmct
	,cdc.padmct_conv
	,cdc.padmcs
	,cdc.paacor
	,cdc.paacor_conv
	,cdc.pahcor
	,cdc.pahcor_conv
	,cdc.padipt
	,cdc.pagdip
	,cdc.pagdip_conv
	,cdc.pawdip
	,cdc.pawdip_conv
	,cdc.pavapp
	,cdc.pavapp_conv
	,cdc.papreu
	,cdc.palpgv
	,cdc.palpgv_conv
	,cdc.patpu1
	,cdc.paslip
	,cdc.pavapw
	,cdc.pavapw_conv
	,cdc.pabum0
	,cdc.pavapv
	,cdc.pavapv_conv
	,cdc.pabum9
	,cdc.paliqw
	,cdc.paliqw_conv
	,cdc.pabum7
	,cdc.paliqv
	,cdc.paliqv_conv
	,cdc.pabum8
	,cdc.paovol
	,cdc.paovol_conv
	,cdc.pabum2
	,cdc.pardtm
	,cdc.padte
	,cdc.padte_conv
	,cdc.pametn
	,cdc.pametn_conv
	,cdc.paopnr
	,cdc.paopnr_conv
	,cdc.papncr
	,cdc.papncr_conv
	,cdc.pavehi
	,cdc.pavehi_conv
	,cdc.pavmcu
	,cdc.pavmcu_conv
	,cdc.patrp
	,cdc.pacmpn
	,cdc.pacmpn_conv
	,cdc.pabfwt
	,cdc.pabfwt_conv
	,cdc.pabwtu
	,cdc.paafwt
	,cdc.paafwt_conv
	,cdc.paawtu
	,cdc.pathdt
	,cdc.pathdt_conv
	,cdc.paopdt
	,cdc.paopdt_conv
	,cdc.palrst
	,cdc.parecs
	,cdc.papgms
	,cdc.paglrn
	,cdc.paglrn_conv
	,cdc.paptnr
	,cdc.paurcd
	,cdc.paurcd_conv
	,cdc.paurat
	,cdc.paurat_conv
	,cdc.paurab
	,cdc.paurab_conv
	,cdc.paurrf
	,cdc.paurrf_conv
	,cdc.paurdt
	,cdc.paurdt_conv
	,cdc.pauser
	,cdc.pauser_conv
	,cdc.papid
	,cdc.papid_conv
	,cdc.pajobn
	,cdc.pajobn_conv
	,cdc.paupmj
	,cdc.paupmj_conv
	,cdc.patday
	,cdc.paukid
	,cdc.pabpas
	,cdc.pabpas_conv
	,cdc.palotc
	,cdc.palotc_conv
FROM stg_jde.tmp_upsert_f41511_cdc cdc
LEFT OUTER JOIN rep_jde.f41511_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f41511_new AF:{{ task_instance_key_str }}' ) 