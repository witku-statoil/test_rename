-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f4301_new
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
	,phkcoo
	,phkcoo_conv
	,phdoco
	,phdcto
	,phsfxo
	,phsfxo_conv
	,phmcu
	,phmcu_conv
	,phokco
	,phokco_conv
	,phoorn
	,phoorn_conv
	,phocto
	,phrkco
	,phrkco_conv
	,phrorn
	,phrorn_conv
	,phrcto
	,phan8
	,phshan
	,phdrqj
	,phdrqj_conv
	,phtrdj
	,phtrdj_conv
	,phpddj
	,phpddj_conv
	,phopdj
	,phopdj_conv
	,phaddj
	,phaddj_conv
	,phcndj
	,phcndj_conv
	,phpefj
	,phpefj_conv
	,phppdj
	,phppdj_conv
	,phpsdj
	,phpsdj_conv
	,phvr01
	,phvr01_conv
	,phvr02
	,phvr02_conv
	,phdel1
	,phdel1_conv
	,phdel2
	,phdel2_conv
	,phrmk
	,phrmk_conv
	,phdesc
	,phdesc_conv
	,phinmg
	,phasn
	,phprgp
	,phptc
	,phptc_conv
	,phexr1
	,phtxa1
	,phtxa1_conv
	,phtxct
	,phtxct_conv
	,phhold
	,phatxt
	,phatxt_conv
	,phinvc
	,phinvc_conv
	,phntr
	,phcnid
	,phcnid_conv
	,phfrth
	,phzon
	,phanby
	,phancr
	,phmot
	,phcot
	,phrcd
	,phfrtc
	,phfrtc_conv
	,phfuf1
	,phfuf2
	,phfuf2_conv
	,photot
	,photot_conv
	,phpcrt
	,phpcrt_conv
	,phrtnr
	,phrtnr_conv
	,phwumd
	,phvumd
	,phpurg
	,phpurg_conv
	,phlgct
	,phlgct_conv
	,phprom
	,phmaty
	,phosts
	,phosts_conv
	,phavch
	,phprpy
	,phprpy_conv
	,phcrmd
	,phprp5
	,phartg
	,phartg_conv
	,phcord
	,phcrrm
	,phcrcd
	,phcrcd_conv
	,phcrr
	,phlngp
	,phfap
	,phfap_conv
	,phorby
	,phorby_conv
	,phtkby
	,phtkby_conv
	,phurcd
	,phurcd_conv
	,phurdt
	,phurdt_conv
	,phurat
	,phurat_conv
	,phurab
	,phurab_conv
	,phurrf
	,phurrf_conv
	,phuser
	,phuser_conv
	,phpid
	,phpid_conv
	,phjobn
	,phjobn_conv
	,phupmj
	,phupmj_conv
	,phtday
	,phrsht
	,phrsht_conv
	,phdrqt
	,phdoc1
	,phdct4
	,phbcrc
	,phbcrc_conv
	,phmkfr
	,phpohp01
	,phpohp01_conv
	,phpohp02
	,phpohp02_conv
	,phpohp03
	,phpohp03_conv
	,phpohp04
	,phpohp04_conv
	,phpohp05
	,phpohp05_conv
	,phpohp06
	,phpohp06_conv
	,phpohp07
	,phpohp07_conv
	,phpohp08
	,phpohp08_conv
	,phpohp09
	,phpohp09_conv
	,phpohp10
	,phpohp10_conv
	,phpohp11
	,phpohp11_conv
	,phpohp12
	,phpohp12_conv
	,phpohc01
	,phpohc02
	,phpohc03
	,phpohc04
	,phpohc05
	,phpohc06
	,phpohc07
	,phpohc08
	,phpohc09
	,phpohc10
	,phpohc11
	,phpohc12
	,phpohd01
	,phpohd01_conv
	,phpohd02
	,phpohd02_conv
	,phpohab01
	,phpohab01_conv
	,phpohab02
	,phpohab02_conv
	,phcukid
	,phcukid_conv
	,phpohp13
	,phpohp13_conv
	,phpohu01
	,phpohu01_conv
	,phpohu02
	,phpohu02_conv
	,phreti
	,phreti_conv
	,phclass01
	,phclass02
	,phclass03
	,phclass04
	,phclass05
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
	,cdc.phkcoo
	,cdc.phkcoo_conv
	,cdc.phdoco
	,cdc.phdcto
	,cdc.phsfxo
	,cdc.phsfxo_conv
	,cdc.phmcu
	,cdc.phmcu_conv
	,cdc.phokco
	,cdc.phokco_conv
	,cdc.phoorn
	,cdc.phoorn_conv
	,cdc.phocto
	,cdc.phrkco
	,cdc.phrkco_conv
	,cdc.phrorn
	,cdc.phrorn_conv
	,cdc.phrcto
	,cdc.phan8
	,cdc.phshan
	,cdc.phdrqj
	,cdc.phdrqj_conv
	,cdc.phtrdj
	,cdc.phtrdj_conv
	,cdc.phpddj
	,cdc.phpddj_conv
	,cdc.phopdj
	,cdc.phopdj_conv
	,cdc.phaddj
	,cdc.phaddj_conv
	,cdc.phcndj
	,cdc.phcndj_conv
	,cdc.phpefj
	,cdc.phpefj_conv
	,cdc.phppdj
	,cdc.phppdj_conv
	,cdc.phpsdj
	,cdc.phpsdj_conv
	,cdc.phvr01
	,cdc.phvr01_conv
	,cdc.phvr02
	,cdc.phvr02_conv
	,cdc.phdel1
	,cdc.phdel1_conv
	,cdc.phdel2
	,cdc.phdel2_conv
	,cdc.phrmk
	,cdc.phrmk_conv
	,cdc.phdesc
	,cdc.phdesc_conv
	,cdc.phinmg
	,cdc.phasn
	,cdc.phprgp
	,cdc.phptc
	,cdc.phptc_conv
	,cdc.phexr1
	,cdc.phtxa1
	,cdc.phtxa1_conv
	,cdc.phtxct
	,cdc.phtxct_conv
	,cdc.phhold
	,cdc.phatxt
	,cdc.phatxt_conv
	,cdc.phinvc
	,cdc.phinvc_conv
	,cdc.phntr
	,cdc.phcnid
	,cdc.phcnid_conv
	,cdc.phfrth
	,cdc.phzon
	,cdc.phanby
	,cdc.phancr
	,cdc.phmot
	,cdc.phcot
	,cdc.phrcd
	,cdc.phfrtc
	,cdc.phfrtc_conv
	,cdc.phfuf1
	,cdc.phfuf2
	,cdc.phfuf2_conv
	,cdc.photot
	,cdc.photot_conv
	,cdc.phpcrt
	,cdc.phpcrt_conv
	,cdc.phrtnr
	,cdc.phrtnr_conv
	,cdc.phwumd
	,cdc.phvumd
	,cdc.phpurg
	,cdc.phpurg_conv
	,cdc.phlgct
	,cdc.phlgct_conv
	,cdc.phprom
	,cdc.phmaty
	,cdc.phosts
	,cdc.phosts_conv
	,cdc.phavch
	,cdc.phprpy
	,cdc.phprpy_conv
	,cdc.phcrmd
	,cdc.phprp5
	,cdc.phartg
	,cdc.phartg_conv
	,cdc.phcord
	,cdc.phcrrm
	,cdc.phcrcd
	,cdc.phcrcd_conv
	,cdc.phcrr
	,cdc.phlngp
	,cdc.phfap
	,cdc.phfap_conv
	,cdc.phorby
	,cdc.phorby_conv
	,cdc.phtkby
	,cdc.phtkby_conv
	,cdc.phurcd
	,cdc.phurcd_conv
	,cdc.phurdt
	,cdc.phurdt_conv
	,cdc.phurat
	,cdc.phurat_conv
	,cdc.phurab
	,cdc.phurab_conv
	,cdc.phurrf
	,cdc.phurrf_conv
	,cdc.phuser
	,cdc.phuser_conv
	,cdc.phpid
	,cdc.phpid_conv
	,cdc.phjobn
	,cdc.phjobn_conv
	,cdc.phupmj
	,cdc.phupmj_conv
	,cdc.phtday
	,cdc.phrsht
	,cdc.phrsht_conv
	,cdc.phdrqt
	,cdc.phdoc1
	,cdc.phdct4
	,cdc.phbcrc
	,cdc.phbcrc_conv
	,cdc.phmkfr
	,cdc.phpohp01
	,cdc.phpohp01_conv
	,cdc.phpohp02
	,cdc.phpohp02_conv
	,cdc.phpohp03
	,cdc.phpohp03_conv
	,cdc.phpohp04
	,cdc.phpohp04_conv
	,cdc.phpohp05
	,cdc.phpohp05_conv
	,cdc.phpohp06
	,cdc.phpohp06_conv
	,cdc.phpohp07
	,cdc.phpohp07_conv
	,cdc.phpohp08
	,cdc.phpohp08_conv
	,cdc.phpohp09
	,cdc.phpohp09_conv
	,cdc.phpohp10
	,cdc.phpohp10_conv
	,cdc.phpohp11
	,cdc.phpohp11_conv
	,cdc.phpohp12
	,cdc.phpohp12_conv
	,cdc.phpohc01
	,cdc.phpohc02
	,cdc.phpohc03
	,cdc.phpohc04
	,cdc.phpohc05
	,cdc.phpohc06
	,cdc.phpohc07
	,cdc.phpohc08
	,cdc.phpohc09
	,cdc.phpohc10
	,cdc.phpohc11
	,cdc.phpohc12
	,cdc.phpohd01
	,cdc.phpohd01_conv
	,cdc.phpohd02
	,cdc.phpohd02_conv
	,cdc.phpohab01
	,cdc.phpohab01_conv
	,cdc.phpohab02
	,cdc.phpohab02_conv
	,cdc.phcukid
	,cdc.phcukid_conv
	,cdc.phpohp13
	,cdc.phpohp13_conv
	,cdc.phpohu01
	,cdc.phpohu01_conv
	,cdc.phpohu02
	,cdc.phpohu02_conv
	,cdc.phreti
	,cdc.phreti_conv
	,cdc.phclass01
	,cdc.phclass02
	,cdc.phclass03
	,cdc.phclass04
	,cdc.phclass05
FROM stg_jde.tmp_upsert_f4301_cdc cdc
LEFT OUTER JOIN rep_jde.f4301_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f4301_new AF:{{ task_instance_key_str }}' ) 