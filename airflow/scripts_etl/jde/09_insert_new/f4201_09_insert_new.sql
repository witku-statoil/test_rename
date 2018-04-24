-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f4201_new
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
	,shkcoo
	,shkcoo_conv
	,shdoco
	,shdcto
	,shsfxo
	,shsfxo_conv
	,shmcu
	,shmcu_conv
	,shco
	,shco_conv
	,shokco
	,shokco_conv
	,shoorn
	,shoorn_conv
	,shocto
	,shrkco
	,shrkco_conv
	,shrorn
	,shrorn_conv
	,shrcto
	,shan8
	,shshan
	,shpa8
	,shdrqj
	,shdrqj_conv
	,shtrdj
	,shtrdj_conv
	,shpddj
	,shpddj_conv
	,shopdj
	,shopdj_conv
	,shaddj
	,shaddj_conv
	,shcndj
	,shcndj_conv
	,shpefj
	,shpefj_conv
	,shppdj
	,shppdj_conv
	,shvr01
	,shvr01_conv
	,shvr02
	,shvr02_conv
	,shdel1
	,shdel1_conv
	,shdel2
	,shdel2_conv
	,shinmg
	,shptc
	,shptc_conv
	,shryin
	,shasn
	,shprgp
	,shtrdc
	,shtrdc_conv
	,shpcrt
	,shpcrt_conv
	,shtxa1
	,shtxa1_conv
	,shexr1
	,shtxct
	,shtxct_conv
	,shatxt
	,shatxt_conv
	,shprio
	,shback
	,shback_conv
	,shsbal
	,shsbal_conv
	,shhold
	,shplst
	,shplst_conv
	,shinvc
	,shinvc_conv
	,shntr
	,shanby
	,shcars
	,shcars_conv
	,shmot
	,shcot
	,shrout
	,shstop
	,shzon
	,shcnid
	,shcnid_conv
	,shfrth
	,shaft
	,shaft_conv
	,shfuf1
	,shfrtc
	,shfrtc_conv
	,shmord
	,shmord_conv
	,shrcd
	,shfuf2
	,shfuf2_conv
	,shotot
	,shotot_conv
	,shtotc
	,shtotc_conv
	,shwumd
	,shvumd
	,shautn
	,shautn_conv
	,shcact
	,shcact_conv
	,shcexp
	,shcexp_conv
	,shsbli
	,shcrmd
	,shcrrm
	,shcrcd
	,shcrcd_conv
	,shcrr
	,shlngp
	,shfap
	,shfap_conv
	,shfcst
	,shfcst_conv
	,shorby
	,shorby_conv
	,shtkby
	,shtkby_conv
	,shurcd
	,shurcd_conv
	,shurdt
	,shurdt_conv
	,shurat
	,shurat_conv
	,shurab
	,shurab_conv
	,shurrf
	,shurrf_conv
	,shuser
	,shuser_conv
	,shpid
	,shpid_conv
	,shjobn
	,shjobn_conv
	,shupmj
	,shupmj_conv
	,shtday
	,shir01
	,shir01_conv
	,shir02
	,shir02_conv
	,shir03
	,shir03_conv
	,shir04
	,shir04_conv
	,shir05
	,shir05_conv
	,shvr03
	,shvr03_conv
	,shsoor
	,shsoor_conv
	,shpmdt
	,shrsdt
	,shrqsj
	,shrqsj_conv
	,shpstm
	,shpdtt
	,shoptt
	,shdrqt
	,shadtm
	,shadlj
	,shadlj_conv
	,shpban
	,shpban_conv
	,shitan
	,shitan_conv
	,shftan
	,shftan_conv
	,shdvan
	,shdvan_conv
	,shdoc1
	,shdct4
	,shcord
	,shbsc
	,shbcrc
	,shbcrc_conv
	,shauft
	,shaufi
	,shopbo
	,shoptc
	,shoptc_conv
	,shopld
	,shopld_conv
	,shopbk
	,shopbk_conv
	,shopsb
	,shopsb_conv
	,shopps
	,shopps_conv
	,shoppl
	,shoppl_conv
	,shopms
	,shopms_conv
	,shopss
	,shopss_conv
	,shopba
	,shopba_conv
	,shopll
	,shopll_conv
	,shpran8
	,shpran8_conv
	,shoppid
	,shoppid_conv
	,shsdattn
	,shsdattn_conv
	,shspattn
	,shspattn_conv
	,shotind
	,shotind_conv
	,shprcidln
	,shprcidln_conv
	,shccidln
	,shccidln_conv
	,shshccidln
	,shshccidln_conv
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
	,cdc.shkcoo
	,cdc.shkcoo_conv
	,cdc.shdoco
	,cdc.shdcto
	,cdc.shsfxo
	,cdc.shsfxo_conv
	,cdc.shmcu
	,cdc.shmcu_conv
	,cdc.shco
	,cdc.shco_conv
	,cdc.shokco
	,cdc.shokco_conv
	,cdc.shoorn
	,cdc.shoorn_conv
	,cdc.shocto
	,cdc.shrkco
	,cdc.shrkco_conv
	,cdc.shrorn
	,cdc.shrorn_conv
	,cdc.shrcto
	,cdc.shan8
	,cdc.shshan
	,cdc.shpa8
	,cdc.shdrqj
	,cdc.shdrqj_conv
	,cdc.shtrdj
	,cdc.shtrdj_conv
	,cdc.shpddj
	,cdc.shpddj_conv
	,cdc.shopdj
	,cdc.shopdj_conv
	,cdc.shaddj
	,cdc.shaddj_conv
	,cdc.shcndj
	,cdc.shcndj_conv
	,cdc.shpefj
	,cdc.shpefj_conv
	,cdc.shppdj
	,cdc.shppdj_conv
	,cdc.shvr01
	,cdc.shvr01_conv
	,cdc.shvr02
	,cdc.shvr02_conv
	,cdc.shdel1
	,cdc.shdel1_conv
	,cdc.shdel2
	,cdc.shdel2_conv
	,cdc.shinmg
	,cdc.shptc
	,cdc.shptc_conv
	,cdc.shryin
	,cdc.shasn
	,cdc.shprgp
	,cdc.shtrdc
	,cdc.shtrdc_conv
	,cdc.shpcrt
	,cdc.shpcrt_conv
	,cdc.shtxa1
	,cdc.shtxa1_conv
	,cdc.shexr1
	,cdc.shtxct
	,cdc.shtxct_conv
	,cdc.shatxt
	,cdc.shatxt_conv
	,cdc.shprio
	,cdc.shback
	,cdc.shback_conv
	,cdc.shsbal
	,cdc.shsbal_conv
	,cdc.shhold
	,cdc.shplst
	,cdc.shplst_conv
	,cdc.shinvc
	,cdc.shinvc_conv
	,cdc.shntr
	,cdc.shanby
	,cdc.shcars
	,cdc.shcars_conv
	,cdc.shmot
	,cdc.shcot
	,cdc.shrout
	,cdc.shstop
	,cdc.shzon
	,cdc.shcnid
	,cdc.shcnid_conv
	,cdc.shfrth
	,cdc.shaft
	,cdc.shaft_conv
	,cdc.shfuf1
	,cdc.shfrtc
	,cdc.shfrtc_conv
	,cdc.shmord
	,cdc.shmord_conv
	,cdc.shrcd
	,cdc.shfuf2
	,cdc.shfuf2_conv
	,cdc.shotot
	,cdc.shotot_conv
	,cdc.shtotc
	,cdc.shtotc_conv
	,cdc.shwumd
	,cdc.shvumd
	,cdc.shautn
	,cdc.shautn_conv
	,cdc.shcact
	,cdc.shcact_conv
	,cdc.shcexp
	,cdc.shcexp_conv
	,cdc.shsbli
	,cdc.shcrmd
	,cdc.shcrrm
	,cdc.shcrcd
	,cdc.shcrcd_conv
	,cdc.shcrr
	,cdc.shlngp
	,cdc.shfap
	,cdc.shfap_conv
	,cdc.shfcst
	,cdc.shfcst_conv
	,cdc.shorby
	,cdc.shorby_conv
	,cdc.shtkby
	,cdc.shtkby_conv
	,cdc.shurcd
	,cdc.shurcd_conv
	,cdc.shurdt
	,cdc.shurdt_conv
	,cdc.shurat
	,cdc.shurat_conv
	,cdc.shurab
	,cdc.shurab_conv
	,cdc.shurrf
	,cdc.shurrf_conv
	,cdc.shuser
	,cdc.shuser_conv
	,cdc.shpid
	,cdc.shpid_conv
	,cdc.shjobn
	,cdc.shjobn_conv
	,cdc.shupmj
	,cdc.shupmj_conv
	,cdc.shtday
	,cdc.shir01
	,cdc.shir01_conv
	,cdc.shir02
	,cdc.shir02_conv
	,cdc.shir03
	,cdc.shir03_conv
	,cdc.shir04
	,cdc.shir04_conv
	,cdc.shir05
	,cdc.shir05_conv
	,cdc.shvr03
	,cdc.shvr03_conv
	,cdc.shsoor
	,cdc.shsoor_conv
	,cdc.shpmdt
	,cdc.shrsdt
	,cdc.shrqsj
	,cdc.shrqsj_conv
	,cdc.shpstm
	,cdc.shpdtt
	,cdc.shoptt
	,cdc.shdrqt
	,cdc.shadtm
	,cdc.shadlj
	,cdc.shadlj_conv
	,cdc.shpban
	,cdc.shpban_conv
	,cdc.shitan
	,cdc.shitan_conv
	,cdc.shftan
	,cdc.shftan_conv
	,cdc.shdvan
	,cdc.shdvan_conv
	,cdc.shdoc1
	,cdc.shdct4
	,cdc.shcord
	,cdc.shbsc
	,cdc.shbcrc
	,cdc.shbcrc_conv
	,cdc.shauft
	,cdc.shaufi
	,cdc.shopbo
	,cdc.shoptc
	,cdc.shoptc_conv
	,cdc.shopld
	,cdc.shopld_conv
	,cdc.shopbk
	,cdc.shopbk_conv
	,cdc.shopsb
	,cdc.shopsb_conv
	,cdc.shopps
	,cdc.shopps_conv
	,cdc.shoppl
	,cdc.shoppl_conv
	,cdc.shopms
	,cdc.shopms_conv
	,cdc.shopss
	,cdc.shopss_conv
	,cdc.shopba
	,cdc.shopba_conv
	,cdc.shopll
	,cdc.shopll_conv
	,cdc.shpran8
	,cdc.shpran8_conv
	,cdc.shoppid
	,cdc.shoppid_conv
	,cdc.shsdattn
	,cdc.shsdattn_conv
	,cdc.shspattn
	,cdc.shspattn_conv
	,cdc.shotind
	,cdc.shotind_conv
	,cdc.shprcidln
	,cdc.shprcidln_conv
	,cdc.shccidln
	,cdc.shccidln_conv
	,cdc.shshccidln
	,cdc.shshccidln_conv
FROM stg_jde.tmp_upsert_f4201_cdc cdc
LEFT OUTER JOIN rep_jde.f4201_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f4201_new AF:{{ task_instance_key_str }}' ) 