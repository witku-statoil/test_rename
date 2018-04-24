-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f0401_new
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
	,a6an8
	,a6apc
	,a6apc_conv
	,a6mcup
	,a6mcup_conv
	,a6obap
	,a6obap_conv
	,a6aidp
	,a6aidp_conv
	,a6kcop
	,a6kcop_conv
	,a6dcap
	,a6dtap
	,a6crrp
	,a6crrp_conv
	,a6txa2
	,a6txa2_conv
	,a6exr2
	,a6hdpy
	,a6txa3
	,a6txa3_conv
	,a6exr3
	,a6tawh
	,a6pcwh
	,a6trap
	,a6trap_conv
	,a6sck
	,a6pyin
	,a6snto
	,a6ab1
	,a6fld
	,a6sqnl
	,a6crca
	,a6crca_conv
	,a6aypd
	,a6aypd_conv
	,a6appd
	,a6appd_conv
	,a6abam
	,a6abam_conv
	,a6aba1
	,a6aba1_conv
	,a6aprc
	,a6aprc_conv
	,a6mino
	,a6mino_conv
	,a6maxo
	,a6maxo_conv
	,a6an8r
	,a6badt
	,a6cpgp
	,a6ortp
	,a6inmg
	,a6hold
	,a6rout
	,a6stop
	,a6zon
	,a6ancr
	,a6cars
	,a6cars_conv
	,a6del1
	,a6del1_conv
	,a6del2
	,a6del2_conv
	,a6ltdt
	,a6ltdt_conv
	,a6frth
	,a6invc
	,a6invc_conv
	,a6plst
	,a6plst_conv
	,a6wumd
	,a6vumd
	,a6prp5
	,a6edpm
	,a6edci
	,a6edii
	,a6edqd
	,a6edad
	,a6edf1
	,a6edf1_conv
	,a6edf2
	,a6vi01
	,a6vi02
	,a6vi03
	,a6vi03_conv
	,a6vi04
	,a6vi04_conv
	,a6vi05
	,a6vi05_conv
	,a6mnsc
	,a6ato
	,a6rvnt
	,a6rvnt_conv
	,a6urcd
	,a6urcd_conv
	,a6urdt
	,a6urdt_conv
	,a6urat
	,a6urat_conv
	,a6urab
	,a6urab_conv
	,a6urrf
	,a6urrf_conv
	,a6user
	,a6user_conv
	,a6pid
	,a6pid_conv
	,a6jobn
	,a6jobn_conv
	,a6upmj
	,a6upmj_conv
	,a6upmt
	,a6asn
	,a6crmd
	,a6avch
	,a6atrl
	,a6atrl_conv
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
	,cdc.a6an8
	,cdc.a6apc
	,cdc.a6apc_conv
	,cdc.a6mcup
	,cdc.a6mcup_conv
	,cdc.a6obap
	,cdc.a6obap_conv
	,cdc.a6aidp
	,cdc.a6aidp_conv
	,cdc.a6kcop
	,cdc.a6kcop_conv
	,cdc.a6dcap
	,cdc.a6dtap
	,cdc.a6crrp
	,cdc.a6crrp_conv
	,cdc.a6txa2
	,cdc.a6txa2_conv
	,cdc.a6exr2
	,cdc.a6hdpy
	,cdc.a6txa3
	,cdc.a6txa3_conv
	,cdc.a6exr3
	,cdc.a6tawh
	,cdc.a6pcwh
	,cdc.a6trap
	,cdc.a6trap_conv
	,cdc.a6sck
	,cdc.a6pyin
	,cdc.a6snto
	,cdc.a6ab1
	,cdc.a6fld
	,cdc.a6sqnl
	,cdc.a6crca
	,cdc.a6crca_conv
	,cdc.a6aypd
	,cdc.a6aypd_conv
	,cdc.a6appd
	,cdc.a6appd_conv
	,cdc.a6abam
	,cdc.a6abam_conv
	,cdc.a6aba1
	,cdc.a6aba1_conv
	,cdc.a6aprc
	,cdc.a6aprc_conv
	,cdc.a6mino
	,cdc.a6mino_conv
	,cdc.a6maxo
	,cdc.a6maxo_conv
	,cdc.a6an8r
	,cdc.a6badt
	,cdc.a6cpgp
	,cdc.a6ortp
	,cdc.a6inmg
	,cdc.a6hold
	,cdc.a6rout
	,cdc.a6stop
	,cdc.a6zon
	,cdc.a6ancr
	,cdc.a6cars
	,cdc.a6cars_conv
	,cdc.a6del1
	,cdc.a6del1_conv
	,cdc.a6del2
	,cdc.a6del2_conv
	,cdc.a6ltdt
	,cdc.a6ltdt_conv
	,cdc.a6frth
	,cdc.a6invc
	,cdc.a6invc_conv
	,cdc.a6plst
	,cdc.a6plst_conv
	,cdc.a6wumd
	,cdc.a6vumd
	,cdc.a6prp5
	,cdc.a6edpm
	,cdc.a6edci
	,cdc.a6edii
	,cdc.a6edqd
	,cdc.a6edad
	,cdc.a6edf1
	,cdc.a6edf1_conv
	,cdc.a6edf2
	,cdc.a6vi01
	,cdc.a6vi02
	,cdc.a6vi03
	,cdc.a6vi03_conv
	,cdc.a6vi04
	,cdc.a6vi04_conv
	,cdc.a6vi05
	,cdc.a6vi05_conv
	,cdc.a6mnsc
	,cdc.a6ato
	,cdc.a6rvnt
	,cdc.a6rvnt_conv
	,cdc.a6urcd
	,cdc.a6urcd_conv
	,cdc.a6urdt
	,cdc.a6urdt_conv
	,cdc.a6urat
	,cdc.a6urat_conv
	,cdc.a6urab
	,cdc.a6urab_conv
	,cdc.a6urrf
	,cdc.a6urrf_conv
	,cdc.a6user
	,cdc.a6user_conv
	,cdc.a6pid
	,cdc.a6pid_conv
	,cdc.a6jobn
	,cdc.a6jobn_conv
	,cdc.a6upmj
	,cdc.a6upmj_conv
	,cdc.a6upmt
	,cdc.a6asn
	,cdc.a6crmd
	,cdc.a6avch
	,cdc.a6atrl
	,cdc.a6atrl_conv
FROM stg_jde.tmp_upsert_f0401_cdc cdc
LEFT OUTER JOIN rep_jde.f0401_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f0401_new AF:{{ task_instance_key_str }}' ) 