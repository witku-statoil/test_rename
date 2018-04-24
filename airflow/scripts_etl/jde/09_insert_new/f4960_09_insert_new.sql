-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f4960_new
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
	,tmvmcu
	,tmvmcu_conv
	,tmldnm
	,tmnmcu
	,tmnmcu_conv
	,tmorgn
	,tmmcux
	,tmmcux_conv
	,tmancc
	,tmanid
	,tmovod
	,tmcty1
	,tmcty1_conv
	,tmadds
	,tmaddz
	,tmaddz_conv
	,tmctr
	,tmzon
	,tmldty
	,tmldls
	,tmload
	,tmload_conv
	,tmshft
	,tmseq
	,tmtmls
	,tmppdj
	,tmppdj_conv
	,tmpmdt
	,tmaddj
	,tmaddj_conv
	,tmadtm
	,tmmot
	,tmovrm
	,tmovrm_conv
	,tmvtyp
	,tmvtyp_conv
	,tmpveh
	,tmpveh_conv
	,tmrlno
	,tmrlno_conv
	,tmdumv
	,tmdumv_conv
	,tmcnnv
	,tmcnnv_conv
	,tmcars
	,tmcars_conv
	,tmovrc
	,tmovrc_conv
	,tmrout
	,tmrtn
	,tmfrsc
	,tmczon
	,tmdsgp
	,tmdaty
	,tmdscd
	,tmldle
	,tmldle_conv
	,tmseal
	,tmseal_conv
	,tmlmcu
	,tmlmcu_conv
	,tmltrp
	,tmdstn
	,tmumd1
	,tmdsrc
	,tmodst
	,tmeltm
	,tmum
	,tmcvum
	,tmwtum
	,tmvlum
	,tmvr01
	,tmvr01_conv
	,tmnstp
	,tmnstp_conv
	,tmrrtr
	,tmratr
	,tmfrvc
	,tmfrvc_conv
	,tmfrvf
	,tmfrvf_conv
	,tmcrcp
	,tmcrcp_conv
	,tmibrs
	,tmibrs_conv
	,tmdkid
	,tmdkid_conv
	,tmdepu
	,tmdepu_conv
	,tmdlpu
	,tmdlpu_conv
	,tmtpuf
	,tmtput
	,tmlslt
	,tmlsut
	,tmlalt
	,tmlaut
	,tmurcd
	,tmurcd_conv
	,tmurdt
	,tmurdt_conv
	,tmurat
	,tmurat_conv
	,tmurab
	,tmurab_conv
	,tmurrf
	,tmurrf_conv
	,tmuser
	,tmuser_conv
	,tmpid
	,tmpid_conv
	,tmjobn
	,tmjobn_conv
	,tmupmj
	,tmupmj_conv
	,tmtday
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
	,cdc.tmvmcu
	,cdc.tmvmcu_conv
	,cdc.tmldnm
	,cdc.tmnmcu
	,cdc.tmnmcu_conv
	,cdc.tmorgn
	,cdc.tmmcux
	,cdc.tmmcux_conv
	,cdc.tmancc
	,cdc.tmanid
	,cdc.tmovod
	,cdc.tmcty1
	,cdc.tmcty1_conv
	,cdc.tmadds
	,cdc.tmaddz
	,cdc.tmaddz_conv
	,cdc.tmctr
	,cdc.tmzon
	,cdc.tmldty
	,cdc.tmldls
	,cdc.tmload
	,cdc.tmload_conv
	,cdc.tmshft
	,cdc.tmseq
	,cdc.tmtmls
	,cdc.tmppdj
	,cdc.tmppdj_conv
	,cdc.tmpmdt
	,cdc.tmaddj
	,cdc.tmaddj_conv
	,cdc.tmadtm
	,cdc.tmmot
	,cdc.tmovrm
	,cdc.tmovrm_conv
	,cdc.tmvtyp
	,cdc.tmvtyp_conv
	,cdc.tmpveh
	,cdc.tmpveh_conv
	,cdc.tmrlno
	,cdc.tmrlno_conv
	,cdc.tmdumv
	,cdc.tmdumv_conv
	,cdc.tmcnnv
	,cdc.tmcnnv_conv
	,cdc.tmcars
	,cdc.tmcars_conv
	,cdc.tmovrc
	,cdc.tmovrc_conv
	,cdc.tmrout
	,cdc.tmrtn
	,cdc.tmfrsc
	,cdc.tmczon
	,cdc.tmdsgp
	,cdc.tmdaty
	,cdc.tmdscd
	,cdc.tmldle
	,cdc.tmldle_conv
	,cdc.tmseal
	,cdc.tmseal_conv
	,cdc.tmlmcu
	,cdc.tmlmcu_conv
	,cdc.tmltrp
	,cdc.tmdstn
	,cdc.tmumd1
	,cdc.tmdsrc
	,cdc.tmodst
	,cdc.tmeltm
	,cdc.tmum
	,cdc.tmcvum
	,cdc.tmwtum
	,cdc.tmvlum
	,cdc.tmvr01
	,cdc.tmvr01_conv
	,cdc.tmnstp
	,cdc.tmnstp_conv
	,cdc.tmrrtr
	,cdc.tmratr
	,cdc.tmfrvc
	,cdc.tmfrvc_conv
	,cdc.tmfrvf
	,cdc.tmfrvf_conv
	,cdc.tmcrcp
	,cdc.tmcrcp_conv
	,cdc.tmibrs
	,cdc.tmibrs_conv
	,cdc.tmdkid
	,cdc.tmdkid_conv
	,cdc.tmdepu
	,cdc.tmdepu_conv
	,cdc.tmdlpu
	,cdc.tmdlpu_conv
	,cdc.tmtpuf
	,cdc.tmtput
	,cdc.tmlslt
	,cdc.tmlsut
	,cdc.tmlalt
	,cdc.tmlaut
	,cdc.tmurcd
	,cdc.tmurcd_conv
	,cdc.tmurdt
	,cdc.tmurdt_conv
	,cdc.tmurat
	,cdc.tmurat_conv
	,cdc.tmurab
	,cdc.tmurab_conv
	,cdc.tmurrf
	,cdc.tmurrf_conv
	,cdc.tmuser
	,cdc.tmuser_conv
	,cdc.tmpid
	,cdc.tmpid_conv
	,cdc.tmjobn
	,cdc.tmjobn_conv
	,cdc.tmupmj
	,cdc.tmupmj_conv
	,cdc.tmtday
FROM stg_jde.tmp_upsert_f4960_cdc cdc
LEFT OUTER JOIN rep_jde.f4960_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f4960_new AF:{{ task_instance_key_str }}' ) 