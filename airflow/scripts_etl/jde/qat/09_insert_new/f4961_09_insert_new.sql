-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f4961_new
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
	,llvmcu
	,llvmcu_conv
	,llldnm
	,lltrpl
	,lltrpl_conv
	,llorgn
	,llnmcu
	,llnmcu_conv
	,llstsq
	,llscwt
	,llscwt_conv
	,llwtum
	,llscvl
	,llscvl_conv
	,llvlum
	,llsccu
	,llsccu_conv
	,llcvum
	,lllddt
	,lllddt_conv
	,llldtm
	,llload
	,llload_conv
	,lltmls
	,llppdj
	,llppdj_conv
	,llpmdt
	,lladdj
	,lladdj_conv
	,lladtm
	,lllrfg
	,lllrfg_conv
	,llpcsd
	,llpcsd_conv
	,lldwnc
	,llurcd
	,llurcd_conv
	,llurdt
	,llurdt_conv
	,llurat
	,llurat_conv
	,llurab
	,llurab_conv
	,llurrf
	,llurrf_conv
	,lluser
	,lluser_conv
	,llpid
	,llpid_conv
	,lljobn
	,lljobn_conv
	,llupmj
	,llupmj_conv
	,lltday
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
	,cdc.llvmcu
	,cdc.llvmcu_conv
	,cdc.llldnm
	,cdc.lltrpl
	,cdc.lltrpl_conv
	,cdc.llorgn
	,cdc.llnmcu
	,cdc.llnmcu_conv
	,cdc.llstsq
	,cdc.llscwt
	,cdc.llscwt_conv
	,cdc.llwtum
	,cdc.llscvl
	,cdc.llscvl_conv
	,cdc.llvlum
	,cdc.llsccu
	,cdc.llsccu_conv
	,cdc.llcvum
	,cdc.lllddt
	,cdc.lllddt_conv
	,cdc.llldtm
	,cdc.llload
	,cdc.llload_conv
	,cdc.lltmls
	,cdc.llppdj
	,cdc.llppdj_conv
	,cdc.llpmdt
	,cdc.lladdj
	,cdc.lladdj_conv
	,cdc.lladtm
	,cdc.lllrfg
	,cdc.lllrfg_conv
	,cdc.llpcsd
	,cdc.llpcsd_conv
	,cdc.lldwnc
	,cdc.llurcd
	,cdc.llurcd_conv
	,cdc.llurdt
	,cdc.llurdt_conv
	,cdc.llurat
	,cdc.llurat_conv
	,cdc.llurab
	,cdc.llurab_conv
	,cdc.llurrf
	,cdc.llurrf_conv
	,cdc.lluser
	,cdc.lluser_conv
	,cdc.llpid
	,cdc.llpid_conv
	,cdc.lljobn
	,cdc.lljobn_conv
	,cdc.llupmj
	,cdc.llupmj_conv
	,cdc.lltday
FROM stg_jde_qat.tmp_upsert_f4961_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f4961_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f4961_new AF:{{ task_instance_key_str }}' ) 