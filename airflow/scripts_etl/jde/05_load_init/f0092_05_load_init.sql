-- Create rep new table for init
IF OBJECT_ID('rep_jde.f0092_new') IS NOT NULL
    DROP TABLE rep_jde.f0092_new


CREATE TABLE rep_jde.f0092_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    	cast(sys_file_name as [NVARCHAR](100)) as sys_file_name
	,cast(sys_file_ln as [INT]) as sys_file_ln
	,cast(sys_integration_id as [NVARCHAR](200)) as sys_integration_id
	,cast(sys_extract_dt as [NVARCHAR](40)) as sys_extract_dt
	,cast(sys_cdc_dt as [NVARCHAR](40)) as sys_cdc_dt
	,cast(sys_cdc_scn as [NVARCHAR](14)) as sys_cdc_scn
	,cast(sys_cdc_operation_type as [NVARCHAR](15)) as sys_cdc_operation_type
	,cast(sys_cdc_before_after as [NVARCHAR](10)) as sys_cdc_before_after
	,cast(sys_line_modified_ind as [INT]) as sys_line_modified_ind
	,uluser as uluser
	,ltrim(rtrim(uluser)) as uluser_conv
	,ulmni as ulmni
	,ltrim(rtrim(ulmni)) as ulmni_conv
	,uloutq as uloutq
	,ltrim(rtrim(uloutq)) as uloutq_conv
	,uljobq as uljobq
	,ltrim(rtrim(uljobq)) as uljobq_conv
	,uljsp as uljsp
	,ltrim(rtrim(uljsp)) as uljsp_conv
	,uloutp as uloutp
	,ltrim(rtrim(uloutp)) as uloutp_conv
	,cast(ulllvl as [NVARCHAR](1)) as ulllvl
	,ullsev as ullsev
	,ltrim(rtrim(ullsev)) as ullsev_conv
	,cast(ullmsg as [NVARCHAR](7)) as ullmsg
	,ulprtl as ulprtl
	,ltrim(rtrim(ulprtl)) as ulprtl_conv
	,cast(ulul01 as [NVARCHAR](10)) as ulul01
	,cast(ulul02 as [NVARCHAR](10)) as ulul02
	,ulul03 as ulul03
	,ltrim(rtrim(ulul03)) as ulul03_conv
	,ulul04 as ulul04
	,ltrim(rtrim(ulul04)) as ulul04_conv
	,ulul05 as ulul05
	,ltrim(rtrim(ulul05)) as ulul05_conv
	,ulul06 as ulul06
	,ltrim(rtrim(ulul06)) as ulul06_conv
	,ulul07 as ulul07
	,ltrim(rtrim(ulul07)) as ulul07_conv
	,ulul08 as ulul08
	,ltrim(rtrim(ulul08)) as ulul08_conv
	,ulul09 as ulul09
	,ltrim(rtrim(ulul09)) as ulul09_conv
	,ulul10 as ulul10
	,ltrim(rtrim(ulul10)) as ulul10_conv
	,ulul11 as ulul11
	,ltrim(rtrim(ulul11)) as ulul11_conv
	,ulul12 as ulul12
	,ltrim(rtrim(ulul12)) as ulul12_conv
	,ulul13 as ulul13
	,ltrim(rtrim(ulul13)) as ulul13_conv
	,ulul14 as ulul14
	,ltrim(rtrim(ulul14)) as ulul14_conv
	,ulul15 as ulul15
	,ltrim(rtrim(ulul15)) as ulul15_conv
	,ulul16 as ulul16
	,ltrim(rtrim(ulul16)) as ulul16_conv
	,ulul17 as ulul17
	,ltrim(rtrim(ulul17)) as ulul17_conv
	,ulul18 as ulul18
	,ltrim(rtrim(ulul18)) as ulul18_conv
	,ulul19 as ulul19
	,ltrim(rtrim(ulul19)) as ulul19_conv
	,ulul20 as ulul20
	,ltrim(rtrim(ulul20)) as ulul20_conv
	,ulul21 as ulul21
	,ltrim(rtrim(ulul21)) as ulul21_conv
	,ulul22 as ulul22
	,ltrim(rtrim(ulul22)) as ulul22_conv
	,ulul23 as ulul23
	,ltrim(rtrim(ulul23)) as ulul23_conv
	,ulul24 as ulul24
	,ltrim(rtrim(ulul24)) as ulul24_conv
	,ulul25 as ulul25
	,ltrim(rtrim(ulul25)) as ulul25_conv
	,ulmska as ulmska
	,ltrim(rtrim(ulmska)) as ulmska_conv
	,ulmskj as ulmskj
	,ltrim(rtrim(ulmskj)) as ulmskj_conv
	,ulmskk as ulmskk
	,ltrim(rtrim(ulmskk)) as ulmskk_conv
	,ulmskd as ulmskd
	,ltrim(rtrim(ulmskd)) as ulmskd_conv
	,cast(ulmskf as [NVARCHAR](1)) as ulmskf
	,cast(ulan8 as [DECIMAL](38, 4)) as ulan8
	,ulutyp as ulutyp
	,ltrim(rtrim(ulutyp)) as ulutyp_conv
	,ulpid as ulpid
	,ltrim(rtrim(ulpid)) as ulpid_conv
	,ulmtvl as ulmtvl
	,ltrim(rtrim(ulmtvl)) as ulmtvl_conv
	,ulcmde as ulcmde
	,ltrim(rtrim(ulcmde)) as ulcmde_conv
	,ulclib as ulclib
	,ltrim(rtrim(ulclib)) as ulclib_conv
	,ulugrp as ulugrp
	,ltrim(rtrim(ulugrp)) as ulugrp_conv
	,ulipgm as ulipgm
	,ltrim(rtrim(ulipgm)) as ulipgm_conv
	,cast(ullod as [NVARCHAR](1)) as ullod
	,ulfstp as ulfstp
	,ltrim(rtrim(ulfstp)) as ulfstp_conv
FROM 
    stg_jde.tmp_f0092_init
OPTION ( LABEL = 'CREATE_rep_jde.f0092_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f0092_sys_integration_id ON rep_jde.f0092_new(sys_integration_id); 
