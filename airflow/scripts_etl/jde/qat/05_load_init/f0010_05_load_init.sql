-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f0010_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f0010_new


CREATE TABLE rep_jde_qat.f0010_new
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
	,ccco as ccco
	,ltrim(rtrim(ccco)) as ccco_conv
	,ccname as ccname
	,ltrim(rtrim(ccname)) as ccname_conv
	,ccaltc as ccaltc
	,ltrim(rtrim(ccaltc)) as ccaltc_conv
	,cast(ccdfyj as [INT]) as ccdfyj
	,case when cast(ccdfyj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ccdfyj as [INT]) %1000, dateadd(year, cast(ccdfyj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ccdfyj_conv
	,cast(ccpnc as [DECIMAL](38, 4)) as ccpnc
	,ccdot1 as ccdot1
	,ltrim(rtrim(ccdot1)) as ccdot1_conv
	,cast(cccryr as [NVARCHAR](1)) as cccryr
	,cccryc as cccryc
	,ltrim(rtrim(cccryc)) as cccryc_conv
	,ccdot2 as ccdot2
	,ltrim(rtrim(ccdot2)) as ccdot2_conv
	,ccmcua as ccmcua
	,ltrim(rtrim(ccmcua)) as ccmcua_conv
	,ccmcud as ccmcud
	,ltrim(rtrim(ccmcud)) as ccmcud_conv
	,ccmcuc as ccmcuc
	,ltrim(rtrim(ccmcuc)) as ccmcuc_conv
	,ccmcub as ccmcub
	,ltrim(rtrim(ccmcub)) as ccmcub_conv
	,ccdprc as ccdprc
	,ltrim(rtrim(ccdprc)) as ccdprc_conv
	,ccbktx as ccbktx
	,ltrim(rtrim(ccbktx)) as ccbktx_conv
	,cast(cctxbm as [DECIMAL](38, 4)) as cctxbm
	,cast(cctxbo as [DECIMAL](38, 4)) as cctxbo
	,cast(ccnwxj as [INT]) as ccnwxj
	,case when cast(ccnwxj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ccnwxj as [INT]) %1000, dateadd(year, cast(ccnwxj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ccnwxj_conv
	,cast(ccglie as [NVARCHAR](1)) as ccglie
	,ccabin as ccabin
	,ltrim(rtrim(ccabin)) as ccabin_conv
	,cast(cccald as [NVARCHAR](2)) as cccald
	,cast(ccdtpn as [NVARCHAR](1)) as ccdtpn
	,cast(ccpnf as [DECIMAL](38, 4)) as ccpnf
	,cast(ccdff as [DECIMAL](38, 4)) as ccdff
	,cccrcd as cccrcd
	,ltrim(rtrim(cccrcd)) as cccrcd_conv
	,ccsmi as ccsmi
	,ltrim(rtrim(ccsmi)) as ccsmi_conv
	,ccsmu as ccsmu
	,ltrim(rtrim(ccsmu)) as ccsmu_conv
	,ccsms as ccsms
	,ltrim(rtrim(ccsms)) as ccsms_conv
	,ccdnlt as ccdnlt
	,ltrim(rtrim(ccdnlt)) as ccdnlt_conv
	,cast(ccstmt as [NVARCHAR](1)) as ccstmt
	,ccatcs as ccatcs
	,ltrim(rtrim(ccatcs)) as ccatcs_conv
	,cast(ccalgm as [NVARCHAR](2)) as ccalgm
	,cast(ccagem as [NVARCHAR](1)) as ccagem
	,cast(cccrdy as [DECIMAL](38, 4)) as cccrdy
	,cast(ccagr1 as [DECIMAL](38, 4)) as ccagr1
	,cast(ccagr2 as [DECIMAL](38, 4)) as ccagr2
	,cast(ccagr3 as [DECIMAL](38, 4)) as ccagr3
	,cast(ccagr4 as [DECIMAL](38, 4)) as ccagr4
	,cast(ccagr5 as [DECIMAL](38, 4)) as ccagr5
	,cast(ccagr6 as [DECIMAL](38, 4)) as ccagr6
	,cast(ccagr7 as [DECIMAL](38, 4)) as ccagr7
	,cast(ccfry as [DECIMAL](38, 4)) as ccfry
	,cast(ccfrp as [DECIMAL](38, 4)) as ccfrp
	,cast(ccnnp as [DECIMAL](38, 4)) as ccnnp
	,cast(ccfp as [DECIMAL](38, 4)) as ccfp
	,cast(ccfp as [DECIMAL](38, 6)) / cast(1000000 as [DECIMAL](38, 6)) as ccfp_conv
	,cast(ccage as [NVARCHAR](1)) as ccage
	,cast(ccdag as [INT]) as ccdag
	,case when cast(ccdag as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ccdag as [INT]) %1000, dateadd(year, cast(ccdag as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ccdag_conv
	,cast(ccarpn as [DECIMAL](38, 4)) as ccarpn
	,cast(ccappn as [DECIMAL](38, 4)) as ccappn
	,cast(ccarfj as [INT]) as ccarfj
	,case when cast(ccarfj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ccarfj as [INT]) %1000, dateadd(year, cast(ccarfj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ccarfj_conv
	,cast(ccapfj as [INT]) as ccapfj
	,case when cast(ccapfj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ccapfj as [INT]) %1000, dateadd(year, cast(ccapfj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ccapfj_conv
	,cast(ccan8 as [DECIMAL](38, 4)) as ccan8
	,ccsgbk as ccsgbk
	,ltrim(rtrim(ccsgbk)) as ccsgbk_conv
	,ccptco as ccptco
	,ltrim(rtrim(ccptco)) as ccptco_conv
	,ccx1 as ccx1
	,ltrim(rtrim(ccx1)) as ccx1_conv
	,ccx2 as ccx2
	,ltrim(rtrim(ccx2)) as ccx2_conv
	,ccuser as ccuser
	,ltrim(rtrim(ccuser)) as ccuser_conv
	,ccpid as ccpid
	,ltrim(rtrim(ccpid)) as ccpid_conv
	,cast(ccupmj as [INT]) as ccupmj
	,case when cast(ccupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ccupmj as [INT]) %1000, dateadd(year, cast(ccupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ccupmj_conv
	,ccjobn as ccjobn
	,ltrim(rtrim(ccjobn)) as ccjobn_conv
	,cast(ccupmt as [DECIMAL](38, 4)) as ccupmt
	,cast(cctsid as [DECIMAL](38, 4)) as cctsid
	,cctsco as cctsco
	,ltrim(rtrim(cctsco)) as cctsco_conv
	,cast(ccthco as [NVARCHAR](3)) as ccthco
	,cast(ccan8c as [DECIMAL](38, 4)) as ccan8c
FROM 
    stg_jde_qat.tmp_f0010_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f0010_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f0010_sys_integration_id ON rep_jde_qat.f0010_new(sys_integration_id); 
