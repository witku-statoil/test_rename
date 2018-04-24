-- Create rep new table for init
IF OBJECT_ID('rep_jde.f1207_new') IS NOT NULL
    DROP TABLE rep_jde.f1207_new


CREATE TABLE rep_jde.f1207_new
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
	,cast(fwnumb as [DECIMAL](38, 4)) as fwnumb
	,fwapid as fwapid
	,ltrim(rtrim(fwapid)) as fwapid_conv
	,fwasid as fwasid
	,ltrim(rtrim(fwasid)) as fwasid_conv
	,cast(fwsrvt as [NVARCHAR](8)) as fwsrvt
	,cast(fwmsts as [NVARCHAR](2)) as fwmsts
	,cast(fwmpri as [NVARCHAR](1)) as fwmpri
	,fwky as fwky
	,ltrim(rtrim(fwky)) as fwky_conv
	,cast(fwanp as [DECIMAL](38, 4)) as fwanp
	,fwrmk as fwrmk
	,ltrim(rtrim(fwrmk)) as fwrmk_conv
	,cast(fwsrvd as [DECIMAL](38, 4)) as fwsrvd
	,cast(fwsrvd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fwsrvd_conv
	,cast(fwsrvm as [DECIMAL](38, 4)) as fwsrvm
	,cast(fwsrvm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwsrvm_conv
	,cast(fwsrvh as [DECIMAL](38, 4)) as fwsrvh
	,cast(fwsrvh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwsrvh_conv
	,cast(fwtdt as [INT]) as fwtdt
	,case when cast(fwtdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fwtdt as [INT]) %1000, dateadd(year, cast(fwtdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fwtdt_conv
	,cast(fwcplm as [DECIMAL](38, 4)) as fwcplm
	,cast(fwcplm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwcplm_conv
	,cast(fwcplh as [DECIMAL](38, 4)) as fwcplh
	,cast(fwcplh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwcplh_conv
	,cast(fwcpld as [INT]) as fwcpld
	,case when cast(fwcpld as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fwcpld as [INT]) %1000, dateadd(year, cast(fwcpld as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fwcpld_conv
	,cast(fwlstm as [DECIMAL](38, 4)) as fwlstm
	,cast(fwlstm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwlstm_conv
	,cast(fwlsth as [DECIMAL](38, 4)) as fwlsth
	,cast(fwlsth as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwlsth_conv
	,cast(fwlcpd as [INT]) as fwlcpd
	,case when cast(fwlcpd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fwlcpd as [INT]) %1000, dateadd(year, cast(fwlcpd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fwlcpd_conv
	,fwuser as fwuser
	,ltrim(rtrim(fwuser)) as fwuser_conv
	,fwpid as fwpid
	,ltrim(rtrim(fwpid)) as fwpid_conv
	,cast(fwupmj as [INT]) as fwupmj
	,case when cast(fwupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fwupmj as [INT]) %1000, dateadd(year, cast(fwupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fwupmj_conv
	,fwjobn as fwjobn
	,ltrim(rtrim(fwjobn)) as fwjobn_conv
	,cast(fwdoco as [DECIMAL](38, 4)) as fwdoco
	,cast(fwwona as [DECIMAL](38, 4)) as fwwona
	,cast(fwmpc as [DECIMAL](38, 4)) as fwmpc
	,cast(fwsrvf as [DECIMAL](38, 4)) as fwsrvf
	,cast(fwsrvf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwsrvf_conv
	,cast(fwcplf as [DECIMAL](38, 4)) as fwcplf
	,cast(fwcplf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwcplf_conv
	,cast(fwlstf as [DECIMAL](38, 4)) as fwlstf
	,cast(fwlstf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwlstf_conv
	,cast(fwmltw as [NVARCHAR](1)) as fwmltw
	,cast(fworgm as [DECIMAL](38, 4)) as fworgm
	,cast(fworgm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fworgm_conv
	,cast(fworgh as [DECIMAL](38, 4)) as fworgh
	,cast(fworgh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fworgh_conv
	,cast(fworgf as [DECIMAL](38, 4)) as fworgf
	,cast(fworgf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fworgf_conv
	,cast(fwoccu as [DECIMAL](38, 4)) as fwoccu
	,cast(fwoccu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwoccu_conv
	,cast(fwfrin as [NVARCHAR](1)) as fwfrin
	,cast(fwupmt as [DECIMAL](38, 4)) as fwupmt
	,fwmcu as fwmcu
	,ltrim(rtrim(fwmcu)) as fwmcu_conv
	,cast(fwaaid as [DECIMAL](38, 4)) as fwaaid
	,cast(fwcrtl as [DECIMAL](38, 4)) as fwcrtl
	,cast(fwcrtl as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fwcrtl_conv
	,cast(fwpnst as [INT]) as fwpnst
	,case when cast(fwpnst as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fwpnst as [INT]) %1000, dateadd(year, cast(fwpnst as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fwpnst_conv
	,cast(fwpmc1 as [NVARCHAR](3)) as fwpmc1
	,cast(fwpmc2 as [NVARCHAR](3)) as fwpmc2
	,cast(fwdnhr as [DECIMAL](38, 4)) as fwdnhr
	,cast(fwdnhr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwdnhr_conv
	,cast(fwpdfl as [NVARCHAR](1)) as fwpdfl
	,cast(fwukid as [DECIMAL](38, 4)) as fwukid
	,cast(fwtolu as [DECIMAL](38, 4)) as fwtolu
	,cast(fwtolu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwtolu_conv
	,cast(fwtoll as [DECIMAL](38, 4)) as fwtoll
	,cast(fwtoll as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwtoll_conv
	,cast(fwhldd as [INT]) as fwhldd
	,case when cast(fwhldd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fwhldd as [INT]) %1000, dateadd(year, cast(fwhldd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fwhldd_conv
	,cast(fwsphr as [DECIMAL](38, 4)) as fwsphr
	,cast(fwsphr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwsphr_conv
	,cast(fwspwk as [DECIMAL](38, 4)) as fwspwk
	,cast(fwspwk as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwspwk_conv
	,cast(fwspmn as [DECIMAL](38, 4)) as fwspmn
	,cast(fwspmn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwspmn_conv
	,cast(fwwk as [DECIMAL](38, 4)) as fwwk
	,cast(fwwk as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fwwk_conv
	,fwspdw as fwspdw
	,ltrim(rtrim(fwspdw)) as fwspdw_conv
	,cast(fwpdfg as [NVARCHAR](1)) as fwpdfg
	,cast(fwsrvm4 as [DECIMAL](38, 4)) as fwsrvm4
	,cast(fwsrvm4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwsrvm4_conv
	,cast(fwlstm4 as [DECIMAL](38, 4)) as fwlstm4
	,cast(fwlstm4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwlstm4_conv
	,cast(fwcplm4 as [DECIMAL](38, 4)) as fwcplm4
	,cast(fwcplm4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwcplm4_conv
	,cast(fworgm4 as [DECIMAL](38, 4)) as fworgm4
	,cast(fworgm4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fworgm4_conv
	,cast(fwsrvm5 as [DECIMAL](38, 4)) as fwsrvm5
	,cast(fwsrvm5 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwsrvm5_conv
	,cast(fwlstm5 as [DECIMAL](38, 4)) as fwlstm5
	,cast(fwlstm5 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwlstm5_conv
	,cast(fwcplm5 as [DECIMAL](38, 4)) as fwcplm5
	,cast(fwcplm5 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwcplm5_conv
	,cast(fworgm5 as [DECIMAL](38, 4)) as fworgm5
	,cast(fworgm5 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fworgm5_conv
	,cast(fwsrvm6 as [DECIMAL](38, 4)) as fwsrvm6
	,cast(fwsrvm6 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwsrvm6_conv
	,cast(fwlstm6 as [DECIMAL](38, 4)) as fwlstm6
	,cast(fwlstm6 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwlstm6_conv
	,cast(fwcplm6 as [DECIMAL](38, 4)) as fwcplm6
	,cast(fwcplm6 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fwcplm6_conv
	,cast(fworgm6 as [DECIMAL](38, 4)) as fworgm6
	,cast(fworgm6 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fworgm6_conv
FROM 
    stg_jde.tmp_f1207_init
OPTION ( LABEL = 'CREATE_rep_jde.f1207_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f1207_sys_integration_id ON rep_jde.f1207_new(sys_integration_id); 
