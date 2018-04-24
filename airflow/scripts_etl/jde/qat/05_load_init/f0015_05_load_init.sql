-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f0015_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f0015_new


CREATE TABLE rep_jde_qat.f0015_new
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
	,cxcrcd as cxcrcd
	,ltrim(rtrim(cxcrcd)) as cxcrcd_conv
	,cxcrdc as cxcrdc
	,ltrim(rtrim(cxcrdc)) as cxcrdc_conv
	,cast(cxan8 as [DECIMAL](38, 4)) as cxan8
	,cast(cxrttyp as [NVARCHAR](2)) as cxrttyp
	,cast(cxeft as [INT]) as cxeft
	,case when cast(cxeft as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cxeft as [INT]) %1000, dateadd(year, cast(cxeft as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cxeft_conv
	,cxclmeth as cxclmeth
	,ltrim(rtrim(cxclmeth)) as cxclmeth_conv
	,cxcrcm as cxcrcm
	,ltrim(rtrim(cxcrcm)) as cxcrcm_conv
	,cxtrcr as cxtrcr
	,ltrim(rtrim(cxtrcr)) as cxtrcr_conv
	,cast(cxcrr as [DECIMAL](38, 4)) as cxcrr
	,cast(cxcrrd as [DECIMAL](38, 4)) as cxcrrd
	,cxcsr as cxcsr
	,ltrim(rtrim(cxcsr)) as cxcsr_conv
	,cxuser as cxuser
	,ltrim(rtrim(cxuser)) as cxuser_conv
	,cxpid as cxpid
	,ltrim(rtrim(cxpid)) as cxpid_conv
	,cxjobn as cxjobn
	,ltrim(rtrim(cxjobn)) as cxjobn_conv
	,cast(cxupmj as [INT]) as cxupmj
	,case when cast(cxupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cxupmj as [INT]) %1000, dateadd(year, cast(cxupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cxupmj_conv
	,cast(cxupmt as [DECIMAL](38, 4)) as cxupmt
FROM 
    stg_jde_qat.tmp_f0015_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f0015_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f0015_sys_integration_id ON rep_jde_qat.f0015_new(sys_integration_id); 
