-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f0115_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f0115_new


CREATE TABLE rep_jde_qat.f0115_new
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
	,cast(wpan8 as [DECIMAL](38, 4)) as wpan8
	,cast(wpidln as [DECIMAL](38, 4)) as wpidln
	,cast(wpidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wpidln_conv
	,cast(wprck7 as [DECIMAL](38, 4)) as wprck7
	,cast(wpcnln as [DECIMAL](38, 4)) as wpcnln
	,cast(wpcnln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wpcnln_conv
	,cast(wpphtp as [NVARCHAR](4)) as wpphtp
	,wpar1 as wpar1
	,ltrim(rtrim(wpar1)) as wpar1_conv
	,wpph1 as wpph1
	,ltrim(rtrim(wpph1)) as wpph1_conv
	,wpuser as wpuser
	,ltrim(rtrim(wpuser)) as wpuser_conv
	,wppid as wppid
	,ltrim(rtrim(wppid)) as wppid_conv
	,cast(wpupmj as [INT]) as wpupmj
	,case when cast(wpupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wpupmj as [INT]) %1000, dateadd(year, cast(wpupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wpupmj_conv
	,wpjobn as wpjobn
	,ltrim(rtrim(wpjobn)) as wpjobn_conv
	,cast(wpupmt as [DECIMAL](38, 4)) as wpupmt
	,cast(wpcfno1 as [DECIMAL](38, 4)) as wpcfno1
	,cast(wpcfno1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wpcfno1_conv
	,wpgen1 as wpgen1
	,ltrim(rtrim(wpgen1)) as wpgen1_conv
	,wpfalge as wpfalge
	,ltrim(rtrim(wpfalge)) as wpfalge_conv
	,cast(wpsyncs as [DECIMAL](38, 4)) as wpsyncs
	,cast(wpcaad as [DECIMAL](38, 4)) as wpcaad
FROM 
    stg_jde_qat.tmp_f0115_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f0115_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f0115_sys_integration_id ON rep_jde_qat.f0115_new(sys_integration_id); 
