-- Create rep new table for init
IF OBJECT_ID('rep_jde.f4096_new') IS NOT NULL
    DROP TABLE rep_jde.f4096_new


CREATE TABLE rep_jde.f4096_new
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
	,cast(faanum as [DECIMAL](38, 4)) as faanum
	,cast(faanum as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as faanum_conv
	,cast(faast as [NVARCHAR](8)) as faast
	,faco as faco
	,ltrim(rtrim(faco)) as faco_conv
	,faitem as faitem
	,ltrim(rtrim(faitem)) as faitem_conv
	,faobjf as faobjf
	,ltrim(rtrim(faobjf)) as faobjf_conv
	,faobjt as faobjt
	,ltrim(rtrim(faobjt)) as faobjt_conv
	,cast(fadcto as [NVARCHAR](2)) as fadcto
	,fa_1rt as fa_1rt
	,ltrim(rtrim(fa_1rt)) as fa_1rt_conv
	,cast(fael as [DECIMAL](38, 4)) as fael
	,fadl01 as fadl01
	,ltrim(rtrim(fadl01)) as fadl01_conv
	,cast(fasblt as [NVARCHAR](1)) as fasblt
	,cast(fasegs as [DECIMAL](38, 4)) as fasegs
	,cast(fasfit as [NVARCHAR](10)) as fasfit
	,cast(fasfdt as [NVARCHAR](1)) as fasfdt
	,faabt1 as faabt1
	,ltrim(rtrim(faabt1)) as faabt1_conv
	,fafile as fafile
	,ltrim(rtrim(fafile)) as fafile_conv
	,fapid as fapid
	,ltrim(rtrim(fapid)) as fapid_conv
	,fajobn as fajobn
	,ltrim(rtrim(fajobn)) as fajobn_conv
	,fauser as fauser
	,ltrim(rtrim(fauser)) as fauser_conv
	,cast(faupmj as [INT]) as faupmj
	,case when cast(faupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(faupmj as [INT]) %1000, dateadd(year, cast(faupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as faupmj_conv
	,cast(fatday as [DECIMAL](38, 4)) as fatday
FROM 
    stg_jde.tmp_f4096_init
OPTION ( LABEL = 'CREATE_rep_jde.f4096_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4096_sys_integration_id ON rep_jde.f4096_new(sys_integration_id); 
