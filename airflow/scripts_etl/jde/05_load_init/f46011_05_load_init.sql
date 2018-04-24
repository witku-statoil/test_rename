-- Create rep new table for init
IF OBJECT_ID('rep_jde.f46011_new') IS NOT NULL
    DROP TABLE rep_jde.f46011_new


CREATE TABLE rep_jde.f46011_new
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
	,iqmcu as iqmcu
	,ltrim(rtrim(iqmcu)) as iqmcu_conv
	,cast(iqprp6 as [NVARCHAR](6)) as iqprp6
	,cast(iqitm as [DECIMAL](38, 4)) as iqitm
	,cast(iquom as [NVARCHAR](2)) as iquom
	,cast(iqgwid as [DECIMAL](38, 4)) as iqgwid
	,cast(iqgwid as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as iqgwid_conv
	,cast(iqgdep as [DECIMAL](38, 4)) as iqgdep
	,cast(iqgdep as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as iqgdep_conv
	,cast(iqghet as [DECIMAL](38, 4)) as iqghet
	,cast(iqghet as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as iqghet_conv
	,cast(iqwium as [NVARCHAR](2)) as iqwium
	,cast(iqgcub as [DECIMAL](38, 4)) as iqgcub
	,cast(iqgcub as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as iqgcub_conv
	,cast(iqvumd as [NVARCHAR](2)) as iqvumd
	,cast(iqgwei as [DECIMAL](38, 4)) as iqgwei
	,cast(iqgwei as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as iqgwei_conv
	,cast(iquwum as [NVARCHAR](2)) as iquwum
	,cast(iqdmth as [NVARCHAR](1)) as iqdmth
	,cast(iqcrmt as [NVARCHAR](1)) as iqcrmt
	,cast(iqequs as [NVARCHAR](1)) as iqequs
	,iqarot as iqarot
	,ltrim(rtrim(iqarot)) as iqarot_conv
	,iqabkd as iqabkd
	,ltrim(rtrim(iqabkd)) as iqabkd_conv
	,iqarol as iqarol
	,ltrim(rtrim(iqarol)) as iqarol_conv
	,cast(iqslim as [DECIMAL](38, 4)) as iqslim
	,cast(iqslim as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as iqslim_conv
	,cast(iqeqty as [NVARCHAR](5)) as iqeqty
	,iqrpck as iqrpck
	,ltrim(rtrim(iqrpck)) as iqrpck_conv
	,cast(iqpack as [NVARCHAR](4)) as iqpack
	,iqlipl as iqlipl
	,ltrim(rtrim(iqlipl)) as iqlipl_conv
	,cast(iqpptg as [NVARCHAR](1)) as iqpptg
	,cast(iqpktg as [NVARCHAR](1)) as iqpktg
	,cast(iqprtg as [NVARCHAR](1)) as iqprtg
	,iqptra as iqptra
	,ltrim(rtrim(iqptra)) as iqptra_conv
	,iqktra as iqktra
	,ltrim(rtrim(iqktra)) as iqktra_conv
	,iqrtra as iqrtra
	,ltrim(rtrim(iqrtra)) as iqrtra_conv
	,iquser as iquser
	,ltrim(rtrim(iquser)) as iquser_conv
	,iqpid as iqpid
	,ltrim(rtrim(iqpid)) as iqpid_conv
	,iqjobn as iqjobn
	,ltrim(rtrim(iqjobn)) as iqjobn_conv
	,cast(iqupmj as [INT]) as iqupmj
	,case when cast(iqupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(iqupmj as [INT]) %1000, dateadd(year, cast(iqupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as iqupmj_conv
	,cast(iqtday as [DECIMAL](38, 4)) as iqtday
	,iquccu as iquccu
	,ltrim(rtrim(iquccu)) as iquccu_conv
FROM 
    stg_jde.tmp_f46011_init
OPTION ( LABEL = 'CREATE_rep_jde.f46011_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f46011_sys_integration_id ON rep_jde.f46011_new(sys_integration_id); 
