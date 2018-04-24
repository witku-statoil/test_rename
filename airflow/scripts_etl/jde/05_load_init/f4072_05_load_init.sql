-- Create rep new table for init
IF OBJECT_ID('rep_jde.f4072_new') IS NOT NULL
    DROP TABLE rep_jde.f4072_new


CREATE TABLE rep_jde.f4072_new
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
	,cast(adast as [NVARCHAR](8)) as adast
	,cast(aditm as [DECIMAL](38, 4)) as aditm
	,adlitm as adlitm
	,ltrim(rtrim(adlitm)) as adlitm_conv
	,adaitm as adaitm
	,ltrim(rtrim(adaitm)) as adaitm_conv
	,cast(adan8 as [DECIMAL](38, 4)) as adan8
	,cast(adigid as [DECIMAL](38, 4)) as adigid
	,cast(adigid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as adigid_conv
	,cast(adcgid as [DECIMAL](38, 4)) as adcgid
	,cast(adcgid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as adcgid_conv
	,cast(adogid as [DECIMAL](38, 4)) as adogid
	,cast(adogid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as adogid_conv
	,adcrcd as adcrcd
	,ltrim(rtrim(adcrcd)) as adcrcd_conv
	,cast(aduom as [NVARCHAR](2)) as aduom
	,cast(admnq as [DECIMAL](38, 4)) as admnq
	,cast(admnq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as admnq_conv
	,cast(adeftj as [INT]) as adeftj
	,case when cast(adeftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(adeftj as [INT]) %1000, dateadd(year, cast(adeftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as adeftj_conv
	,cast(adexdj as [INT]) as adexdj
	,case when cast(adexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(adexdj as [INT]) %1000, dateadd(year, cast(adexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as adexdj_conv
	,cast(adbscd as [NVARCHAR](1)) as adbscd
	,cast(adledg as [NVARCHAR](2)) as adledg
	,cast(adfrmn as [NVARCHAR](10)) as adfrmn
	,cast(adfvtr as [DECIMAL](38, 4)) as adfvtr
	,cast(adfvtr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as adfvtr_conv
	,adfgy as adfgy
	,ltrim(rtrim(adfgy)) as adfgy_conv
	,cast(adatid as [DECIMAL](38, 4)) as adatid
	,adurcd as adurcd
	,ltrim(rtrim(adurcd)) as adurcd_conv
	,cast(adurdt as [INT]) as adurdt
	,case when cast(adurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(adurdt as [INT]) %1000, dateadd(year, cast(adurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as adurdt_conv
	,cast(adurat as [DECIMAL](38, 4)) as adurat
	,cast(adurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as adurat_conv
	,cast(adurab as [DECIMAL](38, 4)) as adurab
	,cast(adurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as adurab_conv
	,adurrf as adurrf
	,ltrim(rtrim(adurrf)) as adurrf_conv
	,cast(adnbrord as [DECIMAL](38, 4)) as adnbrord
	,cast(adnbrord as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as adnbrord_conv
	,cast(aduomvid as [NVARCHAR](2)) as aduomvid
	,cast(adfvum as [NVARCHAR](2)) as adfvum
	,adpartfg as adpartfg
	,ltrim(rtrim(adpartfg)) as adpartfg_conv
	,cast(adaprs as [NVARCHAR](1)) as adaprs
	,aduser as aduser
	,ltrim(rtrim(aduser)) as aduser_conv
	,adpid as adpid
	,ltrim(rtrim(adpid)) as adpid_conv
	,adjobn as adjobn
	,ltrim(rtrim(adjobn)) as adjobn_conv
	,cast(adupmj as [INT]) as adupmj
	,case when cast(adupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(adupmj as [INT]) %1000, dateadd(year, cast(adupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as adupmj_conv
	,cast(adtday as [DECIMAL](38, 4)) as adtday
	,cast(adbktpid as [DECIMAL](38, 4)) as adbktpid
	,cast(adbktpid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as adbktpid_conv
	,adcrcdvid as adcrcdvid
	,ltrim(rtrim(adcrcdvid)) as adcrcdvid_conv
	,adrulename as adrulename
	,ltrim(rtrim(adrulename)) as adrulename_conv
FROM 
    stg_jde.tmp_f4072_init
OPTION ( LABEL = 'CREATE_rep_jde.f4072_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4072_sys_integration_id ON rep_jde.f4072_new(sys_integration_id); 
