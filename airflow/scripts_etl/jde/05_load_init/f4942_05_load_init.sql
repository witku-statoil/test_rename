-- Create rep new table for init
IF OBJECT_ID('rep_jde.f4942_new') IS NOT NULL
    DROP TABLE rep_jde.f4942_new


CREATE TABLE rep_jde.f4942_new
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
	,cast(isshpn as [DECIMAL](38, 4)) as isshpn
	,cast(isrssn as [DECIMAL](38, 4)) as isrssn
	,cast(isdoco as [DECIMAL](38, 4)) as isdoco
	,cast(isdcto as [NVARCHAR](2)) as isdcto
	,iskcoo as iskcoo
	,ltrim(rtrim(iskcoo)) as iskcoo_conv
	,cast(islnid as [DECIMAL](38, 4)) as islnid
	,cast(islnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as islnid_conv
	,cast(iswgts as [DECIMAL](38, 4)) as iswgts
	,cast(iswgts as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iswgts_conv
	,cast(iswtum as [NVARCHAR](2)) as iswtum
	,cast(isscvl as [DECIMAL](38, 4)) as isscvl
	,cast(isscvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isscvl_conv
	,cast(isvlum as [NVARCHAR](2)) as isvlum
	,cast(isitm as [DECIMAL](38, 4)) as isitm
	,cast(issoqs as [DECIMAL](38, 4)) as issoqs
	,cast(issoqs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as issoqs_conv
	,cast(isuom as [NVARCHAR](2)) as isuom
	,cast(isprp1 as [NVARCHAR](3)) as isprp1
	,cast(isnmfc as [NVARCHAR](4)) as isnmfc
	,cast(isdsgp as [NVARCHAR](3)) as isdsgp
	,cast(ishzdc as [NVARCHAR](3)) as ishzdc
	,cast(isfrt1 as [NVARCHAR](6)) as isfrt1
	,cast(isfrt2 as [NVARCHAR](6)) as isfrt2
	,cast(isaexp as [DECIMAL](38, 4)) as isaexp
	,cast(isaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as isaexp_conv
	,cast(isfea as [DECIMAL](38, 4)) as isfea
	,cast(isfea as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as isfea_conv
	,iscrcd as iscrcd
	,ltrim(rtrim(iscrcd)) as iscrcd_conv
	,cast(isecst as [DECIMAL](38, 4)) as isecst
	,cast(isecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as isecst_conv
	,isurcd as isurcd
	,ltrim(rtrim(isurcd)) as isurcd_conv
	,cast(isurdt as [INT]) as isurdt
	,case when cast(isurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(isurdt as [INT]) %1000, dateadd(year, cast(isurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as isurdt_conv
	,cast(isurat as [DECIMAL](38, 4)) as isurat
	,cast(isurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as isurat_conv
	,cast(isurab as [DECIMAL](38, 4)) as isurab
	,cast(isurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as isurab_conv
	,isurrf as isurrf
	,ltrim(rtrim(isurrf)) as isurrf_conv
	,isuser as isuser
	,ltrim(rtrim(isuser)) as isuser_conv
	,ispid as ispid
	,ltrim(rtrim(ispid)) as ispid_conv
	,isjobn as isjobn
	,ltrim(rtrim(isjobn)) as isjobn_conv
	,cast(isupmj as [INT]) as isupmj
	,case when cast(isupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(isupmj as [INT]) %1000, dateadd(year, cast(isupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as isupmj_conv
	,cast(istday as [DECIMAL](38, 4)) as istday
	,cast(iscums as [DECIMAL](38, 4)) as iscums
	,cast(iscums as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iscums_conv
FROM 
    stg_jde.tmp_f4942_init
OPTION ( LABEL = 'CREATE_rep_jde.f4942_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4942_sys_integration_id ON rep_jde.f4942_new(sys_integration_id); 
