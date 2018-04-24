-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f554503_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f554503_new


CREATE TABLE rep_jde_qat.f554503_new
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
	,pry55qn as pry55qn
	,ltrim(rtrim(pry55qn)) as pry55qn_conv
	,cast(pry55qt as [NVARCHAR](2)) as pry55qt
	,cast(pry55qdte as [INT]) as pry55qdte
	,case when cast(pry55qdte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pry55qdte as [INT]) %1000, dateadd(year, cast(pry55qdte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pry55qdte_conv
	,cast(pry55qs as [NVARCHAR](2)) as pry55qs
	,pry55quty as pry55quty
	,ltrim(rtrim(pry55quty)) as pry55quty_conv
	,cast(pruncs as [DECIMAL](38, 4)) as pruncs
	,cast(pruncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pruncs_conv
	,prcrcd as prcrcd
	,ltrim(rtrim(prcrcd)) as prcrcd_conv
	,cast(pruom as [NVARCHAR](2)) as pruom
	,cast(pry55avin as [NVARCHAR](2)) as pry55avin
	,pry55jdqn as pry55jdqn
	,ltrim(rtrim(pry55jdqn)) as pry55jdqn_conv
	,prflag as prflag
	,ltrim(rtrim(prflag)) as prflag_conv
	,prurrf as prurrf
	,ltrim(rtrim(prurrf)) as prurrf_conv
	,prurcd as prurcd
	,ltrim(rtrim(prurcd)) as prurcd_conv
	,cast(prurab as [DECIMAL](38, 4)) as prurab
	,cast(prurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as prurab_conv
	,cast(prurat as [DECIMAL](38, 4)) as prurat
	,cast(prurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as prurat_conv
	,cast(prurdt as [INT]) as prurdt
	,case when cast(prurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(prurdt as [INT]) %1000, dateadd(year, cast(prurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as prurdt_conv
	,pruser as pruser
	,ltrim(rtrim(pruser)) as pruser_conv
	,prpid as prpid
	,ltrim(rtrim(prpid)) as prpid_conv
	,prjobn as prjobn
	,ltrim(rtrim(prjobn)) as prjobn_conv
	,cast(prupmt as [DECIMAL](38, 4)) as prupmt
	,cast(prupmj as [INT]) as prupmj
	,case when cast(prupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(prupmj as [INT]) %1000, dateadd(year, cast(prupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as prupmj_conv
	,pry55char1 as pry55char1
	,ltrim(rtrim(pry55char1)) as pry55char1_conv
	,pry55char2 as pry55char2
	,ltrim(rtrim(pry55char2)) as pry55char2_conv
	,cast(pry55date1 as [INT]) as pry55date1
	,case when cast(pry55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pry55date1 as [INT]) %1000, dateadd(year, cast(pry55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pry55date1_conv
	,cast(pry55date2 as [INT]) as pry55date2
	,case when cast(pry55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pry55date2 as [INT]) %1000, dateadd(year, cast(pry55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pry55date2_conv
	,pry55strg1 as pry55strg1
	,ltrim(rtrim(pry55strg1)) as pry55strg1_conv
	,pry55strg2 as pry55strg2
	,ltrim(rtrim(pry55strg2)) as pry55strg2_conv
	,cast(pry55time1 as [DECIMAL](38, 4)) as pry55time1
	,cast(pry55time2 as [DECIMAL](38, 4)) as pry55time2
	,cast(pry55amnt1 as [DECIMAL](38, 4)) as pry55amnt1
	,cast(pry55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pry55amnt1_conv
	,cast(pry55amnt2 as [DECIMAL](38, 4)) as pry55amnt2
	,cast(pry55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pry55amnt2_conv
FROM 
    stg_jde_qat.tmp_f554503_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f554503_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554503_sys_integration_id ON rep_jde_qat.f554503_new(sys_integration_id); 
