-- Create rep new table for init
IF OBJECT_ID('rep_jde.f1202_new') IS NOT NULL
    DROP TABLE rep_jde.f1202_new


CREATE TABLE rep_jde.f1202_new
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
	,flaid as flaid
	,ltrim(rtrim(flaid)) as flaid_conv
	,cast(flctry as [DECIMAL](38, 4)) as flctry
	,cast(flfy as [DECIMAL](38, 4)) as flfy
	,cast(flfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as flfy_conv
	,cast(flfq as [NVARCHAR](4)) as flfq
	,cast(fllt as [NVARCHAR](2)) as fllt
	,flsbl as flsbl
	,ltrim(rtrim(flsbl)) as flsbl_conv
	,flco as flco
	,ltrim(rtrim(flco)) as flco_conv
	,cast(flapyc as [DECIMAL](38, 4)) as flapyc
	,cast(flapyc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flapyc_conv
	,cast(flan01 as [DECIMAL](38, 4)) as flan01
	,cast(flan01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan01_conv
	,cast(flan02 as [DECIMAL](38, 4)) as flan02
	,cast(flan02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan02_conv
	,cast(flan03 as [DECIMAL](38, 4)) as flan03
	,cast(flan03 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan03_conv
	,cast(flan04 as [DECIMAL](38, 4)) as flan04
	,cast(flan04 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan04_conv
	,cast(flan05 as [DECIMAL](38, 4)) as flan05
	,cast(flan05 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan05_conv
	,cast(flan06 as [DECIMAL](38, 4)) as flan06
	,cast(flan06 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan06_conv
	,cast(flan07 as [DECIMAL](38, 4)) as flan07
	,cast(flan07 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan07_conv
	,cast(flan08 as [DECIMAL](38, 4)) as flan08
	,cast(flan08 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan08_conv
	,cast(flan09 as [DECIMAL](38, 4)) as flan09
	,cast(flan09 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan09_conv
	,cast(flan10 as [DECIMAL](38, 4)) as flan10
	,cast(flan10 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan10_conv
	,cast(flan11 as [DECIMAL](38, 4)) as flan11
	,cast(flan11 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan11_conv
	,cast(flan12 as [DECIMAL](38, 4)) as flan12
	,cast(flan12 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan12_conv
	,cast(flan13 as [DECIMAL](38, 4)) as flan13
	,cast(flan13 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan13_conv
	,cast(flan14 as [DECIMAL](38, 4)) as flan14
	,cast(flan14 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flan14_conv
	,cast(flapyn as [DECIMAL](38, 4)) as flapyn
	,cast(flapyn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flapyn_conv
	,cast(flawtd as [DECIMAL](38, 4)) as flawtd
	,cast(flawtd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flawtd_conv
	,cast(flborg as [DECIMAL](38, 4)) as flborg
	,cast(flborg as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flborg_conv
	,cast(flpou as [DECIMAL](38, 4)) as flpou
	,cast(flpou as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flpou_conv
	,cast(flpc as [DECIMAL](38, 4)) as flpc
	,cast(fltker as [DECIMAL](38, 4)) as fltker
	,cast(fltker as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fltker_conv
	,cast(flbreq as [DECIMAL](38, 4)) as flbreq
	,cast(flbreq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flbreq_conv
	,cast(flbapr as [DECIMAL](38, 4)) as flbapr
	,cast(flbapr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as flbapr_conv
	,flmcu as flmcu
	,ltrim(rtrim(flmcu)) as flmcu_conv
	,flobj as flobj
	,ltrim(rtrim(flobj)) as flobj_conv
	,flsub as flsub
	,ltrim(rtrim(flsub)) as flsub_conv
	,cast(flnumb as [DECIMAL](38, 4)) as flnumb
	,cast(fladlm as [DECIMAL](38, 4)) as fladlm
	,cast(fladm as [NVARCHAR](2)) as fladm
	,cast(flitac as [NVARCHAR](1)) as flitac
	,cast(fladmp as [DECIMAL](38, 4)) as fladmp
	,fladsn as fladsn
	,ltrim(rtrim(fladsn)) as fladsn_conv
	,cast(fldir1 as [NVARCHAR](1)) as fldir1
	,cast(fldsd as [INT]) as fldsd
	,case when cast(fldsd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fldsd as [INT]) %1000, dateadd(year, cast(fldsd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fldsd_conv
	,fluser as fluser
	,ltrim(rtrim(fluser)) as fluser_conv
	,cast(fllct as [INT]) as fllct
	,case when cast(fllct as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fllct as [INT]) %1000, dateadd(year, cast(fllct as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fllct_conv
	,flpid as flpid
	,ltrim(rtrim(flpid)) as flpid_conv
	,cast(flsblt as [NVARCHAR](1)) as flsblt
	,flcrcd as flcrcd
	,ltrim(rtrim(flcrcd)) as flcrcd_conv
	,cast(flupmj as [INT]) as flupmj
	,case when cast(flupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(flupmj as [INT]) %1000, dateadd(year, cast(flupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as flupmj_conv
	,fljobn as fljobn
	,ltrim(rtrim(fljobn)) as fljobn_conv
	,cast(flupmt as [DECIMAL](38, 4)) as flupmt
	,cast(flchcd as [NVARCHAR](1)) as flchcd
	,fldpcf as fldpcf
	,ltrim(rtrim(fldpcf)) as fldpcf_conv
	,cast(flcbxr as [DECIMAL](38, 4)) as flcbxr
FROM 
    stg_jde.tmp_f1202_init
OPTION ( LABEL = 'CREATE_rep_jde.f1202_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f1202_sys_integration_id ON rep_jde.f1202_new(sys_integration_id); 
