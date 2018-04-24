-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4008_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4008_new


CREATE TABLE rep_jde_qat.f4008_new
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
	,tatxa1 as tatxa1
	,ltrim(rtrim(tatxa1)) as tatxa1_conv
	,tataxa as tataxa
	,ltrim(rtrim(tataxa)) as tataxa_conv
	,cast(tata1 as [DECIMAL](38, 4)) as tata1
	,cast(tatxr1 as [DECIMAL](38, 4)) as tatxr1
	,cast(tatxr1 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as tatxr1_conv
	,cast(tata2 as [DECIMAL](38, 4)) as tata2
	,cast(tatxr2 as [DECIMAL](38, 4)) as tatxr2
	,cast(tatxr2 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as tatxr2_conv
	,cast(tata3 as [DECIMAL](38, 4)) as tata3
	,cast(tatxr3 as [DECIMAL](38, 4)) as tatxr3
	,cast(tatxr3 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as tatxr3_conv
	,cast(tata4 as [DECIMAL](38, 4)) as tata4
	,cast(tatxr4 as [DECIMAL](38, 4)) as tatxr4
	,cast(tatxr4 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as tatxr4_conv
	,cast(tata5 as [DECIMAL](38, 4)) as tata5
	,cast(tatxr5 as [DECIMAL](38, 4)) as tatxr5
	,cast(tatxr5 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as tatxr5_conv
	,cast(taefdj as [INT]) as taefdj
	,case when cast(taefdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(taefdj as [INT]) %1000, dateadd(year, cast(taefdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as taefdj_conv
	,cast(taeftj as [INT]) as taeftj
	,case when cast(taeftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(taeftj as [INT]) %1000, dateadd(year, cast(taeftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as taeftj_conv
	,tagl01 as tagl01
	,ltrim(rtrim(tagl01)) as tagl01_conv
	,tagl02 as tagl02
	,ltrim(rtrim(tagl02)) as tagl02_conv
	,tagl03 as tagl03
	,ltrim(rtrim(tagl03)) as tagl03_conv
	,tagl04 as tagl04
	,ltrim(rtrim(tagl04)) as tagl04_conv
	,tagl05 as tagl05
	,ltrim(rtrim(tagl05)) as tagl05_conv
	,cast(taitm as [DECIMAL](38, 4)) as taitm
	,talitm as talitm
	,ltrim(rtrim(talitm)) as talitm_conv
	,taaitm as taaitm
	,ltrim(rtrim(taaitm)) as taaitm_conv
	,cast(tauom as [NVARCHAR](2)) as tauom
	,cast(tafvty as [NVARCHAR](1)) as tafvty
	,cast(tamtax as [DECIMAL](38, 4)) as tamtax
	,cast(tamtax as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as tamtax_conv
	,tatc1 as tatc1
	,ltrim(rtrim(tatc1)) as tatc1_conv
	,tatc2 as tatc2
	,ltrim(rtrim(tatc2)) as tatc2_conv
	,tatc3 as tatc3
	,ltrim(rtrim(tatc3)) as tatc3_conv
	,tatc4 as tatc4
	,ltrim(rtrim(tatc4)) as tatc4_conv
	,tatc5 as tatc5
	,ltrim(rtrim(tatc5)) as tatc5_conv
	,tatt1 as tatt1
	,ltrim(rtrim(tatt1)) as tatt1_conv
	,tatt2 as tatt2
	,ltrim(rtrim(tatt2)) as tatt2_conv
	,cast(tatt3 as [NVARCHAR](1)) as tatt3
	,cast(tatt4 as [NVARCHAR](1)) as tatt4
	,cast(tatt5 as [NVARCHAR](1)) as tatt5
FROM 
    stg_jde_qat.tmp_f4008_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4008_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4008_sys_integration_id ON rep_jde_qat.f4008_new(sys_integration_id); 
