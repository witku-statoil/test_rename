-- Create rep new table for init
IF OBJECT_ID('rep_jde.f0116_new') IS NOT NULL
    DROP TABLE rep_jde.f0116_new


CREATE TABLE rep_jde.f0116_new
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
	,cast(alan8 as [DECIMAL](38, 4)) as alan8
	,cast(aleftb as [INT]) as aleftb
	,case when cast(aleftb as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aleftb as [INT]) %1000, dateadd(year, cast(aleftb as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aleftb_conv
	,aleftf as aleftf
	,ltrim(rtrim(aleftf)) as aleftf_conv
	,aladd1 as aladd1
	,ltrim(rtrim(aladd1)) as aladd1_conv
	,aladd2 as aladd2
	,ltrim(rtrim(aladd2)) as aladd2_conv
	,aladd3 as aladd3
	,ltrim(rtrim(aladd3)) as aladd3_conv
	,aladd4 as aladd4
	,ltrim(rtrim(aladd4)) as aladd4_conv
	,aladdz as aladdz
	,ltrim(rtrim(aladdz)) as aladdz_conv
	,alcty1 as alcty1
	,ltrim(rtrim(alcty1)) as alcty1_conv
	,cast(alcoun as [NVARCHAR](25)) as alcoun
	,cast(aladds as [NVARCHAR](3)) as aladds
	,alcrte as alcrte
	,ltrim(rtrim(alcrte)) as alcrte_conv
	,albkml as albkml
	,ltrim(rtrim(albkml)) as albkml_conv
	,cast(alctr as [NVARCHAR](3)) as alctr
	,aluser as aluser
	,ltrim(rtrim(aluser)) as aluser_conv
	,alpid as alpid
	,ltrim(rtrim(alpid)) as alpid_conv
	,cast(alupmj as [INT]) as alupmj
	,case when cast(alupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(alupmj as [INT]) %1000, dateadd(year, cast(alupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as alupmj_conv
	,aljobn as aljobn
	,ltrim(rtrim(aljobn)) as aljobn_conv
	,cast(alupmt as [DECIMAL](38, 4)) as alupmt
	,cast(alsyncs as [DECIMAL](38, 4)) as alsyncs
	,cast(alcaad as [DECIMAL](38, 4)) as alcaad
FROM 
    stg_jde.tmp_f0116_init
OPTION ( LABEL = 'CREATE_rep_jde.f0116_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f0116_sys_integration_id ON rep_jde.f0116_new(sys_integration_id); 
