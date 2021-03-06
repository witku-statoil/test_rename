-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f49501_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f49501_new


CREATE TABLE rep_jde_qat.f49501_new
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
	,cast(rdprnb as [DECIMAL](38, 4)) as rdprnb
	,cast(rdlnmb as [DECIMAL](38, 4)) as rdlnmb
	,cast(rdorgn as [DECIMAL](38, 4)) as rdorgn
	,cast(rdancc as [DECIMAL](38, 4)) as rdancc
	,cast(rdcars as [DECIMAL](38, 4)) as rdcars
	,cast(rdcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rdcars_conv
	,cast(rdmot as [NVARCHAR](3)) as rdmot
	,cast(rdfrsc as [NVARCHAR](8)) as rdfrsc
	,cast(rdfrcg as [DECIMAL](38, 4)) as rdfrcg
	,cast(rdfrcg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rdfrcg_conv
	,rdcrcd as rdcrcd
	,ltrim(rtrim(rdcrcd)) as rdcrcd_conv
	,cast(rdfrtp as [NVARCHAR](1)) as rdfrtp
	,cast(rdrtgb as [NVARCHAR](1)) as rdrtgb
	,cast(rdrtum as [NVARCHAR](2)) as rdrtum
	,cast(rddstn as [DECIMAL](38, 4)) as rddstn
	,cast(rdumd1 as [NVARCHAR](2)) as rdumd1
	,cast(rdrtn as [DECIMAL](38, 4)) as rdrtn
	,rdcnmr as rdcnmr
	,ltrim(rtrim(rdcnmr)) as rdcnmr_conv
	,cast(rdltdt as [DECIMAL](38, 4)) as rdltdt
	,cast(rdltdt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rdltdt_conv
	,cast(rdeftj as [INT]) as rdeftj
	,case when cast(rdeftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdeftj as [INT]) %1000, dateadd(year, cast(rdeftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdeftj_conv
	,cast(rdexdj as [INT]) as rdexdj
	,case when cast(rdexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdexdj as [INT]) %1000, dateadd(year, cast(rdexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdexdj_conv
	,rdurcd as rdurcd
	,ltrim(rtrim(rdurcd)) as rdurcd_conv
	,cast(rdurdt as [INT]) as rdurdt
	,case when cast(rdurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdurdt as [INT]) %1000, dateadd(year, cast(rdurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdurdt_conv
	,cast(rdurat as [DECIMAL](38, 4)) as rdurat
	,cast(rdurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rdurat_conv
	,cast(rdurab as [DECIMAL](38, 4)) as rdurab
	,cast(rdurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rdurab_conv
	,rdurrf as rdurrf
	,ltrim(rtrim(rdurrf)) as rdurrf_conv
	,rduser as rduser
	,ltrim(rtrim(rduser)) as rduser_conv
	,rdpid as rdpid
	,ltrim(rtrim(rdpid)) as rdpid_conv
	,rdjobn as rdjobn
	,ltrim(rtrim(rdjobn)) as rdjobn_conv
	,cast(rdupmj as [INT]) as rdupmj
	,case when cast(rdupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdupmj as [INT]) %1000, dateadd(year, cast(rdupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdupmj_conv
	,cast(rdtday as [DECIMAL](38, 4)) as rdtday
	,cast(rdtx as [NVARCHAR](1)) as rdtx
	,rdtxa1 as rdtxa1
	,ltrim(rtrim(rdtxa1)) as rdtxa1_conv
	,cast(rdexr1 as [NVARCHAR](2)) as rdexr1
	,cast(rdtax1 as [NVARCHAR](1)) as rdtax1
	,rdtxa2 as rdtxa2
	,ltrim(rtrim(rdtxa2)) as rdtxa2_conv
	,cast(rdexr2 as [NVARCHAR](2)) as rdexr2
FROM 
    stg_jde_qat.tmp_f49501_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f49501_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f49501_sys_integration_id ON rep_jde_qat.f49501_new(sys_integration_id); 
