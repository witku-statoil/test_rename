-- Create rep new table for init
IF OBJECT_ID('rep_jde.f554905_new') IS NOT NULL
    DROP TABLE rep_jde.f554905_new


CREATE TABLE rep_jde.f554905_new
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
	,rdmcu as rdmcu
	,ltrim(rtrim(rdmcu)) as rdmcu_conv
	,cast(rdmot as [NVARCHAR](3)) as rdmot
	,cast(rdan8 as [DECIMAL](38, 4)) as rdan8
	,rdvehi as rdvehi
	,ltrim(rtrim(rdvehi)) as rdvehi_conv
	,cast(rdrtnm as [NVARCHAR](10)) as rdrtnm
	,cast(rdcgc1 as [NVARCHAR](3)) as rdcgc1
	,cast(rdcgc2 as [NVARCHAR](3)) as rdcgc2
	,cast(rdfrtp as [NVARCHAR](1)) as rdfrtp
	,cast(rdrtgb as [NVARCHAR](1)) as rdrtgb
	,cast(rdrtum as [NVARCHAR](2)) as rdrtum
	,cast(rdfrcg as [DECIMAL](38, 4)) as rdfrcg
	,cast(rdfrcg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rdfrcg_conv
	,cast(rdeftj as [INT]) as rdeftj
	,case when cast(rdeftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdeftj as [INT]) %1000, dateadd(year, cast(rdeftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdeftj_conv
	,cast(rdexdj as [INT]) as rdexdj
	,case when cast(rdexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdexdj as [INT]) %1000, dateadd(year, cast(rdexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdexdj_conv
	,rdcrcd as rdcrcd
	,ltrim(rtrim(rdcrcd)) as rdcrcd_conv
	,rduser as rduser
	,ltrim(rtrim(rduser)) as rduser_conv
	,rdpid as rdpid
	,ltrim(rtrim(rdpid)) as rdpid_conv
	,rdjobn as rdjobn
	,ltrim(rtrim(rdjobn)) as rdjobn_conv
	,cast(rdupmj as [INT]) as rdupmj
	,case when cast(rdupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdupmj as [INT]) %1000, dateadd(year, cast(rdupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdupmj_conv
	,cast(rdupmt as [DECIMAL](38, 4)) as rdupmt
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
	,rdy55char1 as rdy55char1
	,ltrim(rtrim(rdy55char1)) as rdy55char1_conv
	,rdy55char2 as rdy55char2
	,ltrim(rtrim(rdy55char2)) as rdy55char2_conv
	,cast(rdy55date1 as [INT]) as rdy55date1
	,case when cast(rdy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdy55date1 as [INT]) %1000, dateadd(year, cast(rdy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdy55date1_conv
	,cast(rdy55date2 as [INT]) as rdy55date2
	,case when cast(rdy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rdy55date2 as [INT]) %1000, dateadd(year, cast(rdy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rdy55date2_conv
	,rdy55strg1 as rdy55strg1
	,ltrim(rtrim(rdy55strg1)) as rdy55strg1_conv
	,rdy55strg2 as rdy55strg2
	,ltrim(rtrim(rdy55strg2)) as rdy55strg2_conv
	,cast(rdy55time1 as [DECIMAL](38, 4)) as rdy55time1
	,cast(rdy55time2 as [DECIMAL](38, 4)) as rdy55time2
	,cast(rdy55amnt1 as [DECIMAL](38, 4)) as rdy55amnt1
	,cast(rdy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rdy55amnt1_conv
	,cast(rdy55amnt2 as [DECIMAL](38, 4)) as rdy55amnt2
	,cast(rdy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rdy55amnt2_conv
FROM 
    stg_jde.tmp_f554905_init
OPTION ( LABEL = 'CREATE_rep_jde.f554905_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554905_sys_integration_id ON rep_jde.f554905_new(sys_integration_id); 
