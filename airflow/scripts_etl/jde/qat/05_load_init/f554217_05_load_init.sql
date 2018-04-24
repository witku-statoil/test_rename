-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f554217_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f554217_new


CREATE TABLE rep_jde_qat.f554217_new
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
	,cast(span8 as [DECIMAL](38, 4)) as span8
	,cast(spac06 as [NVARCHAR](3)) as spac06
	,spaddz as spaddz
	,ltrim(rtrim(spaddz)) as spaddz_conv
	,cast(spshan as [DECIMAL](38, 4)) as spshan
	,cast(spat1 as [NVARCHAR](3)) as spat1
	,cast(spac01 as [NVARCHAR](3)) as spac01
	,cast(spac02 as [NVARCHAR](3)) as spac02
	,cast(spac03 as [NVARCHAR](3)) as spac03
	,spar1 as spar1
	,ltrim(rtrim(spar1)) as spar1_conv
	,spph1 as spph1
	,ltrim(rtrim(spph1)) as spph1_conv
	,cast(spcmtb as [INT]) as spcmtb
	,case when cast(spcmtb as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(spcmtb as [INT]) %1000, dateadd(year, cast(spcmtb as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as spcmtb_conv
	,cast(spptmb as [DECIMAL](38, 4)) as spptmb
	,cast(spcmte as [INT]) as spcmte
	,case when cast(spcmte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(spcmte as [INT]) %1000, dateadd(year, cast(spcmte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as spcmte_conv
	,spev01 as spev01
	,ltrim(rtrim(spev01)) as spev01_conv
	,cast(spptme as [DECIMAL](38, 4)) as spptme
	,cast(spurab as [DECIMAL](38, 4)) as spurab
	,cast(spurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as spurab_conv
	,cast(spupmj as [INT]) as spupmj
	,case when cast(spupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(spupmj as [INT]) %1000, dateadd(year, cast(spupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as spupmj_conv
	,cast(spupmt as [DECIMAL](38, 4)) as spupmt
	,spjobn as spjobn
	,ltrim(rtrim(spjobn)) as spjobn_conv
	,sppid as sppid
	,ltrim(rtrim(sppid)) as sppid_conv
	,spuser as spuser
	,ltrim(rtrim(spuser)) as spuser_conv
	,spmcu as spmcu
	,ltrim(rtrim(spmcu)) as spmcu_conv
	,cast(sprp24 as [NVARCHAR](10)) as sprp24
	,spy55char1 as spy55char1
	,ltrim(rtrim(spy55char1)) as spy55char1_conv
	,spy55char2 as spy55char2
	,ltrim(rtrim(spy55char2)) as spy55char2_conv
	,spy55strg1 as spy55strg1
	,ltrim(rtrim(spy55strg1)) as spy55strg1_conv
	,spy55strg2 as spy55strg2
	,ltrim(rtrim(spy55strg2)) as spy55strg2_conv
	,cast(spy55amnt1 as [DECIMAL](38, 4)) as spy55amnt1
	,cast(spy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as spy55amnt1_conv
	,cast(spy55amnt2 as [DECIMAL](38, 4)) as spy55amnt2
	,cast(spy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as spy55amnt2_conv
	,cast(spy55time1 as [DECIMAL](38, 4)) as spy55time1
	,cast(spy55time2 as [DECIMAL](38, 4)) as spy55time2
	,cast(spy55date1 as [INT]) as spy55date1
	,case when cast(spy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(spy55date1 as [INT]) %1000, dateadd(year, cast(spy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as spy55date1_conv
	,cast(spy55date2 as [INT]) as spy55date2
	,case when cast(spy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(spy55date2 as [INT]) %1000, dateadd(year, cast(spy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as spy55date2_conv
	,cast(spcrtj as [INT]) as spcrtj
	,case when cast(spcrtj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(spcrtj as [INT]) %1000, dateadd(year, cast(spcrtj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as spcrtj_conv
	,spterm as spterm
	,ltrim(rtrim(spterm)) as spterm_conv
	,cast(sppa8 as [DECIMAL](38, 4)) as sppa8
	,sptx1 as sptx1
	,ltrim(rtrim(sptx1)) as sptx1_conv
	,sptax as sptax
	,ltrim(rtrim(sptax)) as sptax_conv
	,spaddznh as spaddznh
	,ltrim(rtrim(spaddznh)) as spaddznh_conv
FROM 
    stg_jde_qat.tmp_f554217_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f554217_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554217_sys_integration_id ON rep_jde_qat.f554217_new(sys_integration_id); 
