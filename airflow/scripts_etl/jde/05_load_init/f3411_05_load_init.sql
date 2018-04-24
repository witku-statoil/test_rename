-- Create rep new table for init
IF OBJECT_ID('rep_jde.f3411_new') IS NOT NULL
    DROP TABLE rep_jde.f3411_new


CREATE TABLE rep_jde.f3411_new
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
	,cast(mmukid as [DECIMAL](38, 4)) as mmukid
	,cast(mmitm as [DECIMAL](38, 4)) as mmitm
	,mmmcu as mmmcu
	,ltrim(rtrim(mmmcu)) as mmmcu_conv
	,mmmmcu as mmmmcu
	,ltrim(rtrim(mmmmcu)) as mmmmcu_conv
	,cast(mmmsgt as [NVARCHAR](1)) as mmmsgt
	,cast(mmmsga as [NVARCHAR](1)) as mmmsga
	,cast(mmhcld as [NVARCHAR](1)) as mmhcld
	,mmkcoo as mmkcoo
	,ltrim(rtrim(mmkcoo)) as mmkcoo_conv
	,cast(mmdoco as [DECIMAL](38, 4)) as mmdoco
	,cast(mmdcto as [NVARCHAR](2)) as mmdcto
	,cast(mmlnid as [DECIMAL](38, 4)) as mmlnid
	,cast(mmlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as mmlnid_conv
	,mmsfxo as mmsfxo
	,ltrim(rtrim(mmsfxo)) as mmsfxo_conv
	,mmdsc1 as mmdsc1
	,ltrim(rtrim(mmdsc1)) as mmdsc1_conv
	,cast(mmtrqt as [DECIMAL](38, 4)) as mmtrqt
	,cast(mmtrqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mmtrqt_conv
	,cast(mmvend as [DECIMAL](38, 4)) as mmvend
	,cast(mmvend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mmvend_conv
	,cast(mmdrqj as [INT]) as mmdrqj
	,case when cast(mmdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mmdrqj as [INT]) %1000, dateadd(year, cast(mmdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mmdrqj_conv
	,cast(mmstrt as [INT]) as mmstrt
	,case when cast(mmstrt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mmstrt as [INT]) %1000, dateadd(year, cast(mmstrt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mmstrt_conv
	,cast(mmrstj as [INT]) as mmrstj
	,case when cast(mmrstj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mmrstj as [INT]) %1000, dateadd(year, cast(mmrstj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mmrstj_conv
	,cast(mmrrqj as [INT]) as mmrrqj
	,case when cast(mmrrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mmrrqj as [INT]) %1000, dateadd(year, cast(mmrrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mmrrqj_conv
	,cast(mmupmj as [INT]) as mmupmj
	,case when cast(mmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mmupmj as [INT]) %1000, dateadd(year, cast(mmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mmupmj_conv
	,cast(mmtday as [DECIMAL](38, 4)) as mmtday
	,mmuser as mmuser
	,ltrim(rtrim(mmuser)) as mmuser_conv
	,mmjobn as mmjobn
	,ltrim(rtrim(mmjobn)) as mmjobn_conv
	,mmpid as mmpid
	,ltrim(rtrim(mmpid)) as mmpid_conv
	,cast(mmredj as [INT]) as mmredj
	,case when cast(mmredj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mmredj as [INT]) %1000, dateadd(year, cast(mmredj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mmredj_conv
	,cast(mmoedj as [INT]) as mmoedj
	,case when cast(mmoedj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mmoedj as [INT]) %1000, dateadd(year, cast(mmoedj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mmoedj_conv
	,mmline as mmline
	,ltrim(rtrim(mmline)) as mmline_conv
	,cast(mmprjm as [DECIMAL](38, 4)) as mmprjm
	,cast(mmprjm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mmprjm_conv
	,cast(mmsrdm as [DECIMAL](38, 4)) as mmsrdm
	,cast(mmsrdm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mmsrdm_conv
	,mmostt as mmostt
	,ltrim(rtrim(mmostt)) as mmostt_conv
	,mmlotn as mmlotn
	,ltrim(rtrim(mmlotn)) as mmlotn_conv
	,cast(mmpns as [DECIMAL](38, 4)) as mmpns
	,mmpmpn as mmpmpn
	,ltrim(rtrim(mmpmpn)) as mmpmpn_conv
FROM 
    stg_jde.tmp_f3411_init
OPTION ( LABEL = 'CREATE_rep_jde.f3411_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f3411_sys_integration_id ON rep_jde.f3411_new(sys_integration_id); 
