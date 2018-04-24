-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4105_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4105_new


CREATE TABLE rep_jde_qat.f4105_new
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
	,cast(coitm as [DECIMAL](38, 4)) as coitm
	,colitm as colitm
	,ltrim(rtrim(colitm)) as colitm_conv
	,coaitm as coaitm
	,ltrim(rtrim(coaitm)) as coaitm_conv
	,comcu as comcu
	,ltrim(rtrim(comcu)) as comcu_conv
	,colocn as colocn
	,ltrim(rtrim(colocn)) as colocn_conv
	,colotn as colotn
	,ltrim(rtrim(colotn)) as colotn_conv
	,cast(colotg as [NVARCHAR](3)) as colotg
	,cast(coledg as [NVARCHAR](2)) as coledg
	,cast(councs as [DECIMAL](38, 4)) as councs
	,cast(councs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as councs_conv
	,cast(cocspo as [NVARCHAR](1)) as cocspo
	,cast(cocsin as [NVARCHAR](1)) as cocsin
	,courcd as courcd
	,ltrim(rtrim(courcd)) as courcd_conv
	,cast(courdt as [INT]) as courdt
	,case when cast(courdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(courdt as [INT]) %1000, dateadd(year, cast(courdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as courdt_conv
	,cast(courat as [DECIMAL](38, 4)) as courat
	,cast(courat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as courat_conv
	,cast(courab as [DECIMAL](38, 4)) as courab
	,cast(courab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as courab_conv
	,courrf as courrf
	,ltrim(rtrim(courrf)) as courrf_conv
	,couser as couser
	,ltrim(rtrim(couser)) as couser_conv
	,copid as copid
	,ltrim(rtrim(copid)) as copid_conv
	,cojobn as cojobn
	,ltrim(rtrim(cojobn)) as cojobn_conv
	,cast(coupmj as [INT]) as coupmj
	,case when cast(coupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(coupmj as [INT]) %1000, dateadd(year, cast(coupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as coupmj_conv
	,cast(cotday as [DECIMAL](38, 4)) as cotday
	,coccfl as coccfl
	,ltrim(rtrim(coccfl)) as coccfl_conv
	,cast(cocrcs as [DECIMAL](38, 4)) as cocrcs
	,cast(cocrcs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as cocrcs_conv
	,cast(coostc as [DECIMAL](38, 4)) as coostc
	,cast(coostc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as coostc_conv
	,cast(costoc as [DECIMAL](38, 4)) as costoc
	,cast(costoc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as costoc_conv
FROM 
    stg_jde_qat.tmp_f4105_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4105_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4105_sys_integration_id ON rep_jde_qat.f4105_new(sys_integration_id); 
