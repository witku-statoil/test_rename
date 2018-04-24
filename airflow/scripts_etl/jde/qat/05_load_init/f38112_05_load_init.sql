-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f38112_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f38112_new


CREATE TABLE rep_jde_qat.f38112_new
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
	,cast(dxdoco as [DECIMAL](38, 4)) as dxdoco
	,cast(dxdcto as [NVARCHAR](2)) as dxdcto
	,dxkcoo as dxkcoo
	,ltrim(rtrim(dxkcoo)) as dxkcoo_conv
	,cast(dxlnid as [DECIMAL](38, 4)) as dxlnid
	,cast(dxlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as dxlnid_conv
	,dxdmct as dxdmct
	,ltrim(rtrim(dxdmct)) as dxdmct_conv
	,cast(dxdmcs as [DECIMAL](38, 4)) as dxdmcs
	,dxmcu as dxmcu
	,ltrim(rtrim(dxmcu)) as dxmcu_conv
	,cast(dxseq as [DECIMAL](38, 4)) as dxseq
	,dxpsr as dxpsr
	,ltrim(rtrim(dxpsr)) as dxpsr_conv
	,cast(dxpsry as [NVARCHAR](2)) as dxpsry
	,cast(dxqcom as [DECIMAL](38, 4)) as dxqcom
	,cast(dxqcom as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dxqcom_conv
	,cast(dxcnqy as [NVARCHAR](1)) as dxcnqy
	,cast(dxaa as [DECIMAL](38, 4)) as dxaa
	,cast(dxaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dxaa_conv
	,dxcrcd as dxcrcd
	,ltrim(rtrim(dxcrcd)) as dxcrcd_conv
	,cast(dxuom as [NVARCHAR](2)) as dxuom
	,cast(dxtrdj as [INT]) as dxtrdj
	,case when cast(dxtrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dxtrdj as [INT]) %1000, dateadd(year, cast(dxtrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dxtrdj_conv
	,dxtv01 as dxtv01
	,ltrim(rtrim(dxtv01)) as dxtv01_conv
	,dxtv02 as dxtv02
	,ltrim(rtrim(dxtv02)) as dxtv02_conv
	,dxtv03 as dxtv03
	,ltrim(rtrim(dxtv03)) as dxtv03_conv
	,dxurcd as dxurcd
	,ltrim(rtrim(dxurcd)) as dxurcd_conv
	,cast(dxurdt as [INT]) as dxurdt
	,case when cast(dxurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dxurdt as [INT]) %1000, dateadd(year, cast(dxurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dxurdt_conv
	,cast(dxurab as [DECIMAL](38, 4)) as dxurab
	,cast(dxurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dxurab_conv
	,dxurrf as dxurrf
	,ltrim(rtrim(dxurrf)) as dxurrf_conv
	,dxuser as dxuser
	,ltrim(rtrim(dxuser)) as dxuser_conv
	,dxpid as dxpid
	,ltrim(rtrim(dxpid)) as dxpid_conv
	,dxjobn as dxjobn
	,ltrim(rtrim(dxjobn)) as dxjobn_conv
	,cast(dxupmj as [INT]) as dxupmj
	,case when cast(dxupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dxupmj as [INT]) %1000, dateadd(year, cast(dxupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dxupmj_conv
	,cast(dxtday as [DECIMAL](38, 4)) as dxtday
FROM 
    stg_jde_qat.tmp_f38112_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f38112_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f38112_sys_integration_id ON rep_jde_qat.f38112_new(sys_integration_id); 
