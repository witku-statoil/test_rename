-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f3412_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f3412_new


CREATE TABLE rep_jde_qat.f3412_new
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
	,cast(mwitm as [DECIMAL](38, 4)) as mwitm
	,mwmcu as mwmcu
	,ltrim(rtrim(mwmcu)) as mwmcu_conv
	,cast(mwdrqj as [INT]) as mwdrqj
	,case when cast(mwdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mwdrqj as [INT]) %1000, dateadd(year, cast(mwdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mwdrqj_conv
	,cast(mwlovd as [DECIMAL](38, 4)) as mwlovd
	,cast(mwlovd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mwlovd_conv
	,cast(mwkit as [DECIMAL](38, 4)) as mwkit
	,mwmmcu as mwmmcu
	,ltrim(rtrim(mwmmcu)) as mwmmcu_conv
	,cast(mwuorg as [DECIMAL](38, 4)) as mwuorg
	,cast(mwuorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mwuorg_conv
	,cast(mwdoco as [DECIMAL](38, 4)) as mwdoco
	,cast(mwdcto as [NVARCHAR](2)) as mwdcto
	,mwrkco as mwrkco
	,ltrim(rtrim(mwrkco)) as mwrkco_conv
	,mwrorn as mwrorn
	,ltrim(rtrim(mwrorn)) as mwrorn_conv
	,cast(mwrcto as [NVARCHAR](2)) as mwrcto
	,cast(mwrlln as [DECIMAL](38, 4)) as mwrlln
	,cast(mwrlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as mwrlln_conv
	,cast(mwukid as [DECIMAL](38, 4)) as mwukid
	,cast(mwplnk as [DECIMAL](38, 4)) as mwplnk
	,cast(mwprjm as [DECIMAL](38, 4)) as mwprjm
	,cast(mwprjm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mwprjm_conv
	,cast(mwsrdm as [DECIMAL](38, 4)) as mwsrdm
	,cast(mwsrdm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mwsrdm_conv
	,cast(mwpns as [DECIMAL](38, 4)) as mwpns
FROM 
    stg_jde_qat.tmp_f3412_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f3412_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f3412_sys_integration_id ON rep_jde_qat.f3412_new(sys_integration_id); 
