-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4930_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4930_new


CREATE TABLE rep_jde_qat.f4930_new
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
	,vmvehi as vmvehi
	,ltrim(rtrim(vmvehi)) as vmvehi_conv
	,vmvehs as vmvehs
	,ltrim(rtrim(vmvehs)) as vmvehs_conv
	,vmdl01 as vmdl01
	,ltrim(rtrim(vmdl01)) as vmdl01_conv
	,vmlrfg as vmlrfg
	,ltrim(rtrim(vmlrfg)) as vmlrfg_conv
	,vmmcu as vmmcu
	,ltrim(rtrim(vmmcu)) as vmmcu_conv
	,vmvtyp as vmvtyp
	,ltrim(rtrim(vmvtyp)) as vmvtyp_conv
	,vmdumv as vmdumv
	,ltrim(rtrim(vmdumv)) as vmdumv_conv
	,cast(vmvown as [DECIMAL](38, 4)) as vmvown
	,cast(vmnce as [DECIMAL](38, 4)) as vmnce
	,cast(vmnce as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as vmnce_conv
	,cast(vmwtem as [DECIMAL](38, 4)) as vmwtem
	,cast(vmwtem as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vmwtem_conv
	,cast(vmwtca as [DECIMAL](38, 4)) as vmwtca
	,cast(vmwtca as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vmwtca_conv
	,cast(vmwtum as [NVARCHAR](2)) as vmwtum
	,cast(vmvlcp as [DECIMAL](38, 4)) as vmvlcp
	,cast(vmvlcp as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vmvlcp_conv
	,cast(vmvlcs as [DECIMAL](38, 4)) as vmvlcs
	,cast(vmvlcs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vmvlcs_conv
	,cast(vmvlum as [NVARCHAR](2)) as vmvlum
	,cast(vmcvol as [DECIMAL](38, 4)) as vmcvol
	,cast(vmcvol as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vmcvol_conv
	,cast(vmcvum as [NVARCHAR](2)) as vmcvum
	,cast(vmlcnt as [DECIMAL](38, 4)) as vmlcnt
	,cast(vmmxml as [DECIMAL](38, 4)) as vmmxml
	,cast(vmumd1 as [NVARCHAR](2)) as vmumd1
	,cast(vminmg as [NVARCHAR](10)) as vminmg
	,cast(vmmest as [NVARCHAR](1)) as vmmest
	,cast(vmvehn as [DECIMAL](38, 4)) as vmvehn
	,cast(vmvehn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as vmvehn_conv
	,vmurcd as vmurcd
	,ltrim(rtrim(vmurcd)) as vmurcd_conv
	,cast(vmurdt as [INT]) as vmurdt
	,case when cast(vmurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vmurdt as [INT]) %1000, dateadd(year, cast(vmurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vmurdt_conv
	,cast(vmurat as [DECIMAL](38, 4)) as vmurat
	,cast(vmurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vmurat_conv
	,cast(vmurab as [DECIMAL](38, 4)) as vmurab
	,cast(vmurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as vmurab_conv
	,vmurrf as vmurrf
	,ltrim(rtrim(vmurrf)) as vmurrf_conv
	,vmuser as vmuser
	,ltrim(rtrim(vmuser)) as vmuser_conv
	,vmpid as vmpid
	,ltrim(rtrim(vmpid)) as vmpid_conv
	,vmjobn as vmjobn
	,ltrim(rtrim(vmjobn)) as vmjobn_conv
	,cast(vmupmj as [INT]) as vmupmj
	,case when cast(vmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vmupmj as [INT]) %1000, dateadd(year, cast(vmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vmupmj_conv
	,cast(vmtday as [DECIMAL](38, 4)) as vmtday
FROM 
    stg_jde_qat.tmp_f4930_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4930_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4930_sys_integration_id ON rep_jde_qat.f4930_new(sys_integration_id); 
