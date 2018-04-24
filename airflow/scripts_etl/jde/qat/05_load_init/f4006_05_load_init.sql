-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4006_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4006_new


CREATE TABLE rep_jde_qat.f4006_new
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
	,cast(oadoco as [DECIMAL](38, 4)) as oadoco
	,cast(oadcto as [NVARCHAR](2)) as oadcto
	,oakcoo as oakcoo
	,ltrim(rtrim(oakcoo)) as oakcoo_conv
	,cast(oaanty as [NVARCHAR](1)) as oaanty
	,oamlnm as oamlnm
	,ltrim(rtrim(oamlnm)) as oamlnm_conv
	,oaadd1 as oaadd1
	,ltrim(rtrim(oaadd1)) as oaadd1_conv
	,oaadd2 as oaadd2
	,ltrim(rtrim(oaadd2)) as oaadd2_conv
	,oaadd3 as oaadd3
	,ltrim(rtrim(oaadd3)) as oaadd3_conv
	,oaadd4 as oaadd4
	,ltrim(rtrim(oaadd4)) as oaadd4_conv
	,oaaddz as oaaddz
	,ltrim(rtrim(oaaddz)) as oaaddz_conv
	,oacty1 as oacty1
	,ltrim(rtrim(oacty1)) as oacty1_conv
	,cast(oacoun as [NVARCHAR](25)) as oacoun
	,cast(oaadds as [NVARCHAR](3)) as oaadds
	,oacrte as oacrte
	,ltrim(rtrim(oacrte)) as oacrte_conv
	,oabkml as oabkml
	,ltrim(rtrim(oabkml)) as oabkml_conv
	,cast(oactr as [NVARCHAR](3)) as oactr
	,oauser as oauser
	,ltrim(rtrim(oauser)) as oauser_conv
	,oapid as oapid
	,ltrim(rtrim(oapid)) as oapid_conv
	,cast(oaupmj as [INT]) as oaupmj
	,case when cast(oaupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(oaupmj as [INT]) %1000, dateadd(year, cast(oaupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as oaupmj_conv
	,oajobn as oajobn
	,ltrim(rtrim(oajobn)) as oajobn_conv
	,cast(oaupmt as [DECIMAL](38, 4)) as oaupmt
	,cast(oalnid as [DECIMAL](38, 4)) as oalnid
	,cast(oalnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as oalnid_conv
FROM 
    stg_jde_qat.tmp_f4006_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4006_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4006_sys_integration_id ON rep_jde_qat.f4006_new(sys_integration_id); 
