-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f1620_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f1620_new


CREATE TABLE rep_jde_qat.f1620_new
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
	,ctabt1 as ctabt1
	,ltrim(rtrim(ctabt1)) as ctabt1_conv
	,ctdl01 as ctdl01
	,ltrim(rtrim(ctdl01)) as ctdl01_conv
	,cast(ctcmer as [NVARCHAR](1)) as ctcmer
	,ctfile as ctfile
	,ltrim(rtrim(ctfile)) as ctfile_conv
	,ctdtai as ctdtai
	,ltrim(rtrim(ctdtai)) as ctdtai_conv
	,cast(ctvalr as [NVARCHAR](2)) as ctvalr
	,ctcmvl as ctcmvl
	,ltrim(rtrim(ctcmvl)) as ctcmvl_conv
	,cast(ctsy as [NVARCHAR](4)) as ctsy
	,ctrt as ctrt
	,ltrim(rtrim(ctrt)) as ctrt_conv
	,ctuser as ctuser
	,ltrim(rtrim(ctuser)) as ctuser_conv
	,ctpid as ctpid
	,ltrim(rtrim(ctpid)) as ctpid_conv
	,cast(ctupmj as [INT]) as ctupmj
	,case when cast(ctupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ctupmj as [INT]) %1000, dateadd(year, cast(ctupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ctupmj_conv
	,cast(ctupmt as [DECIMAL](38, 4)) as ctupmt
	,ctjobn as ctjobn
	,ltrim(rtrim(ctjobn)) as ctjobn_conv
FROM 
    stg_jde_qat.tmp_f1620_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f1620_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f1620_sys_integration_id ON rep_jde_qat.f1620_new(sys_integration_id); 
