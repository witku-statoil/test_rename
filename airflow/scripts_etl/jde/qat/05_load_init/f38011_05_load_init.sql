-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f38011_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f38011_new


CREATE TABLE rep_jde_qat.f38011_new
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
	,dfdmct as dfdmct
	,ltrim(rtrim(dfdmct)) as dfdmct_conv
	,cast(dfdmcs as [DECIMAL](38, 4)) as dfdmcs
	,cast(dfcnqy as [NVARCHAR](1)) as dfcnqy
	,cast(dfdto as [NVARCHAR](1)) as dfdto
	,dfdes as dfdes
	,ltrim(rtrim(dfdes)) as dfdes_conv
	,cast(dfdesy as [NVARCHAR](2)) as dfdesy
	,cast(dfitm as [DECIMAL](38, 4)) as dfitm
	,cast(dfseq as [DECIMAL](38, 4)) as dfseq
	,cast(dfcnqt as [DECIMAL](38, 4)) as dfcnqt
	,cast(dfcnqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dfcnqt_conv
	,cast(dfaa as [DECIMAL](38, 4)) as dfaa
	,cast(dfaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dfaa_conv
	,cast(dfuom as [NVARCHAR](2)) as dfuom
	,cast(dfminq as [DECIMAL](38, 4)) as dfminq
	,cast(dfminq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dfminq_conv
	,cast(dfmaxq as [DECIMAL](38, 4)) as dfmaxq
	,cast(dfmaxq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dfmaxq_conv
	,cast(dfeftj as [INT]) as dfeftj
	,case when cast(dfeftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dfeftj as [INT]) %1000, dateadd(year, cast(dfeftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dfeftj_conv
	,cast(dfexdj as [INT]) as dfexdj
	,case when cast(dfexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dfexdj as [INT]) %1000, dateadd(year, cast(dfexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dfexdj_conv
	,cast(dfuprc as [DECIMAL](38, 4)) as dfuprc
	,cast(dfuprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dfuprc_conv
	,cast(dfasn as [NVARCHAR](8)) as dfasn
	,cast(dfpasn as [NVARCHAR](8)) as dfpasn
	,dfmcu as dfmcu
	,ltrim(rtrim(dfmcu)) as dfmcu_conv
	,dfcrcd as dfcrcd
	,ltrim(rtrim(dfcrcd)) as dfcrcd_conv
	,dftv01 as dftv01
	,ltrim(rtrim(dftv01)) as dftv01_conv
	,dftv02 as dftv02
	,ltrim(rtrim(dftv02)) as dftv02_conv
	,dftv03 as dftv03
	,ltrim(rtrim(dftv03)) as dftv03_conv
	,cast(dfqty1 as [DECIMAL](38, 4)) as dfqty1
	,cast(dfqty1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dfqty1_conv
	,cast(dftvum as [NVARCHAR](2)) as dftvum
	,dfurcd as dfurcd
	,ltrim(rtrim(dfurcd)) as dfurcd_conv
	,cast(dfurdt as [INT]) as dfurdt
	,case when cast(dfurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dfurdt as [INT]) %1000, dateadd(year, cast(dfurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dfurdt_conv
	,cast(dfurat as [DECIMAL](38, 4)) as dfurat
	,cast(dfurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dfurat_conv
	,cast(dfurab as [DECIMAL](38, 4)) as dfurab
	,cast(dfurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dfurab_conv
	,dfurrf as dfurrf
	,ltrim(rtrim(dfurrf)) as dfurrf_conv
	,dfuser as dfuser
	,ltrim(rtrim(dfuser)) as dfuser_conv
	,dfpid as dfpid
	,ltrim(rtrim(dfpid)) as dfpid_conv
	,dfjobn as dfjobn
	,ltrim(rtrim(dfjobn)) as dfjobn_conv
	,cast(dfupmj as [INT]) as dfupmj
	,case when cast(dfupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dfupmj as [INT]) %1000, dateadd(year, cast(dfupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dfupmj_conv
	,cast(dftday as [DECIMAL](38, 4)) as dftday
	,cast(dfrpqt as [DECIMAL](38, 4)) as dfrpqt
	,cast(dfrpqt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dfrpqt_conv
	,dfqedt as dfqedt
	,ltrim(rtrim(dfqedt)) as dfqedt_conv
	,dfqed3 as dfqed3
	,ltrim(rtrim(dfqed3)) as dfqed3_conv
	,dfqed2 as dfqed2
	,ltrim(rtrim(dfqed2)) as dfqed2_conv
	,cast(dfpdp5 as [NVARCHAR](3)) as dfpdp5
FROM 
    stg_jde_qat.tmp_f38011_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f38011_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f38011_sys_integration_id ON rep_jde_qat.f38011_new(sys_integration_id); 
