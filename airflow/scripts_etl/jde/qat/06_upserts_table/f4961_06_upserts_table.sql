-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4961_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4961_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4961_cdc
WITH 
(
    DISTRIBUTION = HASH(sys_integration_id) 
    ,HEAP
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
	,llvmcu as llvmcu
	,ltrim(rtrim(llvmcu)) as llvmcu_conv
	,cast(llldnm as [DECIMAL](38, 4)) as llldnm
	,cast(lltrpl as [DECIMAL](38, 4)) as lltrpl
	,cast(lltrpl as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as lltrpl_conv
	,cast(llorgn as [DECIMAL](38, 4)) as llorgn
	,llnmcu as llnmcu
	,ltrim(rtrim(llnmcu)) as llnmcu_conv
	,cast(llstsq as [DECIMAL](38, 4)) as llstsq
	,cast(llscwt as [DECIMAL](38, 4)) as llscwt
	,cast(llscwt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as llscwt_conv
	,cast(llwtum as [NVARCHAR](2)) as llwtum
	,cast(llscvl as [DECIMAL](38, 4)) as llscvl
	,cast(llscvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as llscvl_conv
	,cast(llvlum as [NVARCHAR](2)) as llvlum
	,cast(llsccu as [DECIMAL](38, 4)) as llsccu
	,cast(llsccu as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as llsccu_conv
	,cast(llcvum as [NVARCHAR](2)) as llcvum
	,cast(lllddt as [INT]) as lllddt
	,case when cast(lllddt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(lllddt as [INT]) %1000, dateadd(year, cast(lllddt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as lllddt_conv
	,cast(llldtm as [DECIMAL](38, 4)) as llldtm
	,cast(llload as [INT]) as llload
	,case when cast(llload as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(llload as [INT]) %1000, dateadd(year, cast(llload as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as llload_conv
	,cast(lltmls as [DECIMAL](38, 4)) as lltmls
	,cast(llppdj as [INT]) as llppdj
	,case when cast(llppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(llppdj as [INT]) %1000, dateadd(year, cast(llppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as llppdj_conv
	,cast(llpmdt as [DECIMAL](38, 4)) as llpmdt
	,cast(lladdj as [INT]) as lladdj
	,case when cast(lladdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(lladdj as [INT]) %1000, dateadd(year, cast(lladdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as lladdj_conv
	,cast(lladtm as [DECIMAL](38, 4)) as lladtm
	,lllrfg as lllrfg
	,ltrim(rtrim(lllrfg)) as lllrfg_conv
	,llpcsd as llpcsd
	,ltrim(rtrim(llpcsd)) as llpcsd_conv
	,cast(lldwnc as [DECIMAL](38, 4)) as lldwnc
	,llurcd as llurcd
	,ltrim(rtrim(llurcd)) as llurcd_conv
	,cast(llurdt as [INT]) as llurdt
	,case when cast(llurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(llurdt as [INT]) %1000, dateadd(year, cast(llurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as llurdt_conv
	,cast(llurat as [DECIMAL](38, 4)) as llurat
	,cast(llurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as llurat_conv
	,cast(llurab as [DECIMAL](38, 4)) as llurab
	,cast(llurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as llurab_conv
	,llurrf as llurrf
	,ltrim(rtrim(llurrf)) as llurrf_conv
	,lluser as lluser
	,ltrim(rtrim(lluser)) as lluser_conv
	,llpid as llpid
	,ltrim(rtrim(llpid)) as llpid_conv
	,lljobn as lljobn
	,ltrim(rtrim(lljobn)) as lljobn_conv
	,cast(llupmj as [INT]) as llupmj
	,case when cast(llupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(llupmj as [INT]) %1000, dateadd(year, cast(llupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as llupmj_conv
	,cast(lltday as [DECIMAL](38, 4)) as lltday
FROM (
    SELECT 
        	sys_file_name
	,sys_file_ln
	,sys_integration_id
	,sys_extract_dt
	,sys_cdc_dt
	,sys_cdc_scn
	,sys_cdc_operation_type
	,sys_cdc_before_after
	,sys_line_modified_ind
	,llvmcu
	,llldnm
	,lltrpl
	,llorgn
	,llnmcu
	,llstsq
	,llscwt
	,llwtum
	,llscvl
	,llvlum
	,llsccu
	,llcvum
	,lllddt
	,llldtm
	,llload
	,lltmls
	,llppdj
	,llpmdt
	,lladdj
	,lladtm
	,lllrfg
	,llpcsd
	,lldwnc
	,llurcd
	,llurdt
	,llurat
	,llurab
	,llurrf
	,lluser
	,llpid
	,lljobn
	,llupmj
	,lltday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4961_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4961_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4961_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4961_cdc(sys_integration_id); 
