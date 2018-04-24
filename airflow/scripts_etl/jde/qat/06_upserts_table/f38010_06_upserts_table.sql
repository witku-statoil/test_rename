-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f38010_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f38010_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f38010_cdc
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
	,dndmct as dndmct
	,ltrim(rtrim(dndmct)) as dndmct_conv
	,cast(dndmcs as [DECIMAL](38, 4)) as dndmcs
	,dndl01 as dndl01
	,ltrim(rtrim(dndl01)) as dndl01_conv
	,dndl02 as dndl02
	,ltrim(rtrim(dndl02)) as dndl02_conv
	,dndl03 as dndl03
	,ltrim(rtrim(dndl03)) as dndl03_conv
	,dnmcu as dnmcu
	,ltrim(rtrim(dnmcu)) as dnmcu_conv
	,cast(dnnwrn as [NVARCHAR](1)) as dnnwrn
	,dnagrs as dnagrs
	,ltrim(rtrim(dnagrs)) as dnagrs_conv
	,cast(dncman as [DECIMAL](38, 4)) as dncman
	,cast(dncadm as [DECIMAL](38, 4)) as dncadm
	,dnpanm as dnpanm
	,ltrim(rtrim(dnpanm)) as dnpanm_conv
	,cast(dncntd as [INT]) as dncntd
	,case when cast(dncntd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dncntd as [INT]) %1000, dateadd(year, cast(dncntd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dncntd_conv
	,cast(dndmtc as [NVARCHAR](1)) as dndmtc
	,cast(dndmpc as [NVARCHAR](3)) as dndmpc
	,cast(dndmsc as [NVARCHAR](1)) as dndmsc
	,cast(dneftj as [INT]) as dneftj
	,case when cast(dneftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dneftj as [INT]) %1000, dateadd(year, cast(dneftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dneftj_conv
	,cast(dnexdj as [INT]) as dnexdj
	,case when cast(dnexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dnexdj as [INT]) %1000, dateadd(year, cast(dnexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dnexdj_conv
	,cast(dnan8 as [DECIMAL](38, 4)) as dnan8
	,dnaagm as dnaagm
	,ltrim(rtrim(dnaagm)) as dnaagm_conv
	,cast(dnaags as [DECIMAL](38, 4)) as dnaags
	,dnragm as dnragm
	,ltrim(rtrim(dnragm)) as dnragm_conv
	,cast(dnrags as [DECIMAL](38, 4)) as dnrags
	,dncagm as dncagm
	,ltrim(rtrim(dncagm)) as dncagm_conv
	,cast(dncags as [DECIMAL](38, 4)) as dncags
	,dnpagm as dnpagm
	,ltrim(rtrim(dnpagm)) as dnpagm_conv
	,cast(dnpags as [DECIMAL](38, 4)) as dnpags
	,cast(dnzd01 as [NVARCHAR](3)) as dnzd01
	,cast(dnzd02 as [NVARCHAR](3)) as dnzd02
	,cast(dnzd03 as [NVARCHAR](3)) as dnzd03
	,cast(dnzd04 as [NVARCHAR](3)) as dnzd04
	,cast(dnzd05 as [NVARCHAR](3)) as dnzd05
	,cast(dnzd06 as [NVARCHAR](3)) as dnzd06
	,cast(dnzd07 as [NVARCHAR](3)) as dnzd07
	,cast(dnzd08 as [NVARCHAR](3)) as dnzd08
	,cast(dnzd09 as [NVARCHAR](3)) as dnzd09
	,cast(dnzd10 as [NVARCHAR](3)) as dnzd10
	,dnqedt as dnqedt
	,ltrim(rtrim(dnqedt)) as dnqedt_conv
	,cast(dncand as [INT]) as dncand
	,case when cast(dncand as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dncand as [INT]) %1000, dateadd(year, cast(dncand as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dncand_conv
	,cast(dncanr as [NVARCHAR](3)) as dncanr
	,dnupgl as dnupgl
	,ltrim(rtrim(dnupgl)) as dnupgl_conv
	,cast(dnpras as [NVARCHAR](1)) as dnpras
	,dncmoo as dncmoo
	,ltrim(rtrim(dncmoo)) as dncmoo_conv
	,cast(dncnqy as [NVARCHAR](1)) as dncnqy
	,dntv01 as dntv01
	,ltrim(rtrim(dntv01)) as dntv01_conv
	,dntv02 as dntv02
	,ltrim(rtrim(dntv02)) as dntv02_conv
	,dntv03 as dntv03
	,ltrim(rtrim(dntv03)) as dntv03_conv
	,dnurcd as dnurcd
	,ltrim(rtrim(dnurcd)) as dnurcd_conv
	,cast(dnurdt as [INT]) as dnurdt
	,case when cast(dnurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dnurdt as [INT]) %1000, dateadd(year, cast(dnurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dnurdt_conv
	,cast(dnurat as [DECIMAL](38, 4)) as dnurat
	,cast(dnurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dnurat_conv
	,cast(dnurab as [DECIMAL](38, 4)) as dnurab
	,cast(dnurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dnurab_conv
	,dnurrf as dnurrf
	,ltrim(rtrim(dnurrf)) as dnurrf_conv
	,dnuser as dnuser
	,ltrim(rtrim(dnuser)) as dnuser_conv
	,dnpid as dnpid
	,ltrim(rtrim(dnpid)) as dnpid_conv
	,dnjobn as dnjobn
	,ltrim(rtrim(dnjobn)) as dnjobn_conv
	,cast(dnupmj as [INT]) as dnupmj
	,case when cast(dnupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dnupmj as [INT]) %1000, dateadd(year, cast(dnupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dnupmj_conv
	,cast(dntday as [DECIMAL](38, 4)) as dntday
	,cast(dnrpqt as [DECIMAL](38, 4)) as dnrpqt
	,cast(dnrpqt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dnrpqt_conv
	,dnqed3 as dnqed3
	,ltrim(rtrim(dnqed3)) as dnqed3_conv
	,dnqed2 as dnqed2
	,ltrim(rtrim(dnqed2)) as dnqed2_conv
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
	,dndmct
	,dndmcs
	,dndl01
	,dndl02
	,dndl03
	,dnmcu
	,dnnwrn
	,dnagrs
	,dncman
	,dncadm
	,dnpanm
	,dncntd
	,dndmtc
	,dndmpc
	,dndmsc
	,dneftj
	,dnexdj
	,dnan8
	,dnaagm
	,dnaags
	,dnragm
	,dnrags
	,dncagm
	,dncags
	,dnpagm
	,dnpags
	,dnzd01
	,dnzd02
	,dnzd03
	,dnzd04
	,dnzd05
	,dnzd06
	,dnzd07
	,dnzd08
	,dnzd09
	,dnzd10
	,dnqedt
	,dncand
	,dncanr
	,dnupgl
	,dnpras
	,dncmoo
	,dncnqy
	,dntv01
	,dntv02
	,dntv03
	,dnurcd
	,dnurdt
	,dnurat
	,dnurab
	,dnurrf
	,dnuser
	,dnpid
	,dnjobn
	,dnupmj
	,dntday
	,dnrpqt
	,dnqed3
	,dnqed2,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f38010_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f38010_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f38010_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f38010_cdc(sys_integration_id); 
