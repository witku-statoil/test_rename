-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4945_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4945_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4945_cdc
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
	,cast(scshpn as [DECIMAL](38, 4)) as scshpn
	,cast(scrssn as [DECIMAL](38, 4)) as scrssn
	,scvmcu as scvmcu
	,ltrim(rtrim(scvmcu)) as scvmcu_conv
	,cast(scldnm as [DECIMAL](38, 4)) as scldnm
	,cast(scdlno as [DECIMAL](38, 4)) as scdlno
	,cast(scoseq as [DECIMAL](38, 4)) as scoseq
	,cast(scoseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as scoseq_conv
	,cast(scrtnm as [NVARCHAR](10)) as scrtnm
	,cast(scsdsq as [DECIMAL](38, 4)) as scsdsq
	,cast(scsdsq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as scsdsq_conv
	,cast(scscsn as [DECIMAL](38, 4)) as scscsn
	,cast(scscsn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as scscsn_conv
	,cast(scnmfc as [NVARCHAR](4)) as scnmfc
	,cast(scdsgp as [NVARCHAR](3)) as scdsgp
	,cast(scfrt1 as [NVARCHAR](6)) as scfrt1
	,cast(scfrt2 as [NVARCHAR](6)) as scfrt2
	,cast(sccgc1 as [NVARCHAR](3)) as sccgc1
	,cast(scag as [DECIMAL](38, 4)) as scag
	,cast(scag as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as scag_conv
	,scblpb as scblpb
	,ltrim(rtrim(scblpb)) as scblpb_conv
	,sccrdc as sccrdc
	,ltrim(rtrim(sccrdc)) as sccrdc_conv
	,cast(scfaa as [DECIMAL](38, 4)) as scfaa
	,cast(scfaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as scfaa_conv
	,cast(scnamf as [DECIMAL](38, 4)) as scnamf
	,cast(scnamf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as scnamf_conv
	,cast(scrtdq as [DECIMAL](38, 4)) as scrtdq
	,cast(scrtdq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as scrtdq_conv
	,cast(scnamt as [DECIMAL](38, 4)) as scnamt
	,cast(scnamt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as scnamt_conv
	,cast(scuom as [NVARCHAR](2)) as scuom
	,cast(scrtgb as [NVARCHAR](1)) as scrtgb
	,sccrcd as sccrcd
	,ltrim(rtrim(sccrcd)) as sccrcd_conv
	,cast(scdoco as [DECIMAL](38, 4)) as scdoco
	,cast(scdcto as [NVARCHAR](2)) as scdcto
	,sckcoo as sckcoo
	,ltrim(rtrim(sckcoo)) as sckcoo_conv
	,cast(sclnid as [DECIMAL](38, 4)) as sclnid
	,cast(sclnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as sclnid_conv
	,cast(scovfg as [NVARCHAR](1)) as scovfg
	,cast(scukid as [DECIMAL](38, 4)) as scukid
	,cast(scuk01 as [DECIMAL](38, 4)) as scuk01
	,scurcd as scurcd
	,ltrim(rtrim(scurcd)) as scurcd_conv
	,cast(scurdt as [INT]) as scurdt
	,case when cast(scurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(scurdt as [INT]) %1000, dateadd(year, cast(scurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as scurdt_conv
	,cast(scurat as [DECIMAL](38, 4)) as scurat
	,cast(scurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as scurat_conv
	,cast(scurab as [DECIMAL](38, 4)) as scurab
	,cast(scurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as scurab_conv
	,scurrf as scurrf
	,ltrim(rtrim(scurrf)) as scurrf_conv
	,scuser as scuser
	,ltrim(rtrim(scuser)) as scuser_conv
	,scpid as scpid
	,ltrim(rtrim(scpid)) as scpid_conv
	,scjobn as scjobn
	,ltrim(rtrim(scjobn)) as scjobn_conv
	,cast(scupmj as [INT]) as scupmj
	,case when cast(scupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(scupmj as [INT]) %1000, dateadd(year, cast(scupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as scupmj_conv
	,cast(sctday as [DECIMAL](38, 4)) as sctday
	,cast(scrtn as [DECIMAL](38, 4)) as scrtn
	,cast(sclnmb as [DECIMAL](38, 4)) as sclnmb
	,scprte as scprte
	,ltrim(rtrim(scprte)) as scprte_conv
	,cast(sceftj as [INT]) as sceftj
	,case when cast(sceftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sceftj as [INT]) %1000, dateadd(year, cast(sceftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sceftj_conv
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
	,scshpn
	,scrssn
	,scvmcu
	,scldnm
	,scdlno
	,scoseq
	,scrtnm
	,scsdsq
	,scscsn
	,scnmfc
	,scdsgp
	,scfrt1
	,scfrt2
	,sccgc1
	,scag
	,scblpb
	,sccrdc
	,scfaa
	,scnamf
	,scrtdq
	,scnamt
	,scuom
	,scrtgb
	,sccrcd
	,scdoco
	,scdcto
	,sckcoo
	,sclnid
	,scovfg
	,scukid
	,scuk01
	,scurcd
	,scurdt
	,scurat
	,scurab
	,scurrf
	,scuser
	,scpid
	,scjobn
	,scupmj
	,sctday
	,scrtn
	,sclnmb
	,scprte
	,sceftj,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4945_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4945_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4945_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4945_cdc(sys_integration_id); 
