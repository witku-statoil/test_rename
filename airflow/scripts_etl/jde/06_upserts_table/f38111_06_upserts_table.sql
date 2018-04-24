-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f38111_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f38111_cdc


CREATE TABLE stg_jde.tmp_upsert_f38111_cdc
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
	,dzdmct as dzdmct
	,ltrim(rtrim(dzdmct)) as dzdmct_conv
	,cast(dzdmcs as [DECIMAL](38, 4)) as dzdmcs
	,cast(dzdmtc as [NVARCHAR](1)) as dzdmtc
	,cast(dzan8 as [DECIMAL](38, 4)) as dzan8
	,cast(dzan8r as [DECIMAL](38, 4)) as dzan8r
	,cast(dzitm as [DECIMAL](38, 4)) as dzitm
	,dzmcu as dzmcu
	,ltrim(rtrim(dzmcu)) as dzmcu_conv
	,dzco as dzco
	,ltrim(rtrim(dzco)) as dzco_conv
	,dzlocn as dzlocn
	,ltrim(rtrim(dzlocn)) as dzlocn_conv
	,dzlot as dzlot
	,ltrim(rtrim(dzlot)) as dzlot_conv
	,cast(dztcaj as [NVARCHAR](1)) as dztcaj
	,cast(dztrdj as [INT]) as dztrdj
	,case when cast(dztrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dztrdj as [INT]) %1000, dateadd(year, cast(dztrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dztrdj_conv
	,cast(dzadjj as [INT]) as dzadjj
	,case when cast(dzadjj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dzadjj as [INT]) %1000, dateadd(year, cast(dzadjj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dzadjj_conv
	,cast(dzdct as [NVARCHAR](2)) as dzdct
	,cast(dzdoc as [DECIMAL](38, 4)) as dzdoc
	,dzkco as dzkco
	,ltrim(rtrim(dzkco)) as dzkco_conv
	,cast(dzjeln as [DECIMAL](38, 4)) as dzjeln
	,cast(dzdoco as [DECIMAL](38, 4)) as dzdoco
	,cast(dzdcto as [NVARCHAR](2)) as dzdcto
	,dzkcoo as dzkcoo
	,ltrim(rtrim(dzkcoo)) as dzkcoo_conv
	,cast(dzlnid as [DECIMAL](38, 4)) as dzlnid
	,cast(dzlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as dzlnid_conv
	,dzvehi as dzvehi
	,ltrim(rtrim(dzvehi)) as dzvehi_conv
	,dzvmcu as dzvmcu
	,ltrim(rtrim(dzvmcu)) as dzvmcu_conv
	,cast(dztrp as [DECIMAL](38, 4)) as dztrp
	,cast(dzcmpn as [DECIMAL](38, 4)) as dzcmpn
	,cast(dzcmpn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dzcmpn_conv
	,cast(dzdto as [NVARCHAR](1)) as dzdto
	,dzdes as dzdes
	,ltrim(rtrim(dzdes)) as dzdes_conv
	,cast(dzdesy as [NVARCHAR](2)) as dzdesy
	,dzpsr as dzpsr
	,ltrim(rtrim(dzpsr)) as dzpsr_conv
	,cast(dzpsry as [NVARCHAR](2)) as dzpsry
	,cast(dzseq as [DECIMAL](38, 4)) as dzseq
	,dztrex as dztrex
	,ltrim(rtrim(dztrex)) as dztrex_conv
	,cast(dztrqt as [DECIMAL](38, 4)) as dztrqt
	,cast(dztrqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dztrqt_conv
	,cast(dzuom as [NVARCHAR](2)) as dzuom
	,cast(dztemp as [DECIMAL](38, 4)) as dztemp
	,cast(dztemp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dztemp_conv
	,cast(dzstpu as [NVARCHAR](1)) as dzstpu
	,cast(dzdnty as [DECIMAL](38, 4)) as dzdnty
	,cast(dzdnty as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dzdnty_conv
	,cast(dzdntp as [NVARCHAR](1)) as dzdntp
	,cast(dzdetp as [DECIMAL](38, 4)) as dzdetp
	,cast(dzdetp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dzdetp_conv
	,cast(dzdtpu as [NVARCHAR](1)) as dzdtpu
	,cast(dzambr as [DECIMAL](38, 4)) as dzambr
	,cast(dzambr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dzambr_conv
	,cast(dzambu as [NVARCHAR](2)) as dzambu
	,cast(dzvcf as [DECIMAL](38, 4)) as dzvcf
	,cast(dzvcf as [DECIMAL](38, 5)) / cast(100000 as [DECIMAL](38, 5)) as dzvcf_conv
	,cast(dzstok as [DECIMAL](38, 4)) as dzstok
	,cast(dzstok as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dzstok_conv
	,cast(dzbum4 as [NVARCHAR](2)) as dzbum4
	,cast(dzwgtr as [DECIMAL](38, 4)) as dzwgtr
	,cast(dzwgtr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dzwgtr_conv
	,cast(dzbum5 as [NVARCHAR](2)) as dzbum5
	,cast(dzstum as [DECIMAL](38, 4)) as dzstum
	,cast(dzstum as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dzstum_conv
	,cast(dzbum6 as [NVARCHAR](2)) as dzbum6
	,cast(dzpras as [NVARCHAR](1)) as dzpras
	,cast(dzaa as [DECIMAL](38, 4)) as dzaa
	,cast(dzaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dzaa_conv
	,dzcrcd as dzcrcd
	,ltrim(rtrim(dzcrcd)) as dzcrcd_conv
	,cast(dzuncs as [DECIMAL](38, 4)) as dzuncs
	,cast(dzuncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dzuncs_conv
	,cast(dzpaid as [DECIMAL](38, 4)) as dzpaid
	,cast(dzpaid as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dzpaid_conv
	,cast(dzdgl as [INT]) as dzdgl
	,case when cast(dzdgl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dzdgl as [INT]) %1000, dateadd(year, cast(dzdgl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dzdgl_conv
	,cast(dzicu as [DECIMAL](38, 4)) as dzicu
	,cast(dzicut as [NVARCHAR](2)) as dzicut
	,cast(dzglpt as [NVARCHAR](4)) as dzglpt
	,dzadzr as dzadzr
	,ltrim(rtrim(dzadzr)) as dzadzr_conv
	,cast(dzrcd as [NVARCHAR](3)) as dzrcd
	,cast(dzrecs as [NVARCHAR](1)) as dzrecs
	,cast(dzcrdj as [INT]) as dzcrdj
	,case when cast(dzcrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dzcrdj as [INT]) %1000, dateadd(year, cast(dzcrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dzcrdj_conv
	,cast(dzukid as [DECIMAL](38, 4)) as dzukid
	,dzupgl as dzupgl
	,ltrim(rtrim(dzupgl)) as dzupgl_conv
	,dzsvpf as dzsvpf
	,ltrim(rtrim(dzsvpf)) as dzsvpf_conv
	,cast(dzsvpd as [INT]) as dzsvpd
	,case when cast(dzsvpd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dzsvpd as [INT]) %1000, dateadd(year, cast(dzsvpd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dzsvpd_conv
	,cast(dzprtf as [NVARCHAR](1)) as dzprtf
	,dzcrpo as dzcrpo
	,ltrim(rtrim(dzcrpo)) as dzcrpo_conv
	,dzcrso as dzcrso
	,ltrim(rtrim(dzcrso)) as dzcrso_conv
	,dzcdmc as dzcdmc
	,ltrim(rtrim(dzcdmc)) as dzcdmc_conv
	,cast(dzcand as [INT]) as dzcand
	,case when cast(dzcand as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dzcand as [INT]) %1000, dateadd(year, cast(dzcand as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dzcand_conv
	,cast(dzcanr as [NVARCHAR](3)) as dzcanr
	,dztv01 as dztv01
	,ltrim(rtrim(dztv01)) as dztv01_conv
	,dztv02 as dztv02
	,ltrim(rtrim(dztv02)) as dztv02_conv
	,dztv03 as dztv03
	,ltrim(rtrim(dztv03)) as dztv03_conv
	,dzurcd as dzurcd
	,ltrim(rtrim(dzurcd)) as dzurcd_conv
	,cast(dzurdt as [INT]) as dzurdt
	,case when cast(dzurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dzurdt as [INT]) %1000, dateadd(year, cast(dzurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dzurdt_conv
	,cast(dzurat as [DECIMAL](38, 4)) as dzurat
	,cast(dzurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dzurat_conv
	,cast(dzurab as [DECIMAL](38, 4)) as dzurab
	,cast(dzurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dzurab_conv
	,dzurrf as dzurrf
	,ltrim(rtrim(dzurrf)) as dzurrf_conv
	,dzuser as dzuser
	,ltrim(rtrim(dzuser)) as dzuser_conv
	,dzpid as dzpid
	,ltrim(rtrim(dzpid)) as dzpid_conv
	,dzjobn as dzjobn
	,ltrim(rtrim(dzjobn)) as dzjobn_conv
	,cast(dzupmj as [INT]) as dzupmj
	,case when cast(dzupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dzupmj as [INT]) %1000, dateadd(year, cast(dzupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dzupmj_conv
	,cast(dztday as [DECIMAL](38, 4)) as dztday
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
	,dzdmct
	,dzdmcs
	,dzdmtc
	,dzan8
	,dzan8r
	,dzitm
	,dzmcu
	,dzco
	,dzlocn
	,dzlot
	,dztcaj
	,dztrdj
	,dzadjj
	,dzdct
	,dzdoc
	,dzkco
	,dzjeln
	,dzdoco
	,dzdcto
	,dzkcoo
	,dzlnid
	,dzvehi
	,dzvmcu
	,dztrp
	,dzcmpn
	,dzdto
	,dzdes
	,dzdesy
	,dzpsr
	,dzpsry
	,dzseq
	,dztrex
	,dztrqt
	,dzuom
	,dztemp
	,dzstpu
	,dzdnty
	,dzdntp
	,dzdetp
	,dzdtpu
	,dzambr
	,dzambu
	,dzvcf
	,dzstok
	,dzbum4
	,dzwgtr
	,dzbum5
	,dzstum
	,dzbum6
	,dzpras
	,dzaa
	,dzcrcd
	,dzuncs
	,dzpaid
	,dzdgl
	,dzicu
	,dzicut
	,dzglpt
	,dzadzr
	,dzrcd
	,dzrecs
	,dzcrdj
	,dzukid
	,dzupgl
	,dzsvpf
	,dzsvpd
	,dzprtf
	,dzcrpo
	,dzcrso
	,dzcdmc
	,dzcand
	,dzcanr
	,dztv01
	,dztv02
	,dztv03
	,dzurcd
	,dzurdt
	,dzurat
	,dzurab
	,dzurrf
	,dzuser
	,dzpid
	,dzjobn
	,dzupmj
	,dztday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f38111_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f38111_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f38111_cdc_sys_integration_id ON stg_jde.tmp_upsert_f38111_cdc(sys_integration_id); 
