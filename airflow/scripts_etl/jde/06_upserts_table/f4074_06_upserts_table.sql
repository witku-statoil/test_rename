-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4074_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4074_cdc


CREATE TABLE stg_jde.tmp_upsert_f4074_cdc
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
	,cast(aldoco as [DECIMAL](38, 4)) as aldoco
	,cast(aldcto as [NVARCHAR](2)) as aldcto
	,alkcoo as alkcoo
	,ltrim(rtrim(alkcoo)) as alkcoo_conv
	,alsfxo as alsfxo
	,ltrim(rtrim(alsfxo)) as alsfxo_conv
	,cast(allnid as [DECIMAL](38, 4)) as allnid
	,cast(allnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as allnid_conv
	,cast(alakid as [DECIMAL](38, 4)) as alakid
	,cast(alsrcfd as [NVARCHAR](2)) as alsrcfd
	,cast(aloseq as [DECIMAL](38, 4)) as aloseq
	,cast(aloseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aloseq_conv
	,cast(alsubseq as [DECIMAL](38, 4)) as alsubseq
	,cast(alsubseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alsubseq_conv
	,cast(altier as [DECIMAL](38, 4)) as altier
	,cast(alasn as [NVARCHAR](8)) as alasn
	,cast(alast as [NVARCHAR](8)) as alast
	,cast(alitm as [DECIMAL](38, 4)) as alitm
	,cast(alan8 as [DECIMAL](38, 4)) as alan8
	,alcrcd as alcrcd
	,ltrim(rtrim(alcrcd)) as alcrcd_conv
	,cast(aluom as [NVARCHAR](2)) as aluom
	,cast(almnq as [DECIMAL](38, 4)) as almnq
	,cast(almnq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as almnq_conv
	,cast(alledg as [NVARCHAR](2)) as alledg
	,cast(alfrmn as [NVARCHAR](10)) as alfrmn
	,cast(albscd as [NVARCHAR](1)) as albscd
	,cast(alfvtr as [DECIMAL](38, 4)) as alfvtr
	,cast(alfvtr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as alfvtr_conv
	,alabas as alabas
	,ltrim(rtrim(alabas)) as alabas_conv
	,cast(aluprc as [DECIMAL](38, 4)) as aluprc
	,cast(aluprc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as aluprc_conv
	,cast(alfup as [DECIMAL](38, 4)) as alfup
	,cast(alfup as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as alfup_conv
	,alglc as alglc
	,ltrim(rtrim(alglc)) as alglc_conv
	,cast(alarsn as [NVARCHAR](3)) as alarsn
	,cast(alacnt as [NVARCHAR](1)) as alacnt
	,cast(alsbif as [NVARCHAR](1)) as alsbif
	,almded as almded
	,ltrim(rtrim(almded)) as almded_conv
	,alprov as alprov
	,ltrim(rtrim(alprov)) as alprov_conv
	,cast(alatid as [DECIMAL](38, 4)) as alatid
	,cast(aligid as [DECIMAL](38, 4)) as aligid
	,cast(aligid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aligid_conv
	,cast(alcgid as [DECIMAL](38, 4)) as alcgid
	,cast(alcgid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alcgid_conv
	,cast(alogid as [DECIMAL](38, 4)) as alogid
	,cast(alogid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alogid_conv
	,cast(alanps as [DECIMAL](38, 4)) as alanps
	,cast(alanps as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alanps_conv
	,cast(albsdval as [DECIMAL](38, 4)) as albsdval
	,cast(albsdval as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as albsdval_conv
	,alsrflag as alsrflag
	,ltrim(rtrim(alsrflag)) as alsrflag_conv
	,aladjcal as aladjcal
	,ltrim(rtrim(aladjcal)) as aladjcal_conv
	,cast(alnbrord as [DECIMAL](38, 4)) as alnbrord
	,cast(alnbrord as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alnbrord_conv
	,cast(aluomvid as [NVARCHAR](2)) as aluomvid
	,cast(alolvl as [NVARCHAR](1)) as alolvl
	,cast(aladjsts as [NVARCHAR](1)) as aladjsts
	,aladjref as aladjref
	,ltrim(rtrim(aladjref)) as aladjref_conv
	,cast(alaccan8 as [DECIMAL](38, 4)) as alaccan8
	,cast(albnad as [DECIMAL](38, 4)) as albnad
	,cast(aladjgrp as [NVARCHAR](10)) as aladjgrp
	,almeadj as almeadj
	,ltrim(rtrim(almeadj)) as almeadj_conv
	,cast(alfvum as [NVARCHAR](2)) as alfvum
	,cast(alpdcl as [NVARCHAR](1)) as alpdcl
	,cast(alcfgid as [DECIMAL](38, 4)) as alcfgid
	,cast(alcfgid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alcfgid_conv
	,cast(alcfgcid as [DECIMAL](38, 4)) as alcfgcid
	,cast(alcfgcid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alcfgcid_conv
	,cast(alaprp1 as [NVARCHAR](3)) as alaprp1
	,cast(alaprp2 as [NVARCHAR](3)) as alaprp2
	,cast(alaprp3 as [NVARCHAR](3)) as alaprp3
	,cast(alaprp4 as [NVARCHAR](6)) as alaprp4
	,cast(alaprp5 as [NVARCHAR](6)) as alaprp5
	,cast(alaprp6 as [NVARCHAR](6)) as alaprp6
	,alndpi as alndpi
	,ltrim(rtrim(alndpi)) as alndpi_conv
	,aluser as aluser
	,ltrim(rtrim(aluser)) as aluser_conv
	,alpid as alpid
	,ltrim(rtrim(alpid)) as alpid_conv
	,aljobn as aljobn
	,ltrim(rtrim(aljobn)) as aljobn_conv
	,cast(alupmj as [INT]) as alupmj
	,case when cast(alupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(alupmj as [INT]) %1000, dateadd(year, cast(alupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as alupmj_conv
	,cast(altday as [DECIMAL](38, 4)) as altday
	,alpmtn as alpmtn
	,ltrim(rtrim(alpmtn)) as alpmtn_conv
	,alrulename as alrulename
	,ltrim(rtrim(alrulename)) as alrulename_conv
	,cast(alpa04 as [NVARCHAR](1)) as alpa04
	,aladjqty as aladjqty
	,ltrim(rtrim(aladjqty)) as aladjqty_conv
	,cast(alqtypy as [DECIMAL](38, 4)) as alqtypy
	,cast(alqtypy as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as alqtypy_conv
	,alstprcf as alstprcf
	,ltrim(rtrim(alstprcf)) as alstprcf_conv
	,altstrsnm as altstrsnm
	,ltrim(rtrim(altstrsnm)) as altstrsnm_conv
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
	,aldoco
	,aldcto
	,alkcoo
	,alsfxo
	,allnid
	,alakid
	,alsrcfd
	,aloseq
	,alsubseq
	,altier
	,alasn
	,alast
	,alitm
	,alan8
	,alcrcd
	,aluom
	,almnq
	,alledg
	,alfrmn
	,albscd
	,alfvtr
	,alabas
	,aluprc
	,alfup
	,alglc
	,alarsn
	,alacnt
	,alsbif
	,almded
	,alprov
	,alatid
	,aligid
	,alcgid
	,alogid
	,alanps
	,albsdval
	,alsrflag
	,aladjcal
	,alnbrord
	,aluomvid
	,alolvl
	,aladjsts
	,aladjref
	,alaccan8
	,albnad
	,aladjgrp
	,almeadj
	,alfvum
	,alpdcl
	,alcfgid
	,alcfgcid
	,alaprp1
	,alaprp2
	,alaprp3
	,alaprp4
	,alaprp5
	,alaprp6
	,alndpi
	,aluser
	,alpid
	,aljobn
	,alupmj
	,altday
	,alpmtn
	,alrulename
	,alpa04
	,aladjqty
	,alqtypy
	,alstprcf
	,altstrsnm,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4074_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4074_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4074_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4074_cdc(sys_integration_id); 
