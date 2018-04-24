-- Create rep new table for init
IF OBJECT_ID('rep_jde.f9210_new') IS NOT NULL
    DROP TABLE rep_jde.f9210_new


CREATE TABLE rep_jde.f9210_new
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
	,frdtai as frdtai
	,ltrim(rtrim(frdtai)) as frdtai_conv
	,frclas as frclas
	,ltrim(rtrim(frclas)) as frclas_conv
	,cast(frdtat as [NVARCHAR](1)) as frdtat
	,cast(frdtas as [DECIMAL](38, 4)) as frdtas
	,cast(frdtad as [DECIMAL](38, 4)) as frdtad
	,frpdta as frpdta
	,ltrim(rtrim(frpdta)) as frpdta_conv
	,cast(frarrn as [DECIMAL](38, 4)) as frarrn
	,frdval as frdval
	,ltrim(rtrim(frdval)) as frdval_conv
	,cast(frlr as [NVARCHAR](1)) as frlr
	,frcdec as frcdec
	,ltrim(rtrim(frcdec)) as frcdec_conv
	,cast(frdrul as [NVARCHAR](6)) as frdrul
	,frdro1 as frdro1
	,ltrim(rtrim(frdro1)) as frdro1_conv
	,cast(frerul as [NVARCHAR](6)) as frerul
	,frero1 as frero1
	,ltrim(rtrim(frero1)) as frero1_conv
	,frero2 as frero2
	,ltrim(rtrim(frero2)) as frero2_conv
	,frhlp1 as frhlp1
	,ltrim(rtrim(frhlp1)) as frhlp1_conv
	,frhlp2 as frhlp2
	,ltrim(rtrim(frhlp2)) as frhlp2_conv
	,cast(frnnix as [DECIMAL](38, 4)) as frnnix
	,cast(frnsy as [NVARCHAR](4)) as frnsy
	,cast(frrls as [NVARCHAR](10)) as frrls
	,fruser as fruser
	,ltrim(rtrim(fruser)) as fruser_conv
	,cast(frupmj as [INT]) as frupmj
	,case when cast(frupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(frupmj as [INT]) %1000, dateadd(year, cast(frupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as frupmj_conv
	,frpid as frpid
	,ltrim(rtrim(frpid)) as frpid_conv
	,frjobn as frjobn
	,ltrim(rtrim(frjobn)) as frjobn_conv
	,cast(frupmt as [DECIMAL](38, 4)) as frupmt
	,frowdi as frowdi
	,ltrim(rtrim(frowdi)) as frowdi_conv
	,cast(frowtp as [NVARCHAR](2)) as frowtp
	,frcntt as frcntt
	,ltrim(rtrim(frcntt)) as frcntt_conv
	,frscfg as frscfg
	,ltrim(rtrim(frscfg)) as frscfg_conv
	,fruper as fruper
	,ltrim(rtrim(fruper)) as fruper_conv
	,fralbk as fralbk
	,ltrim(rtrim(fralbk)) as fralbk_conv
	,cast(frower as [NVARCHAR](6)) as frower
	,froer1 as froer1
	,ltrim(rtrim(froer1)) as froer1_conv
	,froer2 as froer2
	,ltrim(rtrim(froer2)) as froer2_conv
	,cast(frowdr as [NVARCHAR](6)) as frowdr
	,frodr1 as frodr1
	,ltrim(rtrim(frodr1)) as frodr1_conv
	,frdbid as frdbid
	,ltrim(rtrim(frdbid)) as frdbid_conv
	,frbfdn as frbfdn
	,ltrim(rtrim(frbfdn)) as frbfdn_conv
	,frebid as frebid
	,ltrim(rtrim(frebid)) as frebid_conv
	,frbfen as frbfen
	,ltrim(rtrim(frbfen)) as frbfen_conv
	,frsfid as frsfid
	,ltrim(rtrim(frsfid)) as frsfid_conv
	,frsfmn as frsfmn
	,ltrim(rtrim(frsfmn)) as frsfmn_conv
	,frbvid as frbvid
	,ltrim(rtrim(frbvid)) as frbvid_conv
	,frbvnm as frbvnm
	,ltrim(rtrim(frbvnm)) as frbvnm_conv
	,frplfg as frplfg
	,ltrim(rtrim(frplfg)) as frplfg_conv
	,frddid as frddid
	,ltrim(rtrim(frddid)) as frddid_conv
	,frauin as frauin
	,ltrim(rtrim(frauin)) as frauin_conv
FROM 
    stg_jde.tmp_f9210_init
OPTION ( LABEL = 'CREATE_rep_jde.f9210_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f9210_sys_integration_id ON rep_jde.f9210_new(sys_integration_id); 
