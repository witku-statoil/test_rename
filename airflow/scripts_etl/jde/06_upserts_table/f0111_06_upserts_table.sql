-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0111_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0111_cdc


CREATE TABLE stg_jde.tmp_upsert_f0111_cdc
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
	,cast(wwan8 as [DECIMAL](38, 4)) as wwan8
	,cast(wwidln as [DECIMAL](38, 4)) as wwidln
	,cast(wwidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wwidln_conv
	,cast(wwdss5 as [DECIMAL](38, 4)) as wwdss5
	,wwmlnm as wwmlnm
	,ltrim(rtrim(wwmlnm)) as wwmlnm_conv
	,wwattl as wwattl
	,ltrim(rtrim(wwattl)) as wwattl_conv
	,wwrem1 as wwrem1
	,ltrim(rtrim(wwrem1)) as wwrem1_conv
	,wwslnm as wwslnm
	,ltrim(rtrim(wwslnm)) as wwslnm_conv
	,wwalph as wwalph
	,ltrim(rtrim(wwalph)) as wwalph_conv
	,wwdc as wwdc
	,ltrim(rtrim(wwdc)) as wwdc_conv
	,wwgnnm as wwgnnm
	,ltrim(rtrim(wwgnnm)) as wwgnnm_conv
	,wwmdnm as wwmdnm
	,ltrim(rtrim(wwmdnm)) as wwmdnm_conv
	,wwsrnm as wwsrnm
	,ltrim(rtrim(wwsrnm)) as wwsrnm_conv
	,cast(wwtyc as [NVARCHAR](1)) as wwtyc
	,cast(www001 as [NVARCHAR](3)) as www001
	,cast(www002 as [NVARCHAR](3)) as www002
	,cast(www003 as [NVARCHAR](3)) as www003
	,cast(www004 as [NVARCHAR](3)) as www004
	,cast(www005 as [NVARCHAR](3)) as www005
	,cast(www006 as [NVARCHAR](3)) as www006
	,cast(www007 as [NVARCHAR](3)) as www007
	,cast(www008 as [NVARCHAR](3)) as www008
	,cast(www009 as [NVARCHAR](3)) as www009
	,cast(www010 as [NVARCHAR](3)) as www010
	,wwmln1 as wwmln1
	,ltrim(rtrim(wwmln1)) as wwmln1_conv
	,wwalp1 as wwalp1
	,ltrim(rtrim(wwalp1)) as wwalp1_conv
	,wwuser as wwuser
	,ltrim(rtrim(wwuser)) as wwuser_conv
	,wwpid as wwpid
	,ltrim(rtrim(wwpid)) as wwpid_conv
	,cast(wwupmj as [INT]) as wwupmj
	,case when cast(wwupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wwupmj as [INT]) %1000, dateadd(year, cast(wwupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wwupmj_conv
	,wwjobn as wwjobn
	,ltrim(rtrim(wwjobn)) as wwjobn_conv
	,cast(wwupmt as [DECIMAL](38, 4)) as wwupmt
	,cast(wwntyp as [NVARCHAR](3)) as wwntyp
	,wwnick as wwnick
	,ltrim(rtrim(wwnick)) as wwnick_conv
	,cast(wwgend as [NVARCHAR](1)) as wwgend
	,cast(wwddate as [DECIMAL](38, 4)) as wwddate
	,cast(wwdmon as [DECIMAL](38, 4)) as wwdmon
	,cast(wwdyr as [DECIMAL](38, 4)) as wwdyr
	,cast(wwdyr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wwdyr_conv
	,cast(wwwn001 as [NVARCHAR](3)) as wwwn001
	,cast(wwwn002 as [NVARCHAR](3)) as wwwn002
	,cast(wwwn003 as [NVARCHAR](3)) as wwwn003
	,cast(wwwn004 as [NVARCHAR](3)) as wwwn004
	,cast(wwwn005 as [NVARCHAR](3)) as wwwn005
	,cast(wwwn006 as [NVARCHAR](3)) as wwwn006
	,cast(wwwn007 as [NVARCHAR](3)) as wwwn007
	,cast(wwwn008 as [NVARCHAR](3)) as wwwn008
	,cast(wwwn009 as [NVARCHAR](3)) as wwwn009
	,cast(wwwn010 as [NVARCHAR](3)) as wwwn010
	,cast(wwfuco as [NVARCHAR](10)) as wwfuco
	,cast(wwpcm as [NVARCHAR](10)) as wwpcm
	,wwpcf as wwpcf
	,ltrim(rtrim(wwpcf)) as wwpcf_conv
	,wwactin as wwactin
	,ltrim(rtrim(wwactin)) as wwactin_conv
	,wwcfrguid as wwcfrguid
	,ltrim(rtrim(wwcfrguid)) as wwcfrguid_conv
	,cast(wwsyncs as [DECIMAL](38, 4)) as wwsyncs
	,cast(wwcaad as [DECIMAL](38, 4)) as wwcaad
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
	,wwan8
	,wwidln
	,wwdss5
	,wwmlnm
	,wwattl
	,wwrem1
	,wwslnm
	,wwalph
	,wwdc
	,wwgnnm
	,wwmdnm
	,wwsrnm
	,wwtyc
	,www001
	,www002
	,www003
	,www004
	,www005
	,www006
	,www007
	,www008
	,www009
	,www010
	,wwmln1
	,wwalp1
	,wwuser
	,wwpid
	,wwupmj
	,wwjobn
	,wwupmt
	,wwntyp
	,wwnick
	,wwgend
	,wwddate
	,wwdmon
	,wwdyr
	,wwwn001
	,wwwn002
	,wwwn003
	,wwwn004
	,wwwn005
	,wwwn006
	,wwwn007
	,wwwn008
	,wwwn009
	,wwwn010
	,wwfuco
	,wwpcm
	,wwpcf
	,wwactin
	,wwcfrguid
	,wwsyncs
	,wwcaad,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0111_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0111_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0111_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0111_cdc(sys_integration_id); 
