-- REMOVE NON-LATEST DELETES
DELETE FROM stg_jde.tmp_delete_f554901_cdc
WHERE EXISTS (
             SELECT 1
             FROM  stg_jde.tmp_upsert_f554901_cdc u
             WHERE stg_jde.tmp_delete_f554901_cdc.sys_integration_id = u.sys_integration_id
             AND max_scn<=u.sys_cdc_scn
             )
OPTION ( LABEL = 'DELETE_2_stg_jde.tmp_delete_f554901_cdc AF:{{ task_instance_key_str }}' ) 

-- DELETE
DELETE FROM rep_jde.f554901_new
WHERE EXISTS (
            SELECT 1
            FROM stg_jde.tmp_delete_f554901_cdc
            WHERE stg_jde.tmp_delete_f554901_cdc.sys_integration_id = rep_jde.f554901_new.sys_integration_id
            )
OPTION ( LABEL = 'DELETE_2_stg_jde.tmp_delete_f554901_cdc AF:{{ task_instance_key_str }}' ) 