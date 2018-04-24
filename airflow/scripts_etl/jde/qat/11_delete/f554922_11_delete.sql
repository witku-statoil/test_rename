-- REMOVE NON-LATEST DELETES
DELETE FROM stg_jde_qat.tmp_delete_f554922_cdc
WHERE EXISTS (
             SELECT 1
             FROM  stg_jde_qat.tmp_upsert_f554922_cdc u
             WHERE stg_jde_qat.tmp_delete_f554922_cdc.sys_integration_id = u.sys_integration_id
             AND max_scn<=u.sys_cdc_scn
             )
OPTION ( LABEL = 'DELETE_2_stg_jde_qat.tmp_delete_f554922_cdc AF:{{ task_instance_key_str }}' ) 

-- DELETE
DELETE FROM rep_jde_qat.f554922_new
WHERE EXISTS (
            SELECT 1
            FROM stg_jde_qat.tmp_delete_f554922_cdc
            WHERE stg_jde_qat.tmp_delete_f554922_cdc.sys_integration_id = rep_jde_qat.f554922_new.sys_integration_id
            )
OPTION ( LABEL = 'DELETE_2_stg_jde_qat.tmp_delete_f554922_cdc AF:{{ task_instance_key_str }}' ) 