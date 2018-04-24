-- REMOVE NON-LATEST DELETES
DELETE FROM stg_jde_qat.tmp_delete_f0111_cdc
WHERE EXISTS (
             SELECT 1
             FROM  stg_jde_qat.tmp_upsert_f0111_cdc u
             WHERE stg_jde_qat.tmp_delete_f0111_cdc.sys_integration_id = u.sys_integration_id
             AND max_scn<=u.sys_cdc_scn
             )
OPTION ( LABEL = 'DELETE_2_stg_jde_qat.tmp_delete_f0111_cdc AF:{{ task_instance_key_str }}' ) 

-- DELETE
DELETE FROM rep_jde_qat.f0111_new
WHERE EXISTS (
            SELECT 1
            FROM stg_jde_qat.tmp_delete_f0111_cdc
            WHERE stg_jde_qat.tmp_delete_f0111_cdc.sys_integration_id = rep_jde_qat.f0111_new.sys_integration_id
            )
OPTION ( LABEL = 'DELETE_2_stg_jde_qat.tmp_delete_f0111_cdc AF:{{ task_instance_key_str }}' ) 