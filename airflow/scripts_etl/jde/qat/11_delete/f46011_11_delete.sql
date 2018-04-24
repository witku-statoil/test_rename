-- REMOVE NON-LATEST DELETES
DELETE FROM stg_jde_qat.tmp_delete_f46011_cdc
WHERE EXISTS (
             SELECT 1
             FROM  stg_jde_qat.tmp_upsert_f46011_cdc u
             WHERE stg_jde_qat.tmp_delete_f46011_cdc.sys_integration_id = u.sys_integration_id
             AND max_scn<=u.sys_cdc_scn
             )
OPTION ( LABEL = 'DELETE_2_stg_jde_qat.tmp_delete_f46011_cdc AF:{{ task_instance_key_str }}' ) 

-- DELETE
DELETE FROM rep_jde_qat.f46011_new
WHERE EXISTS (
            SELECT 1
            FROM stg_jde_qat.tmp_delete_f46011_cdc
            WHERE stg_jde_qat.tmp_delete_f46011_cdc.sys_integration_id = rep_jde_qat.f46011_new.sys_integration_id
            )
OPTION ( LABEL = 'DELETE_2_stg_jde_qat.tmp_delete_f46011_cdc AF:{{ task_instance_key_str }}' ) 