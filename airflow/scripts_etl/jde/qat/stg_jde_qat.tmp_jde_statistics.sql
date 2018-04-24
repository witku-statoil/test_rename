if object_id('stg_jde_qat.tmp_jde_statistics_previous','U') is not null
    drop table stg_jde_qat.tmp_jde_statistics_previous;
if object_id('stg_jde_qat.tmp_jde_statistics','U') is not null
    rename object stg_jde_qat.tmp_jde_statistics to tmp_jde_statistics_previous;

create table stg_jde_qat.tmp_jde_statistics
with
(
    HEAP
    ,distribution=REPLICATE
)
as
(
    select *
    from (
        select table_cd
              ,table_cdc_exp_file_name
              ,full_stg_table_cd
              ,full_rep_table_cd
              ,table_cdc_type
              ,table_cdc_status_cd
              ,table_cdc_status_msg
              ,table_cdc_chk_cnt,table_cdc_chk_cnt_dist,table_cdc_chk_src_cnt,table_cdc_chk_src_cnt_dist
              ,table_cdc_chk_sum_1,table_cdc_chk_sum_2,table_cdc_chk_sum_3,table_cdc_chk_sum_4,table_cdc_chk_sum_5
			  ,table_cdc_chk_col_1,table_cdc_chk_col_2,table_cdc_chk_col_3,table_cdc_chk_col_4,table_cdc_chk_col_5
			  ,table_ignore_chk_ind
              ,case when OBJECT_ID(full_stg_table_cd,'U') is not null
                        then cast(1 as bit)
                    else cast(0 as bit) end
                    as table_exists
              ,'select @count_stg_OUT=count(*),@count_stg_dist_OUT=-1 /*count(distinct sys_integration_id)*/'
                +' from '+full_stg_table_cd
                +' where 1=1'+(case when isnull(table_cdc_scn_to,'NA') <> 'NA' and table_cdc_type!='FULL'
									then ' and cast(sys_cdc_scn as decimal(38,0)) between '+table_cdc_scn_from+' and '+table_cdc_scn_to 
									else '' end )
                +' and sys_file_name='''+table_cdc_exp_file_name+''';'
                    as qry_stg
              ,N'@count_stg_OUT decimal(38,0) OUTPUT,@count_stg_dist_OUT decimal(38,0) OUTPUT'
                    as qry_stg_params
              ,'select @count_rep_OUT=count(*),@count_rep_dist_OUT=-1 /*count(distinct sys_integration_id)*/'
                +case when isnull(table_cdc_chk_col_1,'')!='' then ',@sum_rep_1_OUT=sum('+lower(table_cdc_chk_col_1)+')' else ',@sum_rep_1_OUT=null' end
				+case when isnull(table_cdc_chk_col_2,'')!='' then ',@sum_rep_2_OUT=sum('+lower(table_cdc_chk_col_2)+')' else ',@sum_rep_2_OUT=null' end
				+case when isnull(table_cdc_chk_col_3,'')!='' then ',@sum_rep_3_OUT=sum('+lower(table_cdc_chk_col_3)+')' else ',@sum_rep_3_OUT=null' end
				+case when isnull(table_cdc_chk_col_4,'')!='' then ',@sum_rep_4_OUT=sum('+lower(table_cdc_chk_col_4)+')' else ',@sum_rep_4_OUT=null' end
				+case when isnull(table_cdc_chk_col_5,'')!='' then ',@sum_rep_5_OUT=sum('+lower(table_cdc_chk_col_5)+')' else ',@sum_rep_5_OUT=null' end
                +' from '+full_rep_table_cd
                +' where 1=1'/*+(case when isnull(table_cdc_scn_to,'NA') <> 'NA' and table_cdc_type!='FULL'
									then ' and cast(sys_cdc_scn as decimal(38,0)) between '+table_cdc_scn_from+' and '+table_cdc_scn_to
									else '' end )*/
                +/*' and sys_file_name='''+table_cdc_exp_file_name+''*/';'
                    as qry_rep
              ,N'@count_rep_OUT decimal(38,0) OUTPUT,@count_rep_dist_OUT decimal(38,0) OUTPUT'
			  +N',@sum_rep_1_OUT decimal(38,0) OUTPUT,@sum_rep_2_OUT decimal(38,0) OUTPUT,@sum_rep_3_OUT decimal(38,0) OUTPUT,@sum_rep_4_OUT decimal(38,0) OUTPUT,@sum_rep_5_OUT decimal(38,0) OUTPUT'
                    as qry_rep_params
              /* Add placeholders for runtime validations */     
              ,cast(null as decimal(38,0)) as rt_count_stg
              ,cast(null as decimal(38,0)) as rt_count_dist_stg
              ,cast(null as decimal(38,0)) as rt_count_rep
              ,cast(null as decimal(38,0)) as rt_count_dist_rep
              ,cast(null as decimal(38,0)) as rt_sum_1,cast(null as decimal(38,0)) as rt_sum_2,cast(null as decimal(38,0)) as rt_sum_3
              ,cast(null as decimal(38,0)) as rt_sum_4,cast(null as decimal(38,0)) as rt_sum_5
              ,cast(null as bit) as rt_diff_stg
              ,cast(null as bit) as rt_diff_rep
              ,cast(null as bit) as rt_diff_full
              ,cast(null as bit) as rt_diff_sums
              ,cast(null as nvarchar(4000)) as rt_validation_message
        from (select table_cd,table_cdc_type,table_cdc_status_cd,table_cdc_status_msg/*,table_cdc_dt*/,table_cdc_scn_from,table_cdc_scn_to,table_cdc_exp_file_name,table_ignore_chk_ind
                    --,LEFT(table_cdc_exp_file_name,LEN(table_cdc_exp_file_name) - charindex('\',reverse(table_cdc_exp_file_name),1) + 1) [path]
                    --,RIGHT(table_cdc_exp_file_name, CHARINDEX('\', REVERSE(table_cdc_exp_file_name)) -1)                                [file_name]
                    ,isnull(table_cdc_chk_cnt,0) as table_cdc_chk_cnt,isnull(table_cdc_chk_cnt_dist,0) as table_cdc_chk_cnt_dist
					,isnull(table_cdc_chk_src_cnt,0) as table_cdc_chk_src_cnt,isnull(table_cdc_chk_src_cnt_dist,0) as table_cdc_chk_src_cnt_dist
                    ,case when table_cdc_chk_col_1 is not null then isnull(table_cdc_chk_sum_1,0) else table_cdc_chk_sum_1 end as table_cdc_chk_sum_1
					,case when table_cdc_chk_col_2 is not null then isnull(table_cdc_chk_sum_2,0) else table_cdc_chk_sum_2 end as table_cdc_chk_sum_2
					,case when table_cdc_chk_col_3 is not null then isnull(table_cdc_chk_sum_3,0) else table_cdc_chk_sum_3 end as table_cdc_chk_sum_3
					,case when table_cdc_chk_col_4 is not null then isnull(table_cdc_chk_sum_4,0) else table_cdc_chk_sum_4 end as table_cdc_chk_sum_4
					,case when table_cdc_chk_col_5 is not null then isnull(table_cdc_chk_sum_5,0) else table_cdc_chk_sum_5 end as table_cdc_chk_sum_5
					,table_cdc_chk_col_1,table_cdc_chk_col_2,table_cdc_chk_col_3,table_cdc_chk_col_4,table_cdc_chk_col_5
                    ,'[stg_jde_qat].[tmp_' + lower(table_cd) + (case when table_cdc_type = 'FULL' then '_init' else '_cdc' end) + ']' as full_stg_table_cd
                    ,'[rep_jde_qat].['     + lower(table_cd)                                                                    + '_new]' as full_rep_table_cd
                    ,row_number() over (partition by table_cd
                                          order by convert(datetime,table_cdc_dt_from,120)   desc
                                                  ,convert(datetime,sys_generated_dt ,120)   desc)
                                         as rown
              from [stg_jde_qat].[ext_jde_statistics]
              --where table_cdc_status_cd = 'DONE'
        ) tables_list
        --where rown=1
    ) dyn_queries
    /*where table_exists=1*/
)
;
