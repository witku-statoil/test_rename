from airflow import DAG
from airflow.operators.sensors import TimeSensor
from airflow.contrib.operators.fs_operator import FileSensor
from airflow.contrib.operators.better_fs_operator import BetterFileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from datetime import time, date, datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.mssql_cli_operator import MsSqlCliOperator
from airflow.contrib.operators.mssql_check_operator import MsSqlCheckOperator
#import airflow.contrib.custom_CK.file_finder import FileFinder
import csv
import os
import re
import json
from subprocess import call,check_call
from airflow.operators.python_operator import PythonOperator

# Airflow Variables
varEnv = Variable.get("ENV_CONTEXT")
varBINotifyEmail = Variable.get("NOTIFY_BI_EMAIL")
varMaxConcurrency = int(Variable.get("JDE_MAX_CONCURRENCY"))
varDisableChecksInd =  Variable.get("JDE_DISABLE_CHECKS_IND")

# other Variables
# Avoid JDE Environment Refrences past this point....
varScriptLocation = '/u01/app/airflow/scripts_etl/jde'
varConfigLocation = '/u01/app/airflow/scripts_setup/jde'
varJDESchema = 'jde'
varRepSchema = 'rep_' + varJDESchema
varStgSchema = 'stg_' + varJDESchema

constFileFilter     = '*' #'{{ ds }}'
constFileFilterRegex  = '.*' #'{{ ds_nodash }}'
#varDWH_connID = 'bi-eur-dwh-etl-staticrc10'
#varDWH_connID = 'test-cp-etl-staticrc10'
varDWH_connID = 'test-cp_compute-etl-staticrc10'
varAZBlobStorMntPtLanding = '/u01/data/dw-landing-fs' #'/u01/dw-landing-fs'
#varLanding_connID = 'bf-landing'
varAZBlobStorMntPtPermStor = '/u01/data/dw-permstore-fs' #'/u01/dw-permstore-fs'

# set of default arguments passed on to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': varBINotifyEmail,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

def listFilesByPat(filePath,pushXCom,doSort,**kwargs):
    filesInDir = sorted(os.listdir(filePath), reverse=True, key=lambda x: int( '-1' if (not doSort or not x or len(x.split(r'_',maxsplit=3))<3) else (x.split(r'_',maxsplit=3)[2]) ) )
    if pushXCom == True:
        kwargs['ti'].xcom_push(key='filesInDir', value=filesInDir)
    return filesInDir

def pickMostRecentFileByPat(filePath,filePatRegex,pushXCom,skipFiles,incPath,procPath,procSkipPath,archivePath,archiveSkipPath,**kwargs):
    filesToSkip  = list( {} )
    filesSelected = list( {} )
    file_pattern = re.compile(filePatRegex)

    filesInDir = listFilesByPat(filePath,pushXCom,True,**kwargs)
    #kwargs['ti'].xcom_pull(task_ids='',key='filesInDir')

    for fileName in filesInDir:
        if re.match(file_pattern, fileName):
            if len(filesSelected)<=0 or (not skipFiles):
                filesSelected.append( {'fileName':fileName,'incPath':incPath,'procPath':procPath,'procSkipPath':procSkipPath,'archivePath':archivePath,'archiveSkipPath':archiveSkipPath} )
            else:
                filesToSkip.append(   {'fileName':fileName,'incPath':incPath,'procPath':procPath,'procSkipPath':procSkipPath,'archivePath':archivePath,'archiveSkipPath':archiveSkipPath} )

    if pushXCom == True:
        kwargs['ti'].xcom_push(key='filesToSkip',  value=filesToSkip)
        kwargs['ti'].xcom_push(key='filesToLoad',  value=filesSelected)
    return { 'filesSelected':filesSelected, 'filesToSkip':filesToSkip }

# Build table configuration and files lists
def buildTablesConfig(steeringFileLocation, **kwargs):
    filesToSkip  = list( {} )
    filesSelected = list( {} )
    tablesConfiguration = list()
    with open( steeringFileLocation ) as csvfile:
        tabfile = csv.DictReader(csvfile, delimiter=';', quotechar='"')

        for row in tabfile:
            table_name = row['table_name']
            if table_name[0]=='#':
                continue
            table_cdc_type = row['table_cdc_type']
            table_cdc_sufx  = ( 'full' if (table_cdc_type.upper()=='FULL') else 'cdc')
            table_cdc_sufx2 = ( 'init' if (table_cdc_type.upper()=='FULL') else 'cdc')

            filePatBlob        = table_name.lower()+ '_'+table_cdc_sufx+ '_'+constFileFilter+'\.csv\.gz'
            filePatRegex       = table_name.lower()+'\_'+table_cdc_sufx+'\_'+constFileFilterRegex+'\.csv\.gz'
            filePathIncoming   = varAZBlobStorMntPtLanding+'/incoming-raw/'+varJDESchema+'/'
            filePathProc       = varAZBlobStorMntPtLanding +'/processing-clean/' +varJDESchema+ '/' + table_name.lower() + '/' + table_cdc_sufx2 +'/'
            filePathProcSkip   = varAZBlobStorMntPtLanding +'/processing-clean/' +varJDESchema+ '/' + table_name.lower() + '/' + 'skipped' +'/'
            filePathArch       = varAZBlobStorMntPtPermStor+'/archive-clean/'    +varJDESchema+ '/' + table_name.lower() + '/' + table_cdc_sufx2 +'/'
            filePathArchSkip   = varAZBlobStorMntPtPermStor+'/archive-clean/'    +varJDESchema+ '/' + table_name.lower() + '/' + 'skipped' +'/'

            # don't push to XCom, just use the returned dict
            filesList = pickMostRecentFileByPat( filePathIncoming , filePatRegex, False, (False if table_cdc_sufx=='cdc' else True), filePathIncoming, filePathProc, filePathProcSkip, filePathArch, filePathArchSkip )
            filesToSkip.append(   filesList['filesToSkip']   )
            filesSelected.append( filesList['filesSelected'] )

            tablesConfiguration.append( {
                                         'tname':              table_name,
                                         'cdctype':            table_cdc_type,
                                         'fullIncPath':        filePathIncoming + table_name.lower() + constFileFilter + '.csv.gz',
                                         'procPath':           filePathProc,
                                         'procSkipPath':       filePathProcSkip,
                                         'archivePath':        filePathArch,
                                         'archiveSkipPath':    filePathArchSkip,
                                         'filesToSkip':        filesList['filesToSkip'],
                                         'filesSelected':      filesList['filesSelected']
                                         } )

    # Add the statistics files to the list of filesSelected, but they need to be cooked up properly
    # stats files (begin)
    filesListStats = listFilesByPat( filePathIncoming+'statistics/',False,False,**kwargs)
    #filesToSkip  = list( {} )
    #filesSelected = list( {} )
    filesStatSelected = list( {} )
    for fileName in filesListStats:
        table_name = 'statistics'
        filePathIncoming   = varAZBlobStorMntPtLanding +'/incoming-raw/'     +varJDESchema+ '/' + table_name.lower() + '/'
        filePathProc       = varAZBlobStorMntPtLanding +'/processing-clean/' +varJDESchema+ '/' + table_name.lower() + '/'
        filePathProcSkip   = varAZBlobStorMntPtLanding +'/processing-clean/' +varJDESchema+ '/' + table_name.lower() + '/'
        filePathArch       = varAZBlobStorMntPtPermStor+'/archive-clean/'    +varJDESchema+ '/' + table_name.lower() + '/'
        filePathArchSkip   = varAZBlobStorMntPtPermStor+'/archive-clean/'    +varJDESchema+ '/' + table_name.lower() + '/'
        filesStatSelected.append( {'fileName':fileName,'incPath':filePathIncoming,'procPath':filePathProc,'procSkipPath':filePathProcSkip,'archivePath':filePathArch,'archiveSkipPath':filePathArchSkip} )
    tablesConfiguration.append( {
                                     'tname':              table_name,
                                     'cdctype':            'FULL',
                                     'fullIncPath':        filePathIncoming + 'statistics_*',
                                     'procPath':           filePathProc,
                                     'procSkipPath':       filePathProcSkip,
                                     'archivePath':        filePathArch,
                                     'archiveSkipPath':    filePathArchSkip,
                                     'filesToSkip':        list( {} ),
                                     'filesSelected':      filesStatSelected
                                } )
    filesSelected.append( filesStatSelected )
    # stats files (end)

    # Now push stuff to XCom -- all these are LISTS of LISTS of DICTs  = list( list({}) )
    # Because there's a list of tables which contains a list of files for that table
    kwargs['ti'].xcom_push(key='filesToSkip'        , value=filesToSkip)
    kwargs['ti'].xcom_push(key='filesToLoad'        , value=filesSelected)
    #kwargs['ti'].xcom_push(key='tablesConfiguration', value=tablesConfiguration)
    return json.dumps(tablesConfiguration, sort_keys=True, indent=4) #tablesConfiguration.toJSON()

def moveBlobs(**kwargs):
    tablesList = kwargs['ti'].xcom_pull(task_ids=kwargs['taskId'], key=kwargs['listKey'])    #files = list( list({'fileName':str(),'incPath':str(),'procPath':str(),'procSkipPath':str(),'archivePath':str(),'archiveSkipPath':str()}) )
    destType = kwargs['destType']
    srcType  = kwargs['srcType']
    cmds  = list()

    listReturn = list( {} )

    if tablesList and len(tablesList)>0:
        for filesList in tablesList:
            if filesList and len(filesList)>0:
                for fileConfigDictObj in filesList:
                    if fileConfigDictObj:
                        listReturn.append( {"obj":fileConfigDictObj,"type":type(fileConfigDictObj)} )
                        cmds.append( ['call(["mv", "-f", "' + fileConfigDictObj[ srcType+"Path" ] + fileConfigDictObj["fileName"] + '", "' + fileConfigDictObj[ destType+"Path" ] + '"])'] )
                        call(["mv", "-f", fileConfigDictObj[ srcType+"Path" ]+fileConfigDictObj["fileName"] , fileConfigDictObj[ destType+"Path"] ])
    return json.dumps(cmds, indent=4)

dag = DAG('jde_frm_daily_load', default_args=default_args, start_date=datetime(2018, 2, 23, 0, 0), schedule_interval='@daily', max_active_runs=1, concurrency=varMaxConcurrency, template_searchpath=varScriptLocation)
dag.catchup = False
dag.doc_md = """\

#### JDE Framework DAG - Check load statistics [here](chart?chart_id=7&iteration_no=2&p_table_name=All) and documentation [here](https://statoilfuelretail.atlassian.net/wiki/spaces/CBDLP/pages/483557849/JDE+Framework+documentation)
Dynamically built DAG loading all tables listed in **./scripts_etl/jde/jde_fmw_tables.csv**

##### Table requres two columns
- table_name
- table_cdc_type=FULL/INCR for either full or incremental load

_Comment out table with # to exclude from load_

##### Airflow Variables controlling DAG
- JDE_MAX_CONCURRENCY - limis number of concurrent Airflow tasks
- JDE_DISABLE_CHECKS_IND - 1/0, 1 disables validation checks comparing source and target counts after load (_validate steps)
"""

# Wait until 6 a.m. UTC - this is to avoid adding time within schedule
t_wait = TimeSensor(task_id='JDE_Wait', target_time=time(hour=6, minute=0, second=0), poke_interval=600, dag=dag)

# Start processing
t_start = DummyOperator(task_id='JDE_FRM_Start_Run', dag=dag)

#
t_test_ready = BetterFileSensor(task_id='JDE_FRM_test_AZS_stats_file',
     retries=0,
     timeout=18000,
     poke_interval=60,
     filepath= varAZBlobStorMntPtLanding+'/incoming-raw/' + varJDESchema + '/statistics/',
     filepattern= 'statistics_'+constFileFilterRegex+'_az.csv',
     dag=dag)
t_test_ready.doc_md = """
##### Test existance of statistics file used later for data reconciliation. File is generated by Extract process at the end of export.
"""

# Rebuild statistics table
t_rebuild_stats_tbl = MsSqlCliOperator(task_id='JDE_FRM_stats_table_rebuild', dag=dag, mssql_conn_id=varDWH_connID,   sql='stg_'+varJDESchema+'.tmp_jde_statistics.sql'     )
# Update dependencies
t_wait >> t_start >> t_test_ready >> t_rebuild_stats_tbl

# Phase 0: sort files and move them to init/cdc & skipped in processing
t_build_files_config_id = 'JDE_FRM_build_files_dictionary'
t_build_tablesConfig = PythonOperator(task_id=t_build_files_config_id,
                                      python_callable=buildTablesConfig,
                                      provide_context=True,
                                      op_kwargs={'steeringFileLocation':varConfigLocation+'/jde_fmw_tables.csv'},
                                      dag=dag)
t_build_tablesConfig.doc_md = """
##### Select files from incoming to be moved to processing (both init/cdc & skipped). **Check XCom for a full lis of files included in the load.**
"""

# move selected files from incoming to processing (both init/cdc & skipped)
t_move_files_inc_proc_skips = PythonOperator(task_id='JDE_FRM_move_incoming_processing_skips'
                      ,python_callable=moveBlobs
                      ,provide_context=True
                      ,op_kwargs={'taskId':t_build_files_config_id
                                  ,'listKey':'filesToSkip'
                                  ,'destType':'procSkip'
                                  ,'srcType':'inc'}
                      ,dag=dag)
t_move_files_inc_proc_loads = PythonOperator(task_id='JDE_FRM_move_incoming_processing_loads'
                      ,python_callable=moveBlobs
                      ,provide_context=True
                      ,op_kwargs={'taskId':t_build_files_config_id
                                  ,'listKey':'filesToLoad'
                                  ,'destType':'proc'
                                  ,'srcType':'inc'}
                      ,dag=dag)

# Update dependencies
t_rebuild_stats_tbl >> t_build_tablesConfig >> t_move_files_inc_proc_skips >> t_move_files_inc_proc_loads

# Phase 1: processing of tables which must go first: F0005
t_f0005_2 = MsSqlCliOperator(task_id='JDE_FRM_f0005_ext_load_stg'       ,mssql_conn_id=varDWH_connID,dag=dag,   sql='03_materialize_table_init/f0005_03_materialize_table_init.sql'     )
t_f0005_3 = MsSqlCliOperator(task_id='JDE_FRM_f0005_stg_load_rep'       ,mssql_conn_id=varDWH_connID,dag=dag,   sql='05_load_init/f0005_05_load_init.sql'                               )
t_f0005_run_tests = MsSqlCliOperator(task_id='JDE_FRM_f0005_run_tests'  ,mssql_conn_id=varDWH_connID,dag=dag,   sql='exec ' + varStgSchema + '.prc_verify_jde_framework_load @TableName=\''+'F0005'+'\',@PrintVerbose=1' )
t_f0005_validate = MsSqlCheckOperator(task_id='JDE_FRM_f0005_validate',
                                      mssql_conn_id=varDWH_connID,
                                      dag=dag,
                                      # 1) there are no bitwise aggregates
                                      # 2) we want to make sure ALL files for this table have no differences detected of any type, hence the weird expressions below
                                      sql='select '
                                              + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_stg  as int)),-1) as bit), '
                                              + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_rep  as int)),-1) as bit), '
                                              + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_stg  as int)),-1) as bit), '
                                              + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_rep  as int)),-1) as bit), '
                                              + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_full as int)),-1) as bit), '
                                              + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_sums as int)),-1) as bit) '
                                              +'from ' + varStgSchema + '.tmp_jde_statistics where table_cd=\'F0005\''
)
t_f0005_5 = MsSqlCliOperator(task_id='JDE_FRM_f0005_rep_switch'         ,mssql_conn_id=varDWH_connID,dag=dag,   sql='12_switch_to_new_table/f0005_12_switch_to_new_table.sql'           )

# NOP node 1 serves as start point for phase 2 jobs which can run concurrently
t_nop_1 = DummyOperator(task_id='NOP_1', dag=dag)
# NOP node 2 serves as end point for phase 2 jobs
t_nop_2 = DummyOperator(task_id='NOP_2', dag=dag)
# NOP node 3 serves as end point for phase 3 jobs
#t_nop_3 = DummyOperator(task_id='NOP_3', dag=dag)

# Update dependencies
t_move_files_inc_proc_loads >> t_f0005_2 >> t_f0005_3 >> t_f0005_run_tests >> t_f0005_validate >> t_f0005_5 >> t_nop_1

# Phase 2: load all other tables in parallel streams (only DB/DWH operations here)
# Dynamic content from here
# Based on .csv file with table_name & table_cdc_type
with open( varConfigLocation+'/jde_fmw_tables.csv' ) as csvfile:
    tabfile = csv.DictReader(csvfile, delimiter=';', quotechar='"')
    for row in tabfile:
        table_name = row['table_name']
        if table_name[0]=='#':
            continue
        table_cdc_type = row['table_cdc_type']
        table_cdc_sufx  = ( 'full' if (table_cdc_type.upper()=='FULL') else 'cdc')
        table_cdc_sufx2 = ( 'init' if (table_cdc_type.upper()=='FULL') else 'cdc')

        task0,task1,task1_skips = None,None,None
        t_fxxxx_2,t_fxxxx_4 = None,None
        t_fxxxx_3,t_fxxxx_3_1,t_fxxxx_3_2,t_fxxxx_3_3,t_fxxxx_3_4,t_fxxxx_3_5,t_fxxxx_3_6 = None,None,None,None,None,None,None
        #t_fxxxx_3_clean = None
        varInitFile = None

        if table_name.lower() != 'f0005': # f0005 is a special handling already covered
            # Step 1: move files - this is alread done at this point
            #

            # Step 2: Load External to Stage
            t_fxxxx_2_id = 'JDE_FRM_' + table_name.lower() + '_ext_load_stg'
            t_fxxxx_2_cmd = ''+('03' if table_cdc_type.upper()=='FULL' else '04') +'_materialize_table_'+table_cdc_sufx2+'/'+table_name.lower()+'_'+('03' if table_cdc_type.upper()=='FULL' else '04')+'_materialize_table_'+table_cdc_sufx2+'.sql'
            t_fxxxx_2 = MsSqlCliOperator(task_id=t_fxxxx_2_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_2_cmd, dag=dag)

            # Update dependencies (steps 1 & 2)
            t_nop_1 >> t_fxxxx_2

            # Steps 3, 4, 5 and 6 : Load Stage to Rep + Run validations on table + Make Switch on Rep (rename new to current, etc.)
            # Pre-declare steps 4 , 5 and 6
            t_fxxxx_4_id         = 'JDE_FRM_' + table_name.lower() + '_run_tests'
            t_fxxxx_4_cmd        = 'exec ' + varStgSchema + '.prc_verify_jde_framework_load @TableName=\''+table_name.upper()+'\',@PrintVerbose=1'
            t_fxxxx_4            = MsSqlCliOperator(task_id=t_fxxxx_4_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_4_cmd, dag=dag)

            t_fxxxx_5 = MsSqlCheckOperator(task_id='JDE_FRM_' + table_name.lower() + '_validate',
                                       mssql_conn_id=varDWH_connID,
                                       dag=dag,
                                       # 1) there are no bitwise aggregates
                                       # 2) we want to make sure ALL files for this table have no differences detected of any type, hence the weird expressions below
                                       sql='select '
                                           + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_stg  as int)),-1) as bit), '
                                           + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_rep  as int)),-1) as bit), '
                                           + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_stg  as int)),-1) as bit), '
                                           + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_rep  as int)),-1) as bit), '
                                           + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_full as int)),-1) as bit), '
                                           + varDisableChecksInd +' |~ cast(coalesce(max(cast(rt_diff_sums as int)),-1) as bit) '
                                          +'from ' + varStgSchema + '.tmp_jde_statistics where table_cd=\''+table_name.upper()+'\''
                                       )
            t_fxxxx_5.doc_md = """Check details in statistics table:
```select * from {0}.tmp_jde_statistics where lower(table_cd) = '{1}';```
or Airflow load statistics report [here](chart?chart_id=7&iteration_no=2&p_table_name={1})""".format(varStgSchema, table_name.lower())

            t_fxxxx_6_id         = 'JDE_FRM_' + table_name.lower() + '_rep_switch'
            t_fxxxx_6_cmd        = '12_switch_to_new_table/'+table_name.lower()+'_12_switch_to_new_table.sql'
            t_fxxxx_6            = MsSqlCliOperator(task_id=t_fxxxx_6_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_6_cmd, dag=dag)

            # Now declare step 3 (for FULL) or steps 3.x (for INCR)
            if (table_cdc_type.upper()!='FULL'):
                t_fxxxx_3 = None
                #t_fxxxx_3_clean = None
                ## CDC: step 3.1: Create Upsert table
                t_fxxxx_3_1_CDC_id    = 'JDE_FRM_' + table_name.lower() + '_crt_upsert'
                t_fxxxx_3_1_CDC_cmd   = '06_upserts_table/'+table_name.lower()+'_06_upserts_table.sql'
                t_fxxxx_3_1           = MsSqlCliOperator(task_id=t_fxxxx_3_1_CDC_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_3_1_CDC_cmd, dag=dag)
                ## CDC: step 3.2: Backup Current Rep
                t_fxxxx_3_2_CDC_id    = 'JDE_FRM_' + table_name.lower() + '_rep_bkp'
                t_fxxxx_3_2_CDC_cmd   = '07_copy_rep_table/'+table_name.lower()+'_07_copy_rep_table.sql'
                t_fxxxx_3_2           = MsSqlCliOperator(task_id=t_fxxxx_3_2_CDC_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_3_2_CDC_cmd, dag=dag)
                ## CDC: step 3.3: Apply Updates
                t_fxxxx_3_3_CDC_id    = 'JDE_FRM_' + table_name.lower() + '_upd'
                t_fxxxx_3_3_CDC_cmd   = '08_update/'+table_name.lower()+'_08_update.sql'
                t_fxxxx_3_3           = MsSqlCliOperator(task_id=t_fxxxx_3_3_CDC_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_3_3_CDC_cmd, dag=dag)
                ## CDC: step 3.4: Insert New rows
                t_fxxxx_3_4_CDC_id    = 'JDE_FRM_' + table_name.lower() + '_ins_new'
                t_fxxxx_3_4_CDC_cmd   = '09_insert_new/'+table_name.lower()+'_09_insert_new.sql'
                t_fxxxx_3_4           = MsSqlCliOperator(task_id=t_fxxxx_3_4_CDC_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_3_4_CDC_cmd, dag=dag)
                ## CDC: step 3.5: Create Deletes table
                t_fxxxx_3_5_CDC_id    = 'JDE_FRM_' + table_name.lower() + '_crt_del'
                t_fxxxx_3_5_CDC_cmd   = '10_deletes_table/'+table_name.lower()+'_10_deletes_table.sql'
                t_fxxxx_3_5           = MsSqlCliOperator(task_id=t_fxxxx_3_5_CDC_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_3_5_CDC_cmd, dag=dag)
                ## CDC: step 3.6: Apply Deletes
                t_fxxxx_3_6_CDC_id    = 'JDE_FRM_' + table_name.lower() + '_del'
                t_fxxxx_3_6_CDC_cmd   = '11_delete/'+table_name.lower()+'_11_delete.sql'
                t_fxxxx_3_6           = MsSqlCliOperator(task_id=t_fxxxx_3_6_CDC_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_3_6_CDC_cmd, dag=dag)

                # Update dependencies
                t_fxxxx_2 >> t_fxxxx_3_1 >> t_fxxxx_3_2 >> t_fxxxx_3_3 >> t_fxxxx_3_4 >> t_fxxxx_3_5 >> t_fxxxx_3_6 >> t_fxxxx_4
            else:
                t_fxxxx_3_1,t_fxxxx_3_2,t_fxxxx_3_3,t_fxxxx_3_4,t_fxxxx_3_5,t_fxxxx_3_6 = None,None,None,None,None,None
                ## FULL: Load Stage to Rep - this is the only step
                t_fxxxx_3_FULL_id     = 'JDE_FRM_' + table_name.lower() + '_stg_load_rep'
                t_fxxxx_3_FULL_cmd    = '05_load_init/'+table_name.lower()+'_05_load_init.sql'
                t_fxxxx_3             = MsSqlCliOperator(task_id=t_fxxxx_3_FULL_id, mssql_conn_id=varDWH_connID, sql=t_fxxxx_3_FULL_cmd, dag=dag)

                # Update dependencies
                t_fxxxx_2 >> t_fxxxx_3 >> t_fxxxx_4

            ## Update dependencies
            t_fxxxx_4 >> t_fxxxx_5 >> t_fxxxx_6 >> t_nop_2

# Phase 3: move stats file to archive
#t_archive_stats = BashOperator(task_id='JDE_FRM_archive_stats', bash_command='mv -f '+varAZBlobStorMntPtLanding+'/incoming-raw/'+varJDESchema+'/statistics/statistics_'+constFileFilter+'_az.csv '+varAZBlobStorMntPtPermStor+'/archive-clean/'+varJDESchema+'/statistics/', dag=dag, retries=3)
# Update dependencies
#t_nop_2 >> t_archive_stats >> t_nop_3

# Phase 4: Move files from processing/init, processing/cdc and processing/skipped to their archive counterparts
t_move_files_proc_arch_skips = PythonOperator(task_id='JDE_FRM_move_processing_archive_skips'
                      ,python_callable=moveBlobs
                      ,provide_context=True
                      ,op_kwargs={'taskId':t_build_files_config_id
                                  ,'listKey':'filesToSkip'
                                  ,'destType':'archiveSkip'
                                  ,'srcType':'procSkip'}
                      ,dag=dag)
t_move_files_proc_arch_loads = PythonOperator(task_id='JDE_FRM_move_processing_archive_loads'
                      ,python_callable=moveBlobs
                      ,provide_context=True
                      ,op_kwargs={'taskId':t_build_files_config_id
                                  ,'listKey':'filesToLoad'
                                  ,'destType':'archive'
                                  ,'srcType':'proc'}
                      ,dag=dag)

# Finished Processing: raise SLA miss exception if running over 6h
t_end = DummyOperator(task_id='JDE_FRM_Finished_Run', sla=timedelta(hours=6), dag=dag)

# Update dependencies
t_nop_2 >> t_move_files_proc_arch_skips >> t_move_files_proc_arch_loads >> t_end
