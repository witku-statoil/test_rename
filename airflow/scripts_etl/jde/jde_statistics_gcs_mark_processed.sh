#!/bin/sh
#  ======================================================================================
#  Script Name    :    jde_statistics_gcs_mark_processed.sh
#  Description    :    Move statistics_*.csv files from an incomming dir into repo
#  Generated for  :    CircleK
#  Generated date :    2017-05-11 15:36:31
#  Author         :    The Great Generator
#  ======================================================================================
#
#  Usage: scrpt.sh {ENV_CONTEXT} {GC_PROJECT} {RUN_DATE}
#         script.sh DEV circlek-devtest 2017-01-18
#
#  Notes / History:

# Airflow variables
if [ $# -eq 0 ]
        then
        ENV_CONTEXT='DEV'
        GC_PROJECT='circlek-devtest'
        RUN_DATE=`date -I`
else
        ENV_CONTEXT=$1
        GC_PROJECT=$2
        RUN_DATE=$3
fi

GCS_REPUCKET='gs://dw-rep-fs-'$GC_PROJECT
GCS_LANDBUCKET='gs://dw-landing-fs-'$GC_PROJECT


st_dt=$(date +%s)

echo "INFO:: `date` :: Start processing"

                        echo "INFO:: `date` :: Moving incomming files into the processing dir... "
                        for file in `gsutil ls $GCS_LANDBUCKET/in/incoming/jde_framework/statistics_*.csv`
                        do
                                mv_file=$GCS_REPUCKET/data/jde-framework/ctl/$(basename $file)
                                echo "INFO:: `date` :: Moving $file to $mv_file"
                                gsutil mv $file $mv_file
                        done

end_dt=$(date +%s)

echo "INFO:: `date` :: Finished processing :: "
echo "INFO:: `date` :: Duration: " $(( ($end_dt - $st_dt)/1 ))
