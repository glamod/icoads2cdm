#!/bin/bash

function bsub_jobs {
	bsub -J qc_assess_$sid_deck -oo $sid_deck_job_dir/$sid_deck"_qc_assess.o" -eo $sid_deck_job_dir/$sid_deck"_qc_assess.e" -q short-serial -W $job_hours:$job_minutes -M $memo -R "rusage[mem=$memo]" python $scripts_directory/report_tools/ICOADS3_to_CDS_qc_assessment.py $sid_deck $first_year $last_year $qc_path $sid_deck_data_dir $sid_deck_report_dir
}


source ../icoads2cds_setenv.sh

jobs_dir=$jobs_directory
sid_deck=$1
logfile=$jobs_dir/$sid_deck"_qc_report_"$(date +'%Y%m%d_%H%M').log
exec > $logfile 2>&1

job_minutes=00
job_hours=03
memo=8000

if [ ! -d $jobs_dir ]; then mkdir $jobs_dir; fi


sid_deck_job_dir=$jobs_dir/$sid_deck/qc_assess
sid_deck_data_dir=$parent_out_directory/$sid_deck
sid_deck_report_dir=$parent_out_directory/$sid_deck/reports
qc_path='/gws/nopw/j04/c3s311a_lot2/data/level1/marine/sub_daily_data/IMMA1_R3.0.0T/working/QC/R3.0.0T/'

if [ ! -d $sid_deck_job_dir ]; then mkdir $sid_deck_job_dir ;else rm $sid_deck_job_dir/*;fi

first_imma=$(basename $(ls $parent_in_directory/$sid_deck/????-??.imma | head -1))
last_imma=$(basename $(ls $parent_in_directory/$sid_deck/????-??.imma | tail -1))
if (( ${first_imma:0:4} < 1981 ));then first_year=1981;else first_year=${first_imma:0:4};fi
if (( ${last_imma:0:4} > 2010 ));then last_year=2010;else last_year=${last_imma:0:4};fi
	bsub_jobs
done
