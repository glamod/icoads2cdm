#!/bin/bash

function bsub_jobs {
	bsub -J ts_report_$sid_deck -oo $sid_deck_job_dir/$sid_deck"_qc_report.o" -eo $sid_deck_job_dir/$sid_deck"_qc_report.e" -q short-serial -W $job_hours:$job_minutes -M $memo -R "rusage[mem=$memo]" python $scripts_directory/report_tools/pdf_report_qc.py $sid_deck $sid_deck_report_dir $first_year $last_year $sid_deck_report_dir/qc
}


source ../icoads2cds_setenv.sh

jobs_dir=$jobs_directory
sid_deck_list_file=$1
filebase=$(basename $sid_deck_list_file)
logfile=$jobs_dir/${filebase%.*}_qc_report_$(date +'%Y%m%d_%H%M').log
exec > $logfile 2>&1

job_minutes=00
job_hours=01
memo=8000

if [ ! -d $jobs_dir ]; then mkdir $jobs_dir; fi

for p in $(awk 'NR>1 {printf "%s-%s,%s\n",$1,$2,$3}' $sid_deck_list_file)
do
	OFS=$IFS
	IFS=',' read -r -a process <<< "$p"
	IFS=$OFS
	sid_deck=${process[0]}
	sid_deck_job_dir=$jobs_dir/$sid_deck/reports
	sid_deck_data_dir=$parent_out_directory/$sid_deck
	sid_deck_report_dir=$parent_out_directory/$sid_deck/reports

	if [ ! -d $sid_deck_job_dir ]; then mkdir $sid_deck_job_dir ;else rm $sid_deck_job_dir/*;fi

	first_imma=$(basename $(ls $parent_in_directory/$sid_deck/????-??.imma | head -1))
	last_imma=$(basename $(ls $parent_in_directory/$sid_deck/????-??.imma | tail -1))
  if (( ${first_imma:0:4} < 1981 ));then first_year=1981;else first_year=${first_imma:0:4};fi
	if (( ${last_imma:0:4} > 2010 ));then last_year=2010;else last_year=${last_imma:0:4};fi
	bsub_jobs
done
