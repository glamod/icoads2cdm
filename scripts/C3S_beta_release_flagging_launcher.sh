#!/bin/bash

function bsub_jobs {
        bsub -J $1"[1-$2]" -oo $3/$1"_%I.o" -eo $3/$1"_%I.e" -q short-serial -W $job_hours:$job_minutes -M $memo -R "rusage[mem=$memo]" $scripts_directory/ICOADS3_to_CDS_flagging_launcher.sh $1
}

source ../icoads2cds_setenv.sh

jobs_dir=$jobs_directory
sid_deck_list_file=$1
filebase=$(basename $sid_deck_list_file)
logfile=$jobs_dir/${filebase%.*}_flagging_$(date +'%Y%m%d_%H%M').log
exec > $logfile 2>&1

job_minutes=00
job_hours=02
memo=16000
qc_path='/group_workspaces/jasmin2/c3s311a_lot2/data/level1/marine/sub_daily_data/QC/R3.0.0T/'

if [ ! -d $jobs_dir ]; then mkdir $jobs_dir; fi

for p in $(awk 'NR>1 {printf "%s-%s,%s\n",$1,$2,$3}' $sid_deck_list_file)
do
	OFS=$IFS
	IFS=',' read -r -a process <<< "$p"
	IFS=$OFS
	sid_deck=${process[0]}
	sid_deck_job_dir=$jobs_dir/$sid_deck/flagging
        sid_deck_data_dir=$parent_out_directory/$sid_deck
	if [ ! -d $sid_deck_job_dir ]; then mkdir -p $sid_deck_job_dir;else rm $sid_deck_job_dir/*;fi
	counter=1
	for filename in $(ls $sid_deck_data_dir/header-*{1981..2010}-*.psv)
	do
		yr_mo=${filename: -11:7 }
		echo "$sid_deck $(basename $filename) listed. BSUB idx: $counter"
                echo ${yr_mo:0:4} ${yr_mo:5:8} $qc_path $sid_deck_data_dir $sid_deck_data_dir > $sid_deck_job_dir/$sid_deck"_"$counter.input
		((counter++))
	done
        ((counter--))
	bsub_jobs $sid_deck $counter $sid_deck_job_dir
done


