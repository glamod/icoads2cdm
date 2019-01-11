#!/bin/bash

function bsub_jobs {
	bsub -J $1"[1-$2]" -oo $3/$1"_%I.o" -eo $3/$1"_%I.e" -q short-serial -W $job_hours:$job_minutes -M $memo -R "rusage[mem=$memo]" $scripts_directory/ICOADS3_to_CDS_launcher.sh $1
}

source ../icoads2cds_setenv.sh
jobs_dir=$jobs_directory
sid_deck_list_file=$1
filebase=$(basename $sid_deck_list_file)
logfile=$jobs_dir/${filebase%.*}_$(date +'%Y%m%d_%H%M').log
exec > $logfile 2>&1

job_minutes=30
job_hours=02
memo=8000

if [ ! -d $jobs_dir ]; then mkdir $jobs_dir; fi


for p in $(awk 'NR>1 {printf "%s-%s,%s\n",$1,$2,$3}' $sid_deck_list_file)
do
	OFS=$IFS
	IFS=',' read -r -a process <<< "$p"
	IFS=$OFS
	sid_deck=${process[0]}
	sid_deck_job_dir=$jobs_dir/$sid_deck/icoads2cdm
	if [ ! -d $sid_deck_job_dir ]; then mkdir -p $sid_deck_job_dir;else rm $sid_deck_job_dir/*;fi
	counter=1
	for filename in $(ls $parent_in_directory/$sid_deck/{1981..2010}-*.imma)
	do
		if [ -e $filename ]
		then
			yr_mo=$(basename $filename .imma)
			echo "$sid_deck $(basename $filename) listed. BSUB idx: $counter"
			echo $filename ${yr_mo:0:4} ${yr_mo:5:8} ${process[1]} > $sid_deck_job_dir/$sid_deck"_"$counter.input
			((counter++))
	  fi
	done
        ((counter--))
	bsub_jobs $sid_deck $counter $sid_deck_job_dir
done
