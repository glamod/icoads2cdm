#!/bin/bash

source ../icoads2cds_setenv.sh

jobs_dir=$jobs_directory
sid_deck_list_file=$1
task=icoads2cdm
filebase=$(basename $sid_deck_list_file)
logfile=$jobs_dir/${filebase%.*}_$task"_check_"$(date +'%Y%m%d_%H%M').log
exec > $logfile 2>&1

for p in $(awk 'NR>1 {printf "%s-%s,%s\n",$1,$2,$3}' $sid_deck_list_file)
do
	OFS=$IFS
	IFS=',' read -r -a process <<< "$p"
	IFS=$OFS
	sid_deck=${process[0]}
	sid_deck_job_dir=$jobs_dir/$sid_deck/$task
        if [ ! -d $sid_deck_job_dir ];then echo $task directory does not exist for $sid_deck;exit;fi
	echo "Checking job dir $sid_deck_job_dir"
        if [ ! -d $sid_deck_job_dir/failed ];then mkdir $sid_deck_job_dir/failed;fi
	if [ ! -d $sid_deck_job_dir/success ];then mkdir $sid_deck_job_dir/success;fi
	if [ -e $sid_deck_job_dir/jobs_success_list ];then rm $sid_deck_job_dir/jobs_success_list;fi
	if [ -e $sid_deck_job_dir/jobs_status_list ];then rm $sid_deck_job_dir/jobs_status_list;fi
	if [ -e $sid_deck_job_dir/jobs_successi ];then rm $sid_deck_job_dir/jobs_successi;fi

  	grep "Successfully completed" $sid_deck_job_dir/*.o > $sid_deck_job_dir/jobs_success_list
  	for infile in $(ls $sid_deck_job_dir/*.input)
	do
		base=$(basename $infile .input)
		grep $sid_deck_job_dir/$base.o $sid_deck_job_dir/jobs_success_list > $sid_deck_job_dir/jobs_successi
		echo $sid_deck_job_dir/$base.o $(awk 'END {print NR}' $sid_deck_job_dir/jobs_successi ) >> $sid_deck_job_dir/jobs_status_list
    		if (( $(awk 'END {print NR}' $sid_deck_job_dir/jobs_successi ) < 1 ));then subdir=failed;else subdir=success;fi
		yr=$(awk '{print $2}' $infile)
		mo=$(awk '{print $3}' $infile)
		for ext in e o input
		do
    			cp $sid_deck_job_dir/$base.$ext  $sid_deck_job_dir/$subdir/$yr-$mo.$ext
		done

	done
done
