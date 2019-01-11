#!/bin/bash
source ../icoads2cds_setenv.sh

jobs_dir=$jobs_directory
sid_deck_list_file=$1
filebase=$(basename $sid_deck_list_file)
logfile=$jobs_dir/${filebase%.*}_process_io_$(date +'%Y%m%d_%H%M').log
exec > $logfile 2>&1

if [ ! -d $jobs_dir ]; then mkdir $jobs_dir; fi

for p in $(awk 'NR>1 {printf "%s-%s,%s\n",$1,$2,$3}' $sid_deck_list_file)
do
        echo $p
	OFS=$IFS
	IFS=',' read -r -a process <<< "$p"
	IFS=$OFS
	sid_deck=${process[0]}

	sid_deck_report_dir=$parent_out_directory/$sid_deck/reports

	if [ ! -d $sid_deck_report_dir ];then mkdir $sid_deck_report_dir;fi

	first_imma=$(basename $(ls $parent_in_directory/$sid_deck/????-??.imma | head -1))
	last_imma=$(basename $(ls $parent_in_directory/$sid_deck/????-??.imma | tail -1))
	if (( ${first_imma:0:4} < 1981 ));then first_year=1981;else first_year=${first_imma:0:4};fi
	if (( ${last_imma:0:4} > 2010 ));then last_year=2010;else last_year=${last_imma:0:4};fi

        if [ -e $sid_deck_report_dir/$sid_deck"_"imma_process_io.csv ];then rm $sid_deck_report_dir/$sid_deck"_"imma_process_io.csv;fi
	echo date,imma,header,sst,at,slp,wd,ws,wbt,dpt > $sid_deck_report_dir/$sid_deck"_"imma_process_io.csv

	if [ -d $parent_out_directory/$sid_deck/rejects_pub47 ];then rejects=true;else rejects=false;fi
               
	if $rejects
	then
        echo $sid_deck 'rejects'
        if [ ! -d $sid_deck_report_dir/rejects_pub47 ];then mkdir $sid_deck_report_dir/rejects_pub47;fi
        if [ -e $sid_deck_report_dir/rejects_pub47/$sid_deck"_"imma_process_io_rejects.csv ];then rm $sid_deck_report_dir/rejects_pub47/$sid_deck"_"imma_process_io_rejects.csv;fi	
	echo date,imma,header,sst,at,slp,wd,ws,wbt,dpt > $sid_deck_report_dir/rejects_pub47/$sid_deck"_"imma_process_io_rejects.csv
	fi
	for yr in $(eval echo {$first_year..$last_year})
	do
		for mo in $(echo {01..12})
		do
                        echo $yr-$mo
			if [ -e $parent_in_directory/$sid_deck/$yr"-"$mo.imma ]
			then
	  		imma=$(awk 'END {print NR}' $parent_in_directory/$sid_deck/$yr"-"$mo.imma)
			else
				imma=NaN
			fi
			if [ -e $parent_out_directory/$sid_deck/header-$yr"-"$mo.psv ]
			then
				header=$(awk 'END {print NR-1}' $parent_out_directory/$sid_deck/header-$yr"-"$mo.psv)
			else
				header=NaN
			fi
			if $rejects
			then
				if [ -e $parent_out_directory/$sid_deck/rejects_pub47/header-$yr"-"$mo.psv ]
				then
					header_rejects=$(awk 'END {print NR-1}' $parent_out_directory/$sid_deck/rejects_pub47/header-$yr"-"$mo.psv)
				else
					header_rejects=NaN
				fi
			fi
			declare -a vars
			counter=0
			for vari in sst at slp wd ws wbt dpt
			do
				if [ -e $parent_out_directory/$sid_deck/observations-$vari"-"$yr"-"$mo.psv ]
				then
					vars[$counter]=$(awk 'END {print NR-1}' $parent_out_directory/$sid_deck/observations-$vari"-"$yr"-"$mo.psv)
				else
					vars[$counter]=NaN
				fi
				((counter ++))
			done
			if $rejects
			then
				declare -a vars_rejects
				counter=0
				for vari in sst at slp wd ws wbt dpt
				do
					if [ -e $parent_out_directory/$sid_deck/rejects_pub47/observations-$vari"-"$yr"-"$mo.psv ]
					then
						vars_rejects[$counter]=$(awk 'END {print NR-1}' $parent_out_directory/$sid_deck/rejects_pub47/observations-$vari"-"$yr"-"$mo.psv)
					else
						vars_rejects[$counter]=NaN
					fi
					((counter ++))
				done
			fi
                        OIF=$IFS
                        echo $yr"-"$mo"-01",$imma,$header,$(IFS=,; echo "${vars[*]}")
		        echo $yr"-"$mo"-01",$imma,$header,$(IFS=,; echo "${vars[*]}") >> $sid_deck_report_dir/$sid_deck"_"imma_process_io.csv
			if $rejects
			then
                                echo "rejects"
                                echo $yr"-"$mo"-01",$imma,$header_rejects,$(IFS=,; echo "${vars_rejects[*]}")
				echo $yr"-"$mo"-01",$imma,$header_rejects,$(IFS=,; echo "${vars_rejects[*]}") >> $sid_deck_report_dir/rejects_pub47/$sid_deck"_"imma_process_io_rejects.csv
			fi
                        IFS=$OIF
		done

	done
        
	python $scripts_directory/report_tools/deck_plot_process_io.py $sid_deck $sid_deck_report_dir/$sid_deck"_"imma_process_io.csv $sid_deck_report_dir
        if $rejects
        then
        python $scripts_directory/report_tools/deck_plot_process_io.py $sid_deck $sid_deck_report_dir/rejects_pub47/$sid_deck"_"imma_process_io_rejects.csv $sid_deck_report_dir/rejects_pub47
        fi
done
