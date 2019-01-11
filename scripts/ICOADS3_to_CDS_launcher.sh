#!/bin/bash
# usage: ./ICOADS3_to_CDS_launcher.sh sid-dck yyyi yyye configFile(basename)
#        ./ICOADS3_to_CDS_launcher.sh 063-714 1980 1981 ICOADS_to_CDS_063-714.json

# Parses a marine level0a /sub_daily_data/IMMA1_R3.0.0T/sid-dck directory for {yyyyi..yyyye}-{01..12}.imma files and submitts a bsub job
# per file to be processed to level1 with python script ICOADS_to_CDS.py
#
# On each submitted job termintation, existence of non-zero size script products (header, observations) files
# is tested, leaving the corresponding yyyy-mo.fail or yyyy-mo.ok signature in the outdir
#
# Job output (std and error) to /outdir/bsub

source ../icoads2cds_setenv.sh

sid_deck=$1
job_idx=$LSB_JOBINDEX
sid_deck_job_dir=$jobs_directory/$1/icoads2cdm

datafile=$(awk '{print $1}' $sid_deck_job_dir/$sid_deck"_"$job_idx.input)
yr=$(awk '{print $2}' $sid_deck_job_dir/$sid_deck"_"$job_idx.input)
mo=$(awk '{print $3}' $sid_deck_job_dir/$sid_deck"_"$job_idx.input)
config_file=$(awk '{print $4}' $sid_deck_job_dir/$sid_deck"_"$job_idx.input)
outdir=$parent_out_directory/$sid_deck

if [ ! -d $outdir ]; then mkdir $outdir;fi

echo "Launching line:"
echo "python $scripts_directory/ICOADS3_to_CDS.py $datafile $yr $mo $config_files_directory/$config_file $outdir"

python $scripts_directory/ICOADS3_to_CDS.py $datafile $yr $mo $config_files_directory/$config_file $outdir

