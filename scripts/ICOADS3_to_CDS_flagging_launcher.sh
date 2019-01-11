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
#
# Uses:
# ICOADS3_to_CDS_setenv.sh
# ICOADS3_to_CDS_on_exit.sh
# ICOADS3_to_CDS.py

# These activations and imports do not seem to be working here.......check this after making sure python script run-p python2.7 envhon2.7 env
#virtualenv -p python2.7 env
#virtualenv --system-site-packages -p python2.7 env
# source ./env/bin/activate # This works
#pip install -r requirements.txt > pip.output

source ../icoads2cds_setenv.sh

sid_deck=$1
job_idx=$LSB_JOBINDEX
sid_deck_job_dir=$jobs_directory/$1/flagging

yr=$(awk '{print $1}' $sid_deck_job_dir/$sid_deck"_"$job_idx.input)
mo=$(awk '{print $2}' $sid_deck_job_dir/$sid_deck"_"$job_idx.input)
qc_dir=$(awk '{print $3}' $sid_deck_job_dir/$sid_deck"_"$job_idx.input)
data_dir=$(awk '{print $4}' $sid_deck_job_dir/$sid_deck"_"$job_idx.input)
out_dir=$parent_out_directory/$sid_deck

if [ ! -d $outdir ]; then mkdir $outdir;fi

echo "Launching line:"
echo "python $scripts_directory/ICOADS3_to_CDS_flagging.py $yr $mo $qc_dir $data_dir $out_dir"

python $scripts_directory/ICOADS3_to_CDS_flagging.py $yr $mo $qc_dir $data_dir $out_dir

