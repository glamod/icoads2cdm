home_directory=/gws/nopw/j04/c3s311a_lot2
code_directory=$home_directory/code/marine_code/python/icoads2cds
pyTools_directory=$code_directory/py_tools
scripts_directory=$code_directory/scripts
jobs_directory=$code_directory/C3S_beta_release_jobs
export PATH=/apps/contrib/jaspy/miniconda_envs/jaspy3.6/m3-4.5.4/bin:$PATH
source activate jaspy3.6-m3-4.5.4-r20180816
pyEnvironment_directory=$code_directory/i2c_py3_env
parent_in_directory=$home_directory/data/level0a/marine/sub_daily_data/IMMA1_R3.0.0T
parent_out_directory=$home_directory/data/level1/marine/sub_daily_data/IMMA1_R3.0.0T

config_files_directory=$code_directory/config_files
source $pyEnvironment_directory/bin/activate
export PYTHONPATH="$pyTools_directory:${PYTHONPATH}"
