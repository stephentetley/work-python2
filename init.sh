#!/bin/bash

# run with: 
# > . _working/coding/work/work-python2/init.sh

cd ~/_working/coding/work/work-python2/
conda activate /home/stephen/miniconda3
conda activate develop-env
export PYTHONPATH="$HOME/_working/coding/work/work-python2/src"
export WORK_SQL_ROOT="$HOME/_working/coding/work/work-sql"

# > python temp/temp_execute_sql.py
