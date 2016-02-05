#!/bin/bash -e

# This script runs all the example scripts

# make any exes
cd simple_exe_job
echo "Making showsize exe"
gcc showsize.c -o showsize
cd ..

# now run through each dir and run the submit script
declare -a examples=("simple_job" "simple_exe_job" "simple_cmssw_job" "dag_example" "dag_example_common")
for exe in ${examples[@]}; do
    cd "$exe"
    echo ">>> Running $exe"
    python "$exe.py"
    cd ..
done