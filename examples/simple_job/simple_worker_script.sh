#!/bin/bash -e
#
# This is a simple script to run on the worker node.
# See simple_job.py for more info.

# Read user arguments
inFile="$1"
outFile="$2"
oldWord="$3"
newWord="$4"

# Read file contents
contents=$(<$inFile)
echo $contents

# Make word substitution
newContents=${contents/$oldWord/$newWord}
echo $newContents

# Write result to new file
echo $newContents >> "$outFile"