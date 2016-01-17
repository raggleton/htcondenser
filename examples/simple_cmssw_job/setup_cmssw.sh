#!/bin/bash -e
export SCRAM_ARCH=slc6_amd64_gcc493
source /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project CMSSW CMSSW_7_6_0
cd CMSSW_7_6_0/src
eval `scramv1 runtime -sh`
cd ../..
