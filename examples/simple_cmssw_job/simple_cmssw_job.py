#!/usr/bin/env python


"""
Example of how to call call a setup script to setup CMSSW then call a command.

setup_cmssw.sh runs some shell commands to setup a CMSSW release.
We use the setup_script arg in JobSet() to define a setup script to run before
executing commands.

Then edmDumpEventContent is called as a simple example.

Note that before running you MUST specify a location on HDFS to store
input/output files (HDFS_STORE), otherwise it will not be able to transfer them.
This way, local files on /user will be automatically put on HDFS.
Alternatively, you can manually move them to a pre-assigned location on HDFS.
"""


import os
import sys
sys.path.append(os.path.abspath('../..'))
import htcondenser as ht


# Set location on HDFS to hold files
HDFS_STORE = "/hdfs/user/%s/simple_cmssw_job" % os.environ['LOGNAME']

if not os.path.isdir(HDFS_STORE):
    os.makedirs(HDFS_STORE)

# Set location for logs
LOG_STORE = "/storage/%s/simple_cmssw_job" % os.environ['LOGNAME']

if not os.path.isdir(LOG_STORE):
    os.makedirs(LOG_STORE)

log_dir = os.path.join(LOG_STORE, 'log')
log_stem = 'simple.$(cluster).$(process)'

# Define a JobSet object for all jobs running the same exe
# with same configuration for logs, etc
job_set = ht.JobSet(exe='edmDumpEventContent',
                    copy_exe=False,
                    setup_script='setup_cmssw.sh',
                    filename='simple_cmssw_job.condor',
                    out_dir=log_dir, out_file=log_stem + '.out',
                    err_dir=log_dir, err_file=log_stem + '.err',
                    log_dir=log_dir, log_file=log_stem + '.log',
                    cpus=1, memory='50MB', disk='1',
                    hdfs_store=HDFS_STORE)

# Now add a Job
job = ht.Job(manager=job_set,
             name='cmssw_job',
             args=['root://xrootd.unl.edu//store/mc/RunIISpring15Digi74/QCD_Pt_30to50_TuneCUETP8M1_13TeV_pythia8/GEN-SIM-RAW/AVE_20_BX_25ns_tsg_MCRUN2_74_V7-v1/00000/00228B32-44F0-E411-9FC7-0025905C3DCE.root'],
             input_files=None,
             output_files=None,
             number=1)

# Now submit jobs
job_set.submit()
