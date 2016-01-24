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
import htcondenser as ht


# Set location on HDFS to hold files
HDFS_STORE = "/hdfs/user/%s/simple_cmssw_job" % os.environ['LOGNAME']

# Set location for logs
LOG_STORE = "/storage/%s/simple_cmssw_job/logs" % os.environ['LOGNAME']
log_stem = 'simple.$(cluster).$(process)'

# Define a JobSet object for all jobs running the same exe
# with same configuration for logs, etc
job_set = ht.JobSet(exe='edmDumpEventContent',
                    copy_exe=False,
                    setup_script='setup_cmssw.sh',
                    filename='simple_cmssw_job.condor',
                    out_dir=LOG_STORE, out_file=log_stem + '.out',
                    err_dir=LOG_STORE, err_file=log_stem + '.err',
                    log_dir=LOG_STORE, log_file=log_stem + '.log',
                    cpus=1, memory='50MB', disk='1',
                    hdfs_store=HDFS_STORE)

# Now add a Job
# Note that in this scenario, we are accessing the file over XRootD,
# and thus we don't need to add it to the input_files argument.
job = ht.Job(name='cmssw_job',
             args=['root://xrootd.unl.edu//store/mc/RunIISpring15Digi74/QCD_Pt_30to50_TuneCUETP8M1_13TeV_pythia8/GEN-SIM-RAW/AVE_20_BX_25ns_tsg_MCRUN2_74_V7-v1/00000/00228B32-44F0-E411-9FC7-0025905C3DCE.root'],
             input_files=None,
             output_files=None)

job_set.add_job(job)

# Now submit jobs
job_set.submit()
