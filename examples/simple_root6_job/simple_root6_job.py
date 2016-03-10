#!/usr/bin/env python


"""
Example of how to call call a setup script to setup libs, etc, and then call
ROOT6 to run a macro.

Until I find a better way to setup a ROOT release agnostically
(ie outside of CMSSW), this will rely on you running inside a CMSSW environment.
i.e. `which root` should return a valid path. THis works becasue we pass the user's
env vars to the worker node, so it will access ROOT on CVMFS.

ROOT6 is called in batch mode, executing the macro hist.C. Note that the
arguments (-l, -b, hist.C) go into the job.args field.

Note that before running you MUST specify a location on HDFS to store
input/output files (HDFS_STORE), otherwise it will not be able to transfer them.
This way, local files on /user will be automatically put on HDFS.
Alternatively, you can manually move them to a pre-assigned location on HDFS.
"""


import os
import htcondenser as ht


# Set location on HDFS to hold files
HDFS_STORE = "/hdfs/user/%s/simple_root6_job" % os.environ['LOGNAME']

# Set location for logs
LOG_STORE = "/storage/%s/simple_root6_job/logs" % os.environ['LOGNAME']
log_stem = 'simple.$(cluster).$(process)'

# Define a JobSet object for all jobs running the same exe
# with same configuration for logs, etc
job_set = ht.JobSet(exe='root',
                    copy_exe=False,
                    # setup_script='setup_root6.sh',
                    setup_script=None,
                    filename='simple_root6_job.condor',
                    out_dir=LOG_STORE, out_file=log_stem + '.out',
                    err_dir=LOG_STORE, err_file=log_stem + '.err',
                    log_dir=LOG_STORE, log_file=log_stem + '.log',
                    cpus=1, memory='50MB', disk='1',
                    hdfs_store=HDFS_STORE)

# Now add individual Jobs
# Here we are running our script multiple times, but passing it different
# arguments, and telling it to produce different output files.
job = ht.Job(name='root6_job',
             args='-l -q -b hist.C'.split(),
             input_files=['hist.C'],
             output_files=['hist.pdf'],
             quantity=1)
job_set.add_job(job)

# Now submit jobs
job_set.submit()
