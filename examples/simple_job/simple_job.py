#!/usr/bin/env python


"""
Simple set of jobs to show off how to use htcondenser.

This will create 3 jobs, each running a simple shell script with arguments
(simple_worker_script.sh), will also read an input file (simple_text.txt),
and produces an output file.

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
HDFS_STORE = "/hdfs/user/%s/simple_job" % os.environ['LOGNAME']

if not os.path.isdir(HDFS_STORE):
    os.makedirs(HDFS_STORE)

# Set location for logs
LOG_STORE = "/storage/ra12451/simple_job"

if not os.path.idir(LOG_STORE):
    os.makedirs(LOG_STORE)


# Define a JobSet object for all jobs running the same exe
# with same configuration for logs, etc
job_set = ht.JobSet(exe='simple_worker_script.sh',
                    copy_exe=True,
                    setup=None,
                    filename='simple_job.condor',
                    out_dir=os.path.join(LOG_STORE, 'log'),
                    out_file='simple.$(cluster).$(process).out',
                    err_dir=os.path.join(LOG_STORE, 'log'),
                    err_file='simple.$(cluster).$(process).err',
                    log_dir=os.path.join(LOG_STORE, 'log'),
                    log_file='simple.$(cluster).$(process).log',
                    cpus=1, memory='50MB', disk='1',
                    hdfs_store=HDFS_STORE)

# Now add individual Jobs
# Here we are runnign our script multiple times, but passing it different
# arguments, and telling it to produce different output files.
for i, word in enumerate(['Easter', 'NYE', 'Summer']):
    job = ht.Job(manager=job_set,
                 name='job%d' % i,
                 args=['simple_text.txt',
                       'simple_results_%d.txt' % i,
                       'Christmas',
                       word],
                 input_files=['simple_text.txt'],
                 output_files=['simple_results_%d.txt' % i],
                 number=1)

# Now submit jobs
job_set.submit()
