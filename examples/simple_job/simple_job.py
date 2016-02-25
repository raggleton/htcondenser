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
import htcondenser as ht


# Set location on HDFS to hold files
HDFS_STORE = "/hdfs/user/%s/simple_job" % os.environ['LOGNAME']

# Set location for logs
LOG_STORE = "/storage/%s/simple_job/logs" % os.environ['LOGNAME']
log_stem = 'simple.$(cluster).$(process)'

# Define a JobSet object for all jobs running the same exe
# with same configuration for logs, etc
job_set = ht.JobSet(exe='simple_worker_script.sh',
                    copy_exe=True,
                    setup_script=None,
                    filename=os.path.join(LOG_STORE, 'simple_job.condor'),
                    out_dir=LOG_STORE, out_file=log_stem + '.out',
                    err_dir=LOG_STORE, err_file=log_stem + '.err',
                    log_dir=LOG_STORE, log_file=log_stem + '.log',
                    cpus=1, memory='50MB', disk='1',
                    hdfs_store=HDFS_STORE)

# Now add individual Jobs
# Here we are running our script multiple times, but passing it different
# arguments, and telling it to produce different output files.
for i, word in enumerate(['Easter', 'NYE', 'Summer']):
    job = ht.Job(name='job%d' % i,
                 args=['simple_text.txt',
                       'simple_results_%d.txt' % i,
                       'Christmas',
                       word],
                 input_files=['simple_text.txt'],
                 output_files=['simple_results_%d.txt' % i],
                 quantity=1)
    job_set.add_job(job)

# Can easily iterate over jobs in a JobSet
print 'My JobSet has', len(job_set), 'jobs:'
for job in job_set:
    print job.name, 'running:', job_set.exe, ' '.join(job.args)

# Now submit jobs
job_set.submit()
