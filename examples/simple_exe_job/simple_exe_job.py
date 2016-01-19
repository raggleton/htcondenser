#!/usr/bin/env python


"""
Example of how to use a user-compiled executable in a HTCondor job.

Before running you MUST compile the exe first:

gcc showsize.c -o showsize

You can run it by doing: ./showsize

Note that the compiled executable MUST have any libraries it needs included -
dynamic linking will not work as it willl read from wherever the library is stored.

Note that before running you MUST specify a location on HDFS to store
input/output files (HDFS_STORE), otherwise it will not be able to transfer them.
This way, local files on /user will be automatically put on HDFS.
Alternatively, you can manually move them to a pre-assigned location on HDFS.
"""


import os
import htcondenser as ht


# Set location on HDFS to hold files
HDFS_STORE = "/hdfs/user/%s/simple_exe_job" % os.environ['LOGNAME']

# Set location for logs
LOG_STORE = "/storage/%s/simple_exe_job/logs" % os.environ['LOGNAME']
log_stem = 'simple.$(cluster).$(process)'

# Define a JobSet object for all jobs running the same exe
# with same configuration for logs, etc
job_set = ht.JobSet(exe='./showsize',
                    copy_exe=True,
                    setup_script=None,
                    filename='simple_exe_job.condor',
                    out_dir=LOG_STORE, out_file=log_stem + '.out',
                    err_dir=LOG_STORE, err_file=log_stem + '.err',
                    log_dir=LOG_STORE, log_file=log_stem + '.log',
                    cpus=1, memory='50MB', disk='1',
                    hdfs_store=HDFS_STORE)

# Now add individual Jobs
job = ht.Job(manager=job_set,
             name='job_exe',
             quantity=1)

# Now submit jobs
# job_set.write()
job_set.submit()
