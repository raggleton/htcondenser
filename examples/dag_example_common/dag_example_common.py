#!/usr/bin/env python
"""
Diamond DAG example

Exploits common_input_files.
"""

import os
import htcondenser as ht


# Set location on HDFS to hold files
HDFS_STORE = "/hdfs/user/%s/dag_example_common" % os.environ['LOGNAME']

# Set location for logs
LOG_STORE = "/storage/%s/dag_example_common/logs" % os.environ['LOGNAME']
log_stem = 'simple.$(cluster).$(process)'

job_set = ht.JobSet(exe='./runScript.sh',
                    copy_exe=True,
                    setup_script='setupScript.sh',
                    filename='simple_job.condor',
                    out_dir=LOG_STORE, out_file=log_stem + '.out',
                    err_dir=LOG_STORE, err_file=log_stem + '.err',
                    log_dir=LOG_STORE, log_file=log_stem + '.log',
                    share_exe_setup=True,
                    common_input_files=['example.txt'],
                    transfer_hdfs_input=False,
                    hdfs_store=HDFS_STORE,
                    dag_mode=True)
jobA = ht.Job(name='jobA', args='A')
jobB = ht.Job(name='jobB', args='B')
jobC = ht.Job(name='jobC', args='C')
jobD = ht.Job(name='jobD', args='D')

job_set.add_job(jobA)
job_set.add_job(jobB)
job_set.add_job(jobC)
job_set.add_job(jobD)

dag_man = ht.DAGMan(filename='diamond.dag',
                    status_file='diamond.status',
                    dot='diamond.dot')

dag_man.add_job(jobA)
dag_man.add_job(jobB, requires=[jobA])
dag_man.add_job(jobC, requires=[jobA])
dag_man.add_job(jobD, requires=[jobB, jobC])

# Can easily iterate over jobs in a DAGMan
print 'My JobSet has', len(dag_man), 'jobs:'
for job in dag_man:
    print job.name, 'running:', job.manager.exe, ' '.join(job.args)

dag_man.submit()
