#!/usr/bin/env python
"""
Diamond DAG example

But more complicated, as jobs A and D run one script,
whilst jobs B and C run another (with separate args)
"""

import os
import htcondenser as ht


# Set location on HDFS to hold files
HDFS_STORE = "/hdfs/user/%s/dag_example" % os.environ['LOGNAME']

# Set location for logs
LOG_STORE = "/storage/%s/dag_example/logs" % os.environ['LOGNAME']
log_stem1 = 'simple1.$(cluster).$(process)'
log_stem2 = 'simple2.$(cluster).$(process)'

job_set1 = ht.JobSet(exe='./script1.sh',
                     copy_exe=True,
                     filename='simple_job1.condor',
                     out_dir=LOG_STORE, out_file=log_stem1 + '.out',
                     err_dir=LOG_STORE, err_file=log_stem1 + '.err',
                     log_dir=LOG_STORE, log_file=log_stem1 + '.log',
                     hdfs_store=HDFS_STORE)
jobA = ht.Job(name='jobA', args='A')
jobD = ht.Job(name='jobD', args='D')

job_set1.add_job(jobA)
job_set1.add_job(jobD)

job_set2 = ht.JobSet(exe='./script2.sh',
                     copy_exe=True,
                     filename='simple_job2.condor',
                     out_dir=LOG_STORE, out_file=log_stem2 + '.out',
                     err_dir=LOG_STORE, err_file=log_stem2 + '.err',
                     log_dir=LOG_STORE, log_file=log_stem2 + '.log',
                     hdfs_store=HDFS_STORE)
jobB = ht.Job(name='jobB', args='B')
jobC = ht.Job(name='jobC', args='C')

job_set2.add_job(jobB)
job_set2.add_job(jobC)

dag_man = ht.DAGMan(filename='diamond.dag',
                    status_file='diamond.status',
                    dot='diamond.dot')

dag_man.add_job(jobA)
dag_man.add_job(jobB, requires=[jobA])
dag_man.add_job(jobC, requires=[jobA])
dag_man.add_job(jobD, requires=[jobB, jobC])

# dag_man.write()
dag_man.submit()
