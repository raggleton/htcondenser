Usage
=====

Here we explain a bit more aobut the basic **htcondenser** classes.

Full details on the API can be found in :doc:`apidoc/htcondenser.core`

For all snippets below, I've used::

    import htcondenser as ht

Basic non-DAG jobs
--------------------

There are only 2 basic classes needed: ``JobSet`` and ``Job``.

``Job`` represents a single job to be run - the execution of some program or script, with arguments, inputs and outputs.

``JobSet`` defines a group of ``Job`` s that share common properties (e.g. executable), so that they can all share a common condor submit file.


By specifying various options, these classes are designed to handle:

* The transferring of any necessary files (including executable) to ``/hdfs``.
* Writing the necessary condor job files.
* Setting up directories for logs, etc.

On the worker node, a wrapper script is run. This handles the transfer of any files before and after execution, and can run a setup script prior to the main executable.

Typically one defines a ``JobSet`` instance for each different executable to be run::

    job_set = ht.JobSet(exe='simple_worker_script.sh',
                        copy_exe=True,
                        setup_script=None,
                        filename='/storage/user1234/simple_job.condor',
                        out_dir='/storage/user1234/logs', out_file='$(cluster).$(process).out',
                        ...
                        cpus=1, memory='50MB', disk='1',
                        hdfs_store='/hdfs/user/user1234')

Then one defines the relevant ``Job`` instances with job-specific arguments and files::

    job = ht.Job(name='job1',
                 args=['simple_text.txt', ..., word],
                 input_files=['simple_text.txt'],
                 output_files=['simple_results_1.txt'],
                 quantity=1)

    job = ht.Job(name='job2',
                 args=['simple_text.txt', ..., other_word],
                 input_files=['simple_text.txt'],
                 output_files=['simple_results_2.txt'],
                 quantity=1)

Note that the files specified by ``input_files`` will automatically get transferred to HDFS before the job starts. This avoids reading straight from ``/users``.
Files specified by ``output_files`` will automatically be transferred to HDFS from the worker node when the job ends.

Each ``Job`` must then be added to the governing ``JobSet``::

    job_set.add_job(job)

Finally, one submits the ``JobSet``::

    job_set.submit()


The ``JobSet`` object has several constructor arguments of interest:

* One **must** specify the script/executable to be run, including its path if it's a non-builtin command: ``./myProg.exe`` not ``myProg.exe``, but ``grep`` is ok.
* The ``copy_exe`` option is used to distinguish between builtin commands which can be accessed without transferring the executable (e.g. ``grep``) and local executables which do require transferring (e.g. ``myProg.exe``).
* A setup script can also be defined, which will be executed before ``JobSet.exe``. This is useful for setting up the environment, e.g. CMSSW, or conda.
* There are also options for the STDOUT/STDERR/condor log files. These should be put on ``/storage``.
* The ``hdfs_store`` argument specifies where on ``/hdfs`` any input/output files are placed.
* The ``transfer_hdfs_input`` option controls whether input files on HDFS are copied to the worker node, or read directly from HDFS.
* ``common_input_files`` allows the user to specify files that should be transferred to the worker node for every job. This is useful for e.g. python module depedence.

The ``Job`` object only has a few arguments, since the majority of configuration is done by the governing ``JobSet``:

* ``name`` is a unique specifier for the Job
* ``args`` allows the user to specify argument unique to this job
* ``hdfs_mirror_dir`` specifies the location on ``/hdfs`` to store input & output files, as well as the job executable & setup script if ``JobSet.share_exe_setup = False``. The default for this is the governing ``JobSet.hdfs_store/Job.name``
* ``input_files/output_files`` allows the user to specify any input files for this job. The output files specified will automatically be transferred to ``hdfs_mirror_dir`` after the exe has finished.


DAG jobs
--------

Setting up DAG jobs is only slightly more complicated. We still use the same structure of ``Job`` s within a ``JobSet``.
However, we now introduce the ``DAGMan`` class (DAG Manager), which holds information about all the jobs, and crucially any inter-job dependence.
The class is constructed with arguments for DAG file, and optionally for status file (very useful for keeping track of lots of jobs)::

    LOG_STORE = "/storage/%s/dag_example/logs" % os.environ['LOGNAME']
    dag_man = ht.DAGMan(filename=os.path.join(LOG_STORE, 'diamond.dag'),
                        status_file=os.path.join(LOG_STORE, 'diamond.status'),

Note that like for ``JobSet`` s, it is best to put the file on ``/storage`` and not ``/users``.

You can then create ``Job`` and ``JobSet`` s as normal::

    job_set1 = ht.JobSet(exe='script1.sh', ...
    jobA = ht.Job(name='jobA', args='A')
    jobB = ht.Job(name='jobB', args='B')

One then simply has to add ``Job`` s to the ``DAGMan`` instance, specifying any requisite ``Job`` s which must be completed first::

    dag_man.add_job(jobA)
    dag_man.add_job(jobB, requires=[jobA])

Finally, instead of calling ``JobSet.submit()``, we instead call ``DAGMan.submit()`` to submit all jobs::

    dag_man.submit()

If ``DAGMan.status_file`` was defined, then one can uses the ``DAGstatus.py`` script to provide a user-friendly status summary table. See :doc:`dagstatus`.


Logging
-------

The **htcondenser** library utilises the python ``logging`` library.
If the user wishes to enable logging messages, one simply has to add into their script::

    import logging

    log = logging.getLogger(__name__)

The user can then configure the level of messages produced, and various other options.
At ``logging.INFO`` level, this typically produces info about files being transferred, and job files written.
See the `full logging library documentation <https://docs.python.org/2/library/logging.html>`_ for more details.