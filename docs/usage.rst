Usage
=====

Here we explain a bit more about the basic **htcondenser** classes.

Full details on the API can be found in :doc:`apidoc/htcondenser`

For all snippets below, I've used::

    import htcondenser as ht


Some basic rules/principles
---------------------------

These go along with the `code of conduct <https://wikis.bris.ac.uk/display/dic/Code+of+Conduct>`_ and help your jobs run smoothly.

* The worker node is restricted to what it can read/write to:

    - **Read-only**: ``/software``, ``/users``
    - **Read + Write**: ``/hdfs``

* However ``/software`` and ``/users`` are all accessed over the network.

.. DANGER:: Reading from ``/users`` with multiple jobs running concurrently is guaranteed to lock up the whole network, including soolin.

* Therefore, it is best to only use ``/hdfs`` for reading & writing to/from worker nodes.

* Similarly, ``JobSet.filename``, ``.out_dir``, ``.err_dir``, ``.log_dir``, and ``DAGMan.filename`` and ``.status`` should be specified on ``/storage`` or similar - **not** ``/users``.

* `hadoop commands <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html>`_ should be used with ``/hdfs`` - use of ``cp``, ``rm``, etc can lead to lockup with many or large files.

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

Note that the files specified by ``input_files`` will automatically get transferred to HDFS before the job starts.
This avoids reading directly from ``/users``.
Files specified by ``output_files`` will automatically be transferred to HDFS from the worker node when the job ends.
Note that any arguments you pass to the job will automatically be updated to reflect any transfers to/from ``/hdfs``: *you do not need to worry about this*.

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

Input and output file arguments
-------------------------------

The ``input_files``/``output_files`` args work in the following manner.

For ``input_files``:

* ``myfile.txt``: the file is assumed to reside in the current directory. It will be copied to ``Job.hdfs_mirror_dir``. On the worker node, it will be copied to the worker.
* ``results/myfile.txt``: similar to the previous case, however **the directory structure will be removed**, and thus ``myfile.txt`` will end up in ``Job.hdfs_mirror_dir``. On the worker node, it will be copied to the worker.
* ``/storage/results/myfile.txt``: same as for ``results/myfile.txt``
* ``/hdfs/results/myfile.txt``: since this file already exists on ``/hdfs`` it will not be copied. If ``JobSet.transfer_hdfs_input`` is ``True`` it will be copied to the worker and accessed from there, otherwise will be accessed directly from ``/hdfs``.

For ``output_files``:

* ``myfile.txt``: assumes that the file will be produced in ``$PWD``. This will be copied to ``Job.hdfs_mirror_dir`` after ``JobSet.exe`` has finished.
* ``results/myfile.txt``: assumes that the file will be produced as ``$PWD/results/myfile.txt``. The file will be copied to ``Job.hdfs_mirror_dir`` after ``JobSet.exe`` has finished, but **the directory structure will be removed**.
* ``/storage/results/myfile.txt``: same as for ``results/myfile.txt``. Note that jobs cannot write to anywhere but ``/hdfs``.
* ``/hdfs/results/myfile.txt``: this assumes a file ``myfile.txt`` will be produced by the exe. It will then be copied to ``/hdfs/results/myfile.txt``. This allows for a custom output location.


**Rational**: this behaviour may seem confusing. However, it tries to account for multiple scenarios and best practices:

* Jobs on the worker node should ideally read from ``/hdfs``. ``/storage`` and ``/software`` are both readable-only by jobs. However, to avoid any potential network lock-up, I figured it was best to put it all on ``/hdfs``

* This has the nice side-effect of creating a 'snapshot' of the code used for the job, incase you ever need to refer to it.

* If a file ``/storage/A/B.txt`` wanted to be used, how would one determine where to put it on ``/hdfs``?

* The one downfall is that output files and input files end up in the same directory on ``/hdfs``, which may note be desirable.

**Note that I am happy to discuss or change this behaviour - please log an issue**: `github issues <https://github.com/raggleton/htcondenser/issues>`_

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

If ``DAGMan.status_file`` was defined, then one can uses the ``DAGStatus`` script to provide a user-friendly status summary table. See :doc:`dagstatus`.


Logging
-------

The **htcondenser** library utilises the python ``logging`` library.
If the user wishes to enable logging messages, one simply has to add into their script::

    import logging

    log = logging.getLogger(__name__)

where ``__name__`` resolves to e.g. ``htcondenser.core.Job``.
The user can then configure the level of messages produced, and various other options.
At ``logging.INFO`` level, this typically produces info about files being transferred, and job files written.
See the `full logging library documentation <https://docs.python.org/2/library/logging.html>`_ for more details.