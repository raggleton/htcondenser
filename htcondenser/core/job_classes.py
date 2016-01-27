"""
Classes to describe jobs, groups of jobs, and other helper classes.
"""


import logging_config
import logging
import os
import re
from subprocess import check_call
from common import cp_hdfs, date_time_now
from collections import OrderedDict


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class JobSet(object):
    """Governs a set of Jobs, all sharing a common submission file, log
    locations, resource request, and setup procedure.

    Parameters
    ----------
    exe : str
        Name of executable for this set of jobs. Note that path must be specified,
        e.g. './myexe'

    copy_exe : bool
        If `True`, copies the executable to HDFS. Set `False` for builtins e.g. awk

    setup_script : str
        Shell script to execute on worker node to setup necessary programs, libs, etc.

    filename : str
        Filename for HTCondor job description file.

    out_dir : str
        Directory for STDOUT output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    out_file : str
        Filename for STDOUT output.

    err_dir : str
        Directory for STDERR output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    err_file : str
        Filename for STDERR output.

    log_dir : str
        Directory for log output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    log_file : str
        Filename for log output.

    cpus : int
        Number of CPU cores for each job.

    memory : str
        RAM to request for each job.

    disk : str
        Disk space to request for each job.

    transfer_hdfs_input : bool
        If True, transfers input files on HDFS to worker node first.
        Auto-updates program arguments to take this into account.
        Otherwise files are read directly from HDFS.

    transfer_input_files : list[str]
        List of files to be transferred across for each job
        (from initial_dir for relative paths).
        **Usage of this argument is highly discouraged**
        (except in scenarios where you have a very small number of jobs,
        and the file(s) are very small) since it can lock up soolin due to both
        processor load and network load.
        Recommended to use input_files argument in Job() instead.

    transfer_output_files : list[str]
        List of files to be transferred across after each job
        (to initial_dir for relative paths).
        **Usage of this argument is highly discouraged**
        (except in scenarios where you have a very small number of jobs,
        and the file(s) are very small) since it can lock up soolin due to both
        processor load and network load.
        Recommended to use output_files argument in Job() instead.

    hdfs_store : str
        If any local files (on `/user`) needs to be transferred to the job, it
        must first be stored on `/hdfs`. This argument specifies the directory
        where those files are stored. Each job will have its own copy of all
        input files, in a subdirectory with the Job name. If this directory does
        not exist, it will be created.

    dag_mode : bool
        If False, writes all Jobs to submit file. If True, then the Jobs are
        part of a DAG and the submit file for this JobSet only needs a
        placeholder for jobs. Job arguments will be specified in the DAG file.

    other_args: dict
        Dictionary of other job options to write to HTCondor submit file.
        These will be added in **before** any arguments or jobs.

    Raises
    ------
    OSError
        If any of `out_file`, `err_file`, or `log_file`, are blank or '.'.

    OSError
        If any of `out_dir`, `err_dir`, `log_dir`, `hdfs_store` cannot be created.

    """

    def __init__(self,
                 exe,
                 copy_exe=True,
                 setup_script=None,
                 filename='jobs.condor',
                 out_dir='logs', out_file='$(cluster).$(process).out',
                 err_dir='logs', err_file='$(cluster).$(process).err',
                 log_dir='logs', log_file='$(cluster).$(process).log',
                 cpus=1, memory='100MB', disk='100MB',
                 transfer_hdfs_input=True,
                 transfer_input_files=None, transfer_output_files=None,
                 hdfs_store=None,
                 dag_mode=False,
                 other_args=None):
        super(JobSet, self).__init__()
        self.exe = exe
        self.copy_exe = copy_exe
        self.setup_script = setup_script
        self.filename = filename
        self.out_dir = os.path.abspath(str(out_dir))
        self.out_file = str(out_file)
        self.err_dir = os.path.abspath(str(err_dir))
        self.err_file = str(err_file)
        self.log_dir = os.path.abspath(str(log_dir))
        self.log_file = str(log_file)
        self.cpus = int(cpus) if int(cpus) >= 1 else 1
        self.memory = str(memory)
        self.disk = str(disk)
        self.transfer_hdfs_input = transfer_hdfs_input
        self.transfer_input_files = transfer_input_files or []
        self.transfer_output_files = transfer_output_files or []
        self.hdfs_store = hdfs_store
        self.dag_mode = dag_mode
        self.job_template = os.path.join(os.path.dirname(__file__), '../templates/job.condor')
        self.other_job_args = other_args

        # Hold all Job object this JobSet governs, key is Job name.
        self.jobs = OrderedDict()

        # Setup directories
        # ---------------------------------------------------------------------
        for d in [self.out_dir, self.err_dir, self.log_dir, self.hdfs_store]:
            if not os.path.isdir(d):
                log.info('Making directory %s', d)
                os.makedirs(d)

        # Check output filenames are not blank
        # ---------------------------------------------------------------------
        for f in [self.out_file, self.err_file, self.log_file]:
            bad_filenames = ['', '.']
            if f in bad_filenames:
                raise OSError('Bad output filename')

    def __eq__(self, other):
        return self.filename == other.filename

    def add_job(self, job):
        """Add a Job to the collection of jobs managed by this JobSet.

        Parameters
        ----------
        job: Job
            Job object to be added.

        Raises
        ------
        TypeError
            If `job` argument isn't of type Job (or derived type).

        KeyError
            If a job with that name is already governed by this JobSet object.
        """
        if not isinstance(job, Job):
            raise TypeError('Added job must by of type Job')

        if job.name in self.jobs:
            raise KeyError('Job %s already exists in JobSet' % job.name)

        self.jobs[job.name] = job
        job.manager = self

    def write(self, dag_mode):
        """Write jobs to HTCondor job file."""

        with open(self.job_template) as tfile:
            template = tfile.read()

        job_contents = self.generate_job_contents(template, dag_mode)

        log.info('Writing HTCondor job file to %s' % self.filename)
        with open(self.filename, 'w') as jfile:
            jfile.write(job_contents)

    def generate_job_contents(self, template, dag_mode=False):
        """Create a job file contents from a template, replacing necessary fields
        and adding in all jobs with necessary arguments.

        Can either be used for normal jobs, in which case all jobs added, or
        for use in a DAG, where a placeholder for any job(s) is used.

        Parameters
        ----------
        template : str
            Job template as a single string, including tokens to be replaced.

        dag_mode : bool
            If True, then submit file will only contain placeholder for job args.
            This is so it can be used in a DAG. Otherwise, the submit file will
            specify each Job attached to this JobSet.

        Returns
        -------
        str
            Completed job template.

        Raises
        ------
        IndexError
            If the JobSet has no Jobs attached.
        """

        if len(self.jobs) == 0:
            raise IndexError('You have not added any jobs to this JobSet.')

        worker_script = os.path.join(os.path.dirname(__file__),
                                     '../templates/condor_worker.py')

        if self.other_job_args:
            other_args_str = '\n'.join('%s = %s' % (str(k), str(v))
                                       for k, v in self.other_job_args.iteritems())
        else:
            other_args_str = None

        # Make replacements in template
        replacement_dict = {
            'EXE_WRAPPER': worker_script,
            'STDOUT': os.path.join(self.out_dir, self.out_file),
            'STDERR': os.path.join(self.err_dir, self.err_file),
            'STDLOG': os.path.join(self.log_dir, self.log_file),
            'CPUS': str(self.cpus),
            'MEMORY': self.memory,
            'DISK': self.disk,
            'TRANSFER_INPUT_FILES': ','.join(self.transfer_input_files),
            'TRANSFER_OUTPUT_FILES': ','.join(self.transfer_input_files),
            'OTHER_ARGS': other_args_str
        }

        for pattern, replacement in replacement_dict.iteritems():
            if replacement:
                template = template.replace("{%s}" % pattern, replacement)

        # Add jobs
        if dag_mode:
            # actual arguments are in the DAG file, only placeholders here
            template += 'arguments=$(%s)\n' % DAGMan.JOB_VAR_NAME
            template += 'queue\n'
        else:
            # specifiy each job in submit file
            for name, job in self.jobs.iteritems():
                template += '\n# %s\n' % name
                template += 'arguments="%s"\n' % job.generate_job_arg_str()
                template += '\nqueue %d\n' % job.quantity

        # Check we haven't left any unused tokens in the template.
        # If we have, then remove them.
        leftover_tokens = re.findall(r'{\w*}', template)
        if leftover_tokens:
            log.debug('Leftover tokens in job file:')
        for tok in leftover_tokens:
            log.debug('%s' % tok)
            template = template.replace(tok, '')

        return template

    def submit(self):
        """Write HTCondor job file, copy necessary files to HDFS, and submit.
        Also prints out info for user.
        """
        self.write(dag_mode=False)

        for job in self.jobs.itervalues():
            job.transfer_to_hdfs()

        check_call(['condor_submit', self.filename])

        if self.log_dir == self.out_dir == self.err_dir:
            log.info('Output/error/htcondor logs written to %s' % self.out_dir)
        else:
            for t, d in {'STDOUT': self.out_dir,
                         'STDERR': self.err_dir,
                         'HTCondor log': self.log_dir}:
                log.info('%s written to %s' % (t, d))


# this should prob be a dict or namedtuple
class FileMirror(object):
    """Simple class to store location of mirrored files: the original,
    the copy of HDFS, and the copy on the worker node."""
    def __init__(self, original, hdfs, worker):
        super(FileMirror, self).__init__()
        self.original = original
        self.hdfs = hdfs
        self.worker = worker


class Job(object):
    """One job instance in a JobSet, with defined arguments and inputs/outputs.

    Parameters
    ----------
    manager : JobSet
        JobSet object that will be responsible for this Job. The Job will
        inherit settings from the manager, such as executable, log directories,
        resource request, and setup procedure.

    name : str
        Name of this job. Must be unique in the managing JobSet.

    args : list[str] or str
        Arguments for this job.

    input_files : list[str]
        List of input files to be transferred across before running executable.
        If the path is not on HDFS, a copy will be placed on HDFS under
        `hdfs_store`/`job.name`. Otherwise, the original on HDFS will be used.

    output_files : list[str]
        List of output files to be transferred across to HDFS after executable finishes.
        If the path is on HDFS, then that will be the destination. Otherwise
        `hdfs_store`/`job.name` will be used as destination directory.

    quantity : int
        Quantity of this Job to submit.

    hdfs_mirror_dir : str
        Mirror directory for files to be put on HDFS. If not specified, will
        use `hdfs_mirror_dir`/self.name, where `hdfs_mirror_dir` is taken
        from the manager. If the directory does not exist, it is created.

    Raises
    ------
    KeyError
        If the user tries to create a Job in a JobSet which already governs
        a Job with that name.

    TypeError
        If the user tries to assign a manager that is not of type JobSet
        (or a derived class).
    """

    def __init__(self, name, args=None,
                 input_files=None, output_files=None,
                 quantity=1, hdfs_mirror_dir=None):
        super(Job, self).__init__()
        self._manager = None
        self.name = str(name)
        self.args = args or []
        if isinstance(args, str):
            self.args = args.split()
        self.user_input_files = input_files or []
        self.user_output_files = output_files or []
        self.quantity = int(quantity)
        # Hold settings for file mirroring on HDFS
        self.input_file_mirrors = []  # input original, mirror on HDFS, and worker
        self.output_file_mirrors = []  # output mirror on HDFS, and worker
        self.hdfs_mirror_dir = hdfs_mirror_dir

    def __eq__(self, other):
        return self.name == other.name

    @property
    def manager(self):
        return self._manager

    @manager.setter
    def manager(self, manager):
        """Set the manager for this Job.

        Also triggers the setting of other info that depends on having a manager,
        mainly setting up the file mirroring on HDFS for input and output files.
        """
        if not isinstance(manager, JobSet):
            raise TypeError('Incorrect object type set as Job manager - requires a JobSet object')
        self._manager = manager
        if manager.copy_exe:
            self.user_input_files.append(manager.exe)
        if manager.setup_script:
            self.user_input_files.append(manager.setup_script)
        # Setup mirroring in HDFS
        if not self.hdfs_mirror_dir:
            self.hdfs_mirror_dir = os.path.join(self.manager.hdfs_store, self.name)
        if not os.path.isdir(self.hdfs_mirror_dir):
            os.makedirs(self.hdfs_mirror_dir)
        self.setup_input_file_mirrors(self.hdfs_mirror_dir)
        self.setup_output_file_mirrors(self.hdfs_mirror_dir)

    def setup_input_file_mirrors(self, hdfs_mirror_dir):
        """Attach a mirror HDFS location for each non-HDFS input file.
        Also attaches a location for the worker node, incase the user wishes to
        copy the input file from HDFS to worker node first before processing.

        Parameters
        ----------
        hdfs_mirror_dir : str
            Location of directory to store mirrored copies.
        """
        for ifile in self.user_input_files:
            basename = os.path.basename(ifile)
            hdfs_mirror = (ifile if ifile.startswith('/hdfs')
                           else os.path.join(hdfs_mirror_dir, basename))
            mirror = FileMirror(original=ifile, hdfs=hdfs_mirror, worker=basename)
            self.input_file_mirrors.append(mirror)

    def setup_output_file_mirrors(self, hdfs_mirror_dir):
        """Attach a mirror HDFS location for each output file.

        Parameters
        ----------
        hdfs_mirror_dir : str
            Location of directory to store mirrored copies.
        """
        for ofile in self.user_output_files:
            basename = os.path.basename(ofile)
            hdfs_mirror = (ofile if ofile.startswith('/hdfs')
                           else os.path.join(hdfs_mirror_dir, basename))
            mirror = FileMirror(original=ofile, hdfs=hdfs_mirror, worker=basename)
            self.output_file_mirrors.append(mirror)

    def transfer_to_hdfs(self):
        """Transfer files across to HDFS."""
        for ifile in self.input_file_mirrors:
            if ifile.original != ifile.hdfs:
                log.info('Copying %s to %s', ifile.original, ifile.hdfs)
                cp_hdfs(ifile.original, ifile.hdfs)

    def generate_job_arg_str(self):
        """Generate arg string to pass to the condor_worker.py script.

        This includes the user's args (in `self.args`), but also includes options
        for input and output files, and automatically updating the args to
        account for new locations on HDFS or worker node.

        Returns
        -------
        str:
            Argument string for the job, to be passed to condor_worker.py

        """
        job_args = []
        if self.manager.setup_script:
            job_args.extend(['--setup', os.path.basename(self.manager.setup_script)])

        new_args = self.args[:]

        if self.manager.transfer_hdfs_input:
            # Replace input files in exe args with their worker node copies
            for ifile in self.input_file_mirrors:
                for i, arg in enumerate(new_args):
                    if arg == ifile.original:
                        new_args[i] = ifile.worker

                # Add input files to be transferred across
                job_args.extend(['--copyToLocal', ifile.hdfs, ifile.worker])
        else:
            # Replace input files in exe args with their HDFS node copies
            for i, arg in enumerate(new_args):
                for ifile in self.input_file_mirrors:
                    if arg == ifile.original:
                        new_args[i] = ifile.hdfs

        log.debug("New job args:")
        log.debug(new_args)

        # Add output files to be transferred across
        # Replace output files in exe args with their worker node copies
        for ofile in self.output_file_mirrors:
            for i, arg in enumerate(new_args):
                if arg == ofile.original or arg == ofile.hdfs:
                    new_args[i] = ofile.worker
            job_args.extend(['--copyFromLocal', ofile.worker, ofile.hdfs])

        # Add the exe
        job_args.extend(['--exe', self.manager.exe])

        # Add arguments for exe
        if new_args:
            job_args.append('--args')
            job_args.extend(new_args)
        job_args[-1] = job_args[-1]
        return ' '.join(job_args)


class DAGMan(object):
    """Class to implement DAG and manages Jobs and dependencies.

    Parameters
    ----------
    filename : str

    status_file : str

    status_update_period : int or str

    dot : str or None

    """

    # name of variable for indiviudal condor submit files
    JOB_VAR_NAME = 'jobOpts'

    def __init__(self,
                 filename='jobs.dag',
                 status_file='jobs.status',
                 status_update_period=30,
                 dot=None,
                 other_args=None):
        super(DAGMan, self).__init__()
        self.dag_filename = filename
        self.status_file = status_file
        self.status_update_period = str(status_update_period)
        self.dot = dot
        self.other_args = other_args

        # hold info about Jobs. key is name, value is a dict
        self.jobs = OrderedDict()

    def add_job(self, job, requires=None, job_vars=None, retry=None):
        """Add a Job to the DAG.

        Parameters
        ----------
        job : TYPE
            Description
        requires : TYPE, optional
            Description
        job_vars : TYPE, optional
            Description
        retry : int or str, optional
            Description

        Raises
        ------
        KeyError
            Description
        TypeError
            Description
        """
        if job.name in self.jobs:
            raise KeyError()

        # Append necessary job arguments to any user opts.
        job_vars = job_vars or ""
        job_vars += 'jobOpts="%s"' % job.generate_job_arg_str()

        self.jobs[job.name] = dict(job=job, job_vars=job_vars, retry=retry, requires=None)

        hierarchy_list = []
        # requires can be:
        # - a job name [str]
        # - a list/tuple/set of job names [list(str)]
        # - a Job [Job]
        # - a list/tuple/set of Jobs [list(Job)]
        if requires:
            if isinstance(requires, str):
                hierarchy_list.append(requires)
            elif isinstance(requires, Job):
                hierarchy_list.append(requires.name)
            elif hasattr(requires, '__getitem__'):  # maybe getattr better?
                for it in requires:
                    if isinstance(it, str):
                        hierarchy_list.append(it)
                    elif isinstance(it, Job):
                        hierarchy_list.append(it.name)
                    else:
                        raise TypeError('Can only add list of Jobs or list of job names')
            else:
                raise TypeError('Can only add Job(s) or job name(s)')

        # Keep list of names of Jobs that must be executed before this one.
        self.jobs[job.name]['requires'] = hierarchy_list

    def check_job_requirements(self, job):
        """Check that the required Jobs actually exist and have been added to DAG.

        Parameters
        ----------
        job : Job or str
            Job object or name of Jobs to check.

        Raises
        ------
        KeyError
            If job(s) have prerequisite jobs that have not been added to the DAG.
        TypeError
            If `job` argument is not of type str or Job, or an iterable of
            strings or Jobs.
        """
        job_name = ''
        if isinstance(job, Job):
            job_name = job.name
        elif isinstance(job, str):
            job_name = job
        else:
            log.debug(type(job))
            raise TypeError('job argument must be job name or Job object.')
        req_jobs = set(self.jobs[job_name]['requires'])
        all_jobs = set(self.jobs)
        if not req_jobs.issubset(all_jobs):
            raise KeyError('The following requirements on %s do not have corresponding '
                           'Job objects: %s' % (job_name, ', '.join(list(req_jobs - all_jobs))))

    def generate_job_str(self, job):
        """Generate a string for job, for use in DAG file.

        Includes condor job file, any vars, and other options e.g. RETRY.
        Job requirements (parents) are handled separately.

        Parameters
        ----------
        job : Job or str
            Job or job name.

        Returns
        -------
        name : str
            Job listing.

        Raises
        ------
        TypeError
            If `job` argument is not of type str or Job.
        """
        job_name = ''
        if isinstance(job, Job):
            job_name = job.name
        elif isinstance(job, str):
            job_name = job
        else:
            log.debug(type(job))
            raise TypeError('job argument must be job name or Job object.')

        job_obj = self.jobs[job_name]['job']
        job_contents = ['JOB %s %s' % (job_name, job_obj.manager.filename)]

        job_vars = self.jobs[job_name]['job_vars']
        if job_vars:
            job_contents.append('VARS %s %s' % (job_name, job_vars))

        job_retry = self.jobs[job_name]['retry']
        if job_retry:
            job_contents.append('RETRY %s %s' % (job_name, job_retry))

        return '\n'.join(job_contents)

    def generate_job_requirements_str(self, job):
        """Generate a string of prerequisite jobs for this job.

        Does a check to make sure that the prerequisite Jobs do exist in the DAG.

        Parameters
        ----------
        job : Job or str
            Job object or name of job.

        Returns
        -------
        str
            Job requirements if prerequisite jobs. Otherwise blank string.

        Raises
        ------
        TypeError
            If `job` argument is not of type str or Job.
        """
        job_name = ''
        if isinstance(job, Job):
            job_name = job.name
        elif isinstance(job, str):
            job_name = job
        else:
            log.debug(type(job))
            raise TypeError('job argument must be job name or Job object.')

        self.check_job_requirements(job)

        if self.jobs[job_name]['requires']:
            return 'PARENT %s CHILD %s' % (' '.join(self.jobs[job_name]['requires']), job_name)
        else:
            return ''

    def generate_dag_contents(self):
        """
        Generate DAG file contents as a string.

        Returns
        -------
        str:
            DAG file contents
        """
        # Hold each line as entry in this list, then finally join with \n
        contents = ['# DAG created at %s' % date_time_now(), '']

        # Add jobs
        for name in self.jobs:
            contents.append(self.generate_job_str(name))

        # Add parent-child relationships
        for name in self.jobs:
            contents.append(self.generate_job_requirements_str(name))

        # Add other options for DAG
        if self.status_file:
            contents.append('')
            contents.append('NODE_STATUS_FILE %s %s' % (self.status_file, self.status_update_period))

        if self.dot:
            contents.append('')
            contents.append('# Make a visual representation of this DAG (for PDF format):')
            fmt = 'pdf'
            output_file = os.path.splitext(self.dot)[0] + '.' + fmt
            contents.append('# dot -T%s %s -o %s' % (fmt, self.dot, output_file))
            contents.append('DOT %s' % self.dot)

        if self.other_args:
            contents.append('')
            for k, v in self.other_args.iteritems():
                contents.append('%s = %s' % (k, v))

        contents.append('')
        return '\n'.join(contents)

    def write(self):
        """Write DAG to file and causes all Jobs to write their HTCondor submit files."""

        dag_contents = self.generate_dag_contents()
        log.info('Writing DAG to %s' % self.dag_filename)
        with open(self.dag_filename, 'w') as dfile:
            dfile.write(dag_contents)

        # Write job files for each JobSet
        managers = set([jdict['job'].manager for jdict in self.jobs.values()])
        for manager in managers:
            manager.write(dag_mode=True)

    def submit(self):
        """Write all necessary submit files, transfer files to HDFS, and submit DAG."""
        self.write()
        for job in self.jobs.values():
            job['job'].transfer_to_hdfs()
        check_call(['condor_submit_dag', self.dag_filename])
