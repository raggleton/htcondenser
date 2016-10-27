"""
Class to describe groups of jobs sharing common settings, that becomes one condor submit file.
"""


import logging
import os
import re
from subprocess import check_call
from htcondenser.common import cp_hdfs, check_certificate, check_dir_create
from collections import OrderedDict
import htcondenser as ht


log = logging.getLogger(__name__)


class JobSet(object):
    """Manages a set of Jobs, all sharing a common submission file, log
    locations, resource request, and setup procedure.

    Parameters
    ----------
    exe : str
        Name of executable for this set of jobs. Note that path must be specified,
        e.g. './myexe'

    copy_exe : bool, optional
        If `True`, copies the executable to HDFS. Set `False` for builtins e.g. awk

    setup_script : str, optional
        Shell script to execute on worker node to setup necessary programs, libs, etc.

    filename : str, optional
        Filename for HTCondor job description file.

    out_dir : str, optional
        Directory for STDOUT output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    out_file : str, optional
        Filename for STDOUT output.

    err_dir : str, optional
        Directory for STDERR output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    err_file : str, optional
        Filename for STDERR output.

    log_dir : str, optional
        Directory for log output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    log_file : str, optional
        Filename for log output.

    cpus : int, optional
        Number of CPU cores for each job.

    memory : str, optional
        RAM to request for each job.

    disk : str, optional
        Disk space to request for each job.

    certificate : bool, optional
        Whether the JobSet requires the user's grid certificate.

    transfer_hdfs_input : bool, optional
        If True, transfers input files on HDFS to worker node first.
        Auto-updates program arguments to take this into account.
        Otherwise files are read directly from HDFS.
        Note that this does not affect input files **not** on HDFS - they will
        be transferred across regardlass.

    share_exe_setup : bool, optional
        If True, then all jobs will use the same exe and setup files on HDFS.
        If False, each job will have their own copy of the exe and setup script
        in their individual job folder.

    common_input_files : list[str], optional
        List of common input files for each job. Unlike Job input files, there
        will only be 1 copy of this input file made on HDFS. Not sure if this
        will break anything...

    hdfs_store : str, optional
        If any local files (on `/user`) needs to be transferred to the job, it
        must first be stored on `/hdfs`. This argument specifies the directory
        where those files are stored. Each job will have its own copy of all
        input files, in a subdirectory with the Job name. If this directory does
        not exist, it will be created.

    other_args: dict, optional
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
                 certificate=False,
                 transfer_hdfs_input=True,
                 share_exe_setup=True,
                 common_input_files=None,
                 hdfs_store=None,
                 dag_mode=False,
                 other_args=None):
        super(JobSet, self).__init__()
        self.exe = exe
        self.copy_exe = copy_exe
        self.setup_script = setup_script
        self.filename = os.path.abspath(filename)
        self.out_dir = os.path.realpath(str(out_dir))
        self.out_file = str(out_file)
        self.err_dir = os.path.realpath(str(err_dir))
        self.err_file = str(err_file)
        self.log_dir = os.path.realpath(str(log_dir))
        self.log_file = str(log_file)
        self.cpus = int(cpus) if int(cpus) >= 1 else 1
        self.memory = str(memory)
        self.disk = str(disk)
        self.certificate = certificate
        self.transfer_hdfs_input = transfer_hdfs_input
        self.share_exe_setup = share_exe_setup
        # can't use X[:] or [] idiom as [:] evaulated first (so breaks on None)
        if not common_input_files:
            common_input_files = []
        self.common_input_files = common_input_files[:]
        self.common_input_file_mirrors = []  # To hold FileMirror obj
        if hdfs_store is None:
            raise IOError('Need to specify hdfs_store')
        self.hdfs_store = hdfs_store
        # self.dag_mode = dag_mode
        self.job_template = os.path.join(os.path.dirname(__file__), 'templates/job.condor')
        self.other_job_args = other_args
        # Hold all Job object this JobSet manages, key is Job name.
        self.jobs = OrderedDict()


        # Setup directories
        # ---------------------------------------------------------------------
        for d in [self.out_dir, self.err_dir, self.log_dir, self.hdfs_store]:
            if d:
                check_dir_create(d)

        # Check output filenames are not blank
        # ---------------------------------------------------------------------
        for f in [self.filename, self.out_file, self.err_file, self.log_file]:
            bad_filenames = ['', '.']
            if f in bad_filenames:
                raise OSError('Bad output filename')

        # Setup mirrors for any common input files
        # ---------------------------------------------------------------------
        self.setup_common_input_file_mirrors(self.hdfs_store)

    def __eq__(self, other):
        return self.filename == other.filename

    def __getitem__(self, i):
        if isinstance(i, int):
            if i >= len(self):
                raise IndexError()
            return self.jobs.values()[i]
        elif isinstance(i, slice):
            return self.jobs.values()[i]
        else:
            raise TypeError('Invalid argument type - must be int or slice')

    def __len__(self):
        return len(self.jobs)

    def setup_common_input_file_mirrors(self, hdfs_mirror_dir):
        """Attach a mirror HDFS location for each non-HDFS input file.
        Also attaches a location for the worker node, incase the user wishes to
        copy the input file from HDFS to worker node first before processing.

        Parameters
        ----------
        hdfs_mirror_dir : str
            Location of directory to store mirrored copies.
        """
        for ifile in self.common_input_files:
            ifile = os.path.abspath(ifile)
            basename = os.path.basename(ifile)
            mirror_dir = hdfs_mirror_dir
            hdfs_mirror = (ifile if ifile.startswith('/hdfs')
                           else os.path.join(mirror_dir, basename))
            mirror = ht.FileMirror(original=ifile, hdfs=hdfs_mirror, worker=basename)
            self.common_input_file_mirrors.append(mirror)

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
        if not isinstance(job, ht.Job):
            raise TypeError('Added job must by of type Job')

        if job.name in self.jobs:
            raise KeyError('Job %s already exists in JobSet' % job.name)

        self.jobs[job.name] = job
        job.manager = self

    def write(self, dag_mode):
        """Write jobs to HTCondor job file."""

        with open(self.job_template) as tfile:
            template = tfile.read()

        file_contents = self.generate_file_contents(template, dag_mode)

        log.info('Writing HTCondor job file to %s', self.filename)
        check_dir_create(os.path.dirname(os.path.realpath(self.filename)))
        with open(self.filename, 'w') as jfile:
            jfile.write(file_contents)

    def generate_file_contents(self, template, dag_mode=False):
        """Create a job file contents from a template, replacing necessary fields
        and adding in all jobs with necessary arguments.

        Can either be used for normal jobs, in which case all jobs added, or
        for use in a DAG, where a placeholder for any job(s) is used.

        Parameters
        ----------
        template : str
            Job template as a single string, including tokens to be replaced.

        dag_mode : bool, optional
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
                                     'templates/condor_worker.py')

        # Update other_job_args if dag
        if dag_mode:
            if not self.other_job_args:
                self.other_job_args = dict()
            self.other_job_args['accounting_group'] = 'group_physics.hep'
            self.other_job_args['accounting_group_user'] = '$ENV(LOGNAME)'

        # Update other_job_args if certificate
        if self.certificate:
            check_certificate()
            if not self.other_job_args:
                self.other_job_args = dict()
            self.other_job_args['use_x509userproxy'] = 'True'

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
            'OTHER_ARGS': other_args_str
        }

        for pattern, replacement in replacement_dict.iteritems():
            if replacement:
                template = template.replace("{%s}" % pattern, replacement)

        # Add jobs
        if dag_mode:
            # actual arguments are in the DAG file, only placeholders here
            template += 'arguments=$(%s)\n' % ht.DAGMan.JOB_VAR_NAME
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
            log.debug('%s', tok)
            template = template.replace(tok, '')

        return template

    def transfer_to_hdfs(self):
        """Copy any necessary input files to HDFS.

        This transfers both common exe/setup (if self.share_exe_setup == True),
        and the individual files required by each Job.
        """
        # Do copying of exe/setup script here instead of through Jobs if only
        # 1 instance required on HDFS.
        if self.share_exe_setup:
            if self.copy_exe:
                log.info('Copying %s -->> %s', self.exe, self.hdfs_store)
                cp_hdfs(self.exe, self.hdfs_store)
            if self.setup_script:
                log.info('Copying %s -->> %s', self.setup_script, self.hdfs_store)
                cp_hdfs(self.setup_script, self.hdfs_store)

        # Transfer common input files
        for ifile in self.common_input_file_mirrors:
            log.info('Copying %s -->> %s', ifile.original, ifile.hdfs)
            cp_hdfs(ifile.original, ifile.hdfs)

        # Get each job to transfer their necessary files
        for job in self.jobs.itervalues():
            job.transfer_to_hdfs()

    def submit(self, force=False):
        """Write HTCondor job file, copy necessary files to HDFS, and submit.
        Also prints out info for user.

        Parameters
        ----------
        force : bool, optional
            Force condor_submit

        Raises
        ------
        CalledProcessError
            If condor_submit returns non-zero exit code.
        """
        self.write(dag_mode=False)
        self.transfer_to_hdfs()

        cmds = ['condor_submit', self.filename]
        if force:
            cmds.insert(1, '-f')
        check_call(cmds)

        if self.log_dir == self.out_dir == self.err_dir:
            log.info('Output/error/htcondor logs written to %s', self.out_dir)
        else:
            for t, d in {'STDOUT': self.out_dir,
                         'STDERR': self.err_dir,
                         'HTCondor log': self.log_dir}:
                log.info('%s written to %s', t, d)
