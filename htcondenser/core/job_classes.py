"""
Classes to describe jobs, and other helper classes.

Robin Aggleton
"""


import logging_config
import logging
import os
import re
from subprocess import check_call
from common import cp_hdfs
from collections import OrderedDict


log = logging.getLogger(__name__)


class JobSet(object):
    """A set of jobs, all sharing a common submission file.

    Params:
    -------
    exe: str
        Name of executable for this set of jobs.

    copy_exe: bool
        If True, copies the executable to HDFS.

    setup: TODO
        TODO

    filename: str
        Filename for HTCondor job description file.

    out_dir: str
        Directory for STDOUT output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    out_file: str
        Filename for STDOUT output.

    err_dir: str
        Directory for STDERR output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    err_file: str
        Filename for STDERR output.

    log_dir: str
        Directory for log output. Will be automatically created if it does not
        already exist. Raises an OSError if already exists but is not a directory.

    log_file: str
        Filename for log output.

    cpus: int
        Number of CPU cores for each job.

    memory: str
        RAM to request for each job.

    disk: str
        Disk space to request for each job.

    transfer_hdfs_input: bool
        If True, transfers input files on HDFS to worker node first.
        Auto-updates program arguments to take this into account.

    transfer_input_files: list[str]
        List of files to be transferred across for each job. See notes in README.

    transfer_output_files: list[str]
        List of files to be transferred across for each job. See notes in README.

    hdfs_store: str
        If any local files (on /user) needs to be transferred to the job, it
        must first be stored on /hdfs. This argument specifies the directory
        where those files are stored.

    """

    def __init__(self,
                 exe,
                 copy_exe=True,
                 setup=None,
                 filename='jobs.condor',
                 out_dir='logs', out_file='$(cluster).$(process).out',
                 err_dir='logs', err_file='$(cluster).$(process).err',
                 log_dir='logs', log_file='$(cluster).$(process).log',
                 cpus=1, memory='100MB', disk='100MB',
                 transfer_hdfs_input=True,
                 transfer_input_files=None, transfer_output_files=None,
                 hdfs_store=None):
        super(JobSet, self).__init__()
        self.exe = exe
        self.copy_exe = copy_exe
        self.setup = setup
        self.job_filename = filename
        self.out_dir = os.path.abspath(str(out_dir))
        self.out_file = str(out_file)
        self.err_dir = os.path.abspath(str(err_dir))
        self.err_file = str(err_file)
        self.log_dir = os.path.abspath(str(log_dir))
        self.log_file = str(log_file)
        self.cpus = cpus if cpus >= 1 else 1
        self.memory = str(memory)
        self.disk = str(disk)
        self.transfer_hdfs_input = transfer_hdfs_input
        self.transfer_input_files = transfer_input_files or []
        self.transfer_output_files = transfer_output_files or []
        self.hdfs_store = hdfs_store
        self.job_template = os.path.join(os.path.dirname(__file__), '../templates/job.condor')

        # Hold all Job object this JobSet governs, key is Job name.
        self.jobs = OrderedDict()

        # Setup directories
        # ---------------------------------------------------------------------
        for d in [self.out_dir, self.err_dir, self.log_dir, self.hdfs_store]:
            if not os.path.isdir(d):
                os.makedirs(d)

        # if not os.path.isdir(self.hdfs_store):
        #     raise OSError('No such directory for hdfs_store argument')

        # Check output filenames are not blank
        # ---------------------------------------------------------------------
        for f in [self.out_file, self.err_file, self.log_file]:
            bad_filenames = ['', '.']
            if f in bad_filenames:
                raise OSError('Bad output filename')

    def write(self):
        """Write jobs to HTCondor job file."""

        with open(self.job_template) as tfile:
            template = tfile.read()

        job_contents = self.generate_job_contents(template)

        with open(self.job_filename, 'w') as jfile:
            jfile.write(job_contents)

    def generate_job_contents(self, template):
        """Create a job file from a template, replacing the necessary fields
        and adding in jobs with arguments.

        Params:
        -------
        template: str
            Job template as a single string, including tokens to be replaced.
        """

        # Make replacements in template
        replacement_dict = {
            'EXE_WRAPPER': os.path.join(os.path.dirname(__file__), '../templates/condor_worker.py'),
            'STDOUT': os.path.join(self.out_dir, self.out_file),
            'STDERR': os.path.join(self.err_dir, self.err_file),
            'STDLOG': os.path.join(self.log_dir, self.log_file),
            'CPUS': str(self.cpus),
            'MEMORY': self.memory,
            'DISK': self.disk,
            'TRANSFER_INPUT_FILES': ','.join(self.transfer_input_files),
            'TRANSFER_OUTPUT_FILES': ','.join(self.transfer_input_files),
        }

        for pattern, replacement in replacement_dict.iteritems():
            template = template.replace("{%s}" % pattern, replacement)

        # Add jobs
        for name, job in self.jobs.iteritems():
            template += '\n# %s\n' % name
            template += job.generate_job_arg_str()
            template += '\nqueue %d\n' % job.number

        # Check we haven't left any unused tokens in the template.
        # If we have, then remove them.
        leftover_tokens = re.findall(r'{\w*}', template)
        if leftover_tokens:
            log.warning('Leftover tokens in job file:')
        for tok in leftover_tokens:
            log.warning('%s' % tok)
            template = template.replace(tok, '')

        return template

    def submit(self):
        """Write HTCondor job file, copy necessary files to HDFS, and submit."""
        self.write()

        for job in self.jobs.itervalues():
            job.transfer_to_hdfs()

        check_call(['condor_submit', self.job_filename])


class FileMirror(object):
    """Simple class to store location of mirrored files: the original,
    the copy of HDFS, and the copy on the worker node."""
    def __init__(self, original, hdfs, worker):
        super(FileMirror, self).__init__()
        self.original = original
        self.hdfs = hdfs
        self.worker = worker


class Job(object):
    """One job instance in a JobSet, with defined arguments and in/outputs.

    Params:
    -------
    manager: JobSet

    name: str
        Name of this job. Must be unique in this JobSet.

    args: list[str]
        Arguments for this job.

    input_files: list[str]
        List of input files to be transferred across before running executable.

    output_files: list[str]
        List of output files to be transferred across to HDFS after executable finishes.

    number: int
        Number of this Job object to submit.
    """

    def __init__(self, manager, name, args=None,
                 input_files=None, output_files=None,
                 number=1):
        super(Job, self).__init__()
        self._manager = manager
        self.name = str(name)
        self.args = args or []
        self.user_input_files = input_files or []
        if self.manager.copy_exe:
            self.user_input_files.append(self.manager.exe)
        self.user_output_files = output_files or []
        self.number = int(number)

        if name in self.manager.jobs.keys():
            raise KeyError('Job with name %s already in this JobSet.' % name)
        self.manager.jobs[self.name] = self

        # Setup file mirroring on HDFS
        self.input_file_mirrors = []  # input original, mirror on HDFS, and worker
        self.output_file_mirrors = []  # output mirror on HDFS, and worker
        self.hdfs_mirror_dir = os.path.join(self.manager.hdfs_store, self.name)
        if not os.path.isdir(self.hdfs_mirror_dir):
            os.makedirs(self.hdfs_mirror_dir)
        self.setup_input_file_mirrors(hdfs_mirror_dir=self.hdfs_mirror_dir)
        self.setup_output_file_mirrors(hdfs_mirror_dir=self.hdfs_mirror_dir)

    @property
    def manager(self):
        return self._manager

    @manager.setter
    def manager(self, manager):
        if isinstance(manager, JobSet):
            self._manager = manager
        else:
            raise TypeError('Incorrect object type set as Job manager - requires a JobSet object')

    def setup_input_file_mirrors(self, hdfs_mirror_dir):
        """Attach a mirror HDFS location for each non-HDFS input file.
        Also attaches a worker node location, incase the user wishes to copy
        the HDFS input to worker node first before processing.

        Params:
        -------
        hdfs_mirror_dir: str
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

        Params:
        -------
        hdfs_mirror_dir: str
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

        This includes the user's args (in self.args), but also includes options
        for input and output files, and automatically updating the args to
        account for new locations on HDFS or worker node.
        """
        job_args = ['arguments=']

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
        job_args.append('--args')
        job_args.extend(new_args)

        return ' '.join(job_args)
