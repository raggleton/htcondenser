"""
Classes to describe individual job, as part of a JobSet.
"""


import htcondenser.core.logging_config
import logging
import os
import htcondenser as ht
from htcondenser.core.common import cp_hdfs
from itertools import chain


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class Job(object):
    """One job instance in a JobSet, with defined arguments and inputs/outputs.

    Parameters
    ----------
    name : str
        Name of this job. Must be unique in the managing JobSet, and DAGMan.

    args : list[str] or str, optional
        Arguments for this job.

    input_files : list[str], optional
        List of input files to be transferred across before running executable.
        If the path is not on HDFS, a copy will be placed on HDFS under
        `hdfs_store`/`job.name`. Otherwise, the original on HDFS will be used.

    output_files : list[str], optional
        List of output files to be transferred across to HDFS after executable finishes.
        If the path is on HDFS, then that will be the destination. Otherwise
        `hdfs_store`/`job.name` will be used as destination directory.

    quantity : int, optional
        Quantity of this Job to submit.

    hdfs_mirror_dir : str, optional
        Mirror directory for files to be put on HDFS. If not specified, will
        use `hdfs_mirror_dir`/self.name, where `hdfs_mirror_dir` is taken
        from the manager. If the directory does not exist, it is created.

    Raises
    ------
    KeyError
        If the user tries to create a Job in a JobSet which already manages
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
        if not args:
            args = []
        self.args = args[:]
        if isinstance(args, str):
            self.args = args.split()
        if not input_files:
            input_files = []
        self.input_files = input_files[:]
        if not output_files:
            output_files = []
        self.output_files = output_files[:]
        self.quantity = int(quantity)
        # Hold settings for file mirroring on HDFS
        self.input_file_mirrors = []  # input original, mirror on HDFS, and worker
        self.output_file_mirrors = []  # output mirror on HDFS, and worker
        self.hdfs_mirror_dir = hdfs_mirror_dir

    def __eq__(self, other):
        return self.name == other.name

    @property
    def manager(self):
        """Returns the Job's managing JobSet."""
        return self._manager

    @manager.setter
    def manager(self, manager):
        """Set the manager for this Job.

        Also triggers the setting of other info that depends on having a manager,
        mainly setting up the file mirroring on HDFS for input and output files.
        """
        if not isinstance(manager, ht.JobSet):
            raise TypeError('Incorrect object type set as Job manager - requires a JobSet object')
        self._manager = manager
        if manager.copy_exe:
            self.input_files.append(manager.exe)
        if manager.setup_script:
            self.input_files.append(manager.setup_script)
        # Setup mirroring in HDFS
        if not self.hdfs_mirror_dir:
            self.hdfs_mirror_dir = os.path.join(self.manager.hdfs_store, self.name)
            log.debug('Auto setting mirror dir %s', self.hdfs_mirror_dir)
        self.setup_input_file_mirrors(self.hdfs_mirror_dir)
        self.setup_output_file_mirrors(self.hdfs_mirror_dir)

    def setup_input_file_mirrors(self, hdfs_mirror_dir):
        """Attach a mirror HDFS location for each non-HDFS input file.
        Also attaches a location for the worker node, incase the user wishes to
        copy the input file from HDFS to worker node first before processing.

        Will correctly account for managing JobSet's preference for share_exe_setup.
        Since input_file_mirrors is used for generate_job_arg_str(), we need to add
        the exe/setup here, even though they don't get transferred by the Job itself.

        Parameters
        ----------
        hdfs_mirror_dir : str
            Location of directory to store mirrored copies.
        """
        for ifile in self.input_files:
            basename = os.path.basename(ifile)
            mirror_dir = hdfs_mirror_dir
            if (ifile in [self.manager.exe, self.manager.setup_script] and
                    self.manager.share_exe_setup):
                mirror_dir = self.manager.hdfs_store
            hdfs_mirror = (ifile if ifile.startswith('/hdfs')
                           else os.path.join(mirror_dir, basename))
            mirror = ht.FileMirror(original=ifile, hdfs=hdfs_mirror, worker=basename)
            self.input_file_mirrors.append(mirror)

    def setup_output_file_mirrors(self, hdfs_mirror_dir):
        """Attach a mirror HDFS location for each output file.

        Parameters
        ----------
        hdfs_mirror_dir : str
            Location of directory to store mirrored copies.
        """
        for ofile in self.output_files:
            basename = os.path.basename(ofile)
            hdfs_mirror = (ofile if ofile.startswith('/hdfs')
                           else os.path.join(hdfs_mirror_dir, basename))
            # set worker copy depending on if it's on hdfs or not, since we
            # can't stream to it.
            if ofile.startswith('/hdfs'):
                worker = basename
            else:
                worker = ofile
            mirror = ht.FileMirror(original=ofile, hdfs=hdfs_mirror, worker=worker)
            self.output_file_mirrors.append(mirror)

    def transfer_to_hdfs(self):
        """Transfer files across to HDFS.

        Auto-creates HDFS mirror dir if it doesn't exist, but only if
        there are 1 or more files to transfer.

        Will not transfer exe or setup script if manager.share_exe_setup is True.
        That is left for the manager to do.
        """
        # skip the exe.setup script - the JobSet should handle this itself.
        files_to_transfer = []
        for ifile in self.input_file_mirrors:
            if ((ifile.original == ifile.hdfs) or (self.manager.share_exe_setup and
                    ifile.original in [self.manager.exe, self.manager.setup_script])):
                continue
            files_to_transfer.append(ifile)

        if len(files_to_transfer) > 0 and not os.path.isdir(self.hdfs_mirror_dir):
            os.makedirs(self.hdfs_mirror_dir)

        for ifile in files_to_transfer:
            log.info('Copying %s -->> %s', ifile.original, ifile.hdfs)
            cp_hdfs(ifile.original, ifile.hdfs)

    def generate_job_arg_str(self):
        """Generate arg string to pass to the condor_worker.py script.

        This includes the user's args (in `self.args`), but also includes options
        for input and output files, and automatically updating the args to
        account for new locations on HDFS or worker node. It also includes
        common input files from managing JobSet.

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
            for ifile in chain(self.input_file_mirrors, self.manager.common_input_file_mirrors):
                for i, arg in enumerate(new_args):
                    if arg == ifile.original:
                        new_args[i] = ifile.worker

                # Add input files to be transferred across
                job_args.extend(['--copyToLocal', ifile.hdfs, ifile.worker])
        else:
            # Replace input files in exe args with their HDFS node copies
            for ifile in chain(self.input_file_mirrors, self.manager.common_input_file_mirrors):
                for i, arg in enumerate(new_args):
                    if arg == ifile.original:
                        new_args[i] = ifile.hdfs
                # Add input files to be transferred across,
                # but only if they originally aren't on hdfs
                if not ifile.original.startswith('/hdfs'):
                    job_args.extend(['--copyToLocal', ifile.hdfs, ifile.worker])

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
        job_args.extend(['--exe', os.path.basename(self.manager.exe)])

        # Add arguments for exe MUST COME LAST AS GREEDY
        if new_args:
            job_args.append('--args')
            job_args.extend(new_args)

        # Convert everything to str
        job_args = [str(x) for x in job_args]
        return ' '.join(job_args)
