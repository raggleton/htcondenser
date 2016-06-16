"""
DAGMan class to handle DAGs in HTCondor.
"""


import logging
import os
from copy import deepcopy
from subprocess import check_call
from collections import OrderedDict
import htcondenser as ht
from htcondenser.common import date_time_now, check_dir_create


log = logging.getLogger(__name__)


class DAGMan(object):
    """Class to implement DAG, and manage Jobs and dependencies.

    Parameters
    ----------
    filename : str
        Filename to write DAG jobs. This cannot be on /users,
        must be on NFS drive, e.g. /storage.

    status_file : str, optional
        Filename for DAG status file. See
        https://research.cs.wisc.edu/htcondor/manual/current/2_10DAGMan_Applications.html#SECTION0031012000000000000000

    status_update_period : int or str, optional
        Refresh period for DAG status file in seconds.

    dot : str, optional
        Filename for dot file. dot can then be used to generate a pictoral
        representation of jobs in the DAG and their relationships.

    other_args : dict, optional
        Dictionary of {variable: value} for other DAG options.

    Attributes
    ----------
    JOB_VAR_NAME : str
        Name of variable to hold job arguments string to pass to condor_worker.py,
        required in both DAG file and condor submit file.
    """

    # name of variable for individual condor submit files
    JOB_VAR_NAME = 'jobOpts'

    def __init__(self,
                 filename='jobs.dag',
                 status_file='jobs.status',
                 status_update_period=30,
                 dot=None,
                 other_args=None):
        super(DAGMan, self).__init__()
        self.dag_filename = filename
        if os.path.abspath(self.dag_filename).startswith('/users'):
            raise IOError('You cannot put DAG filename on /users - must be on NFS (e.g.. /storage')
        self.status_file = status_file
        self.status_update_period = str(status_update_period)
        self.dot = dot
        self.other_args = other_args

        # hold info about Jobs. key is name, value is a dict
        self.jobs = OrderedDict()

    def __getitem__(self, i):
        if isinstance(i, int):
            if i >= len(self):
                raise IndexError()
            return self.jobs.values()[i]['job']
        elif isinstance(i, slice):
            return [x['job'] for x in self.jobs.values()[i]]
        else:
            raise TypeError('Invalid argument type - must be int or slice')

    def __len__(self):
        return len(self.jobs)

    def add_job(self, job, requires=None, job_vars=None, retry=None):
        """Add a Job to the DAG.

        Parameters
        ----------
        job : Job
            Job object to be added to DAG

        requires : str, Job, iterable[str], iterable[Job], optional
            Individual or a collection of Jobs or job names that must run first
            before this job can run. i.e. the job(s) specified here are the
            parents, whilst the added job is their child.

        job_vars : str, optional
            String of job variables specifically for the DAG. Note that program
            arguments should be set in Job.args not here.

        retry : int or str, optional
            Number of retry attempts for this job. By default the job runs once,
            and if its exit code != 0, the job has failed.

        Raises
        ------
        KeyError
            If a Job with that name has already been added to the DAG.

        TypeError
            If the `job` argument is not of type Job.
            If `requires` argument is not of type str, Job, iterable(str)
            or iterable(Job).
        """
        if not isinstance(job, ht.Job):
            raise TypeError('Cannot added a non-Job object to DAGMan.')

        if job.name in self.jobs:
            raise KeyError('Job with name %s already exists in DAG - names must be unique' % job.name)

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
            elif isinstance(requires, ht.Job):
                hierarchy_list.append(requires.name)
            elif hasattr(requires, '__getitem__'):  # maybe getattr better?
                for it in requires:
                    if isinstance(it, str):
                        hierarchy_list.append(it)
                    elif isinstance(it, ht.Job):
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
            Job object or name of Job to check.

        Raises
        ------
        KeyError
            If job(s) have prerequisite jobs that have not been added to the DAG.

        TypeError
            If `job` argument is not of type str or Job, or an iterable of
            strings or Jobs.
        """
        job_name = ''
        if isinstance(job, ht.Job):
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

    def check_job_acyclic(self, job):
        """Check no circular requirements, e.g. A ->- B ->- A

        Get all requirements for all parent jobs recursively, and check for
        the presence of this job in that list.

        Parameters
        ----------
        job : Job or str
            Job or job name to check

        Raises
        ------
        RuntimeError
            If job has circular dependency.
        """
        job_name = job.name if isinstance(job, ht.Job) else job
        parents = self.jobs[job_name]['requires']
        log.debug('Checking %s', job_name)
        log.debug(parents)
        while parents:
            new_parents = []
            for p in parents:
                grandparents = self.jobs[p]['requires']
                if job_name in grandparents:
                    raise RuntimeError("%s is in requirements for %s - cannot "
                                       "have cyclic dependencies" % (job_name, p))
                new_parents.extend(grandparents)
                parents = new_parents[:]
        return True

    def generate_job_str(self, job):
        """Generate a string for job, for use in DAG file.

        Includes condor job file, any vars, and other options e.g. RETRY.
        Job requirements (parents) are handled separately in another method.

        Parameters
        ----------
        job : Job or str
            Job or job name.

        Returns
        -------
        name : str
            Job listing for DAG file.

        Raises
        ------
        TypeError
            If `job` argument is not of type str or Job.
        """
        job_name = ''
        if isinstance(job, ht.Job):
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

        Does a check to make sure that the prerequisite Jobs do exist in the DAG,
        and that DAG is acyclic.

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
        if isinstance(job, ht.Job):
            job_name = job.name
        elif isinstance(job, str):
            job_name = job
        else:
            log.debug(type(job))
            raise TypeError('job argument must be job name or Job object.')

        self.check_job_requirements(job)
        self.check_job_acyclic(job)

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
            req_str = self.generate_job_requirements_str(name)
            if req_str != '':
                contents.append(req_str)

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

    def get_jobsets(self):
        """Get a list of all unique JobSets managing Jobs in this DAG.

        Returns
        -------
        name : list
            List of unique JobSet objects.
        """
        return list(set([jdict['job'].manager for jdict in self.jobs.itervalues()]))

    def write(self):
        """Write DAG to file and causes all Jobs to write their HTCondor submit files."""
        dag_contents = self.generate_dag_contents()
        log.info('Writing DAG to %s', self.dag_filename)
        check_dir_create(os.path.dirname(os.path.realpath(self.dag_filename)))
        with open(self.dag_filename, 'w') as dfile:
            dfile.write(dag_contents)

        # Write job files for each JobSet
        for manager in self.get_jobsets():
            manager.write(dag_mode=True)

    def submit(self, force=False, submit_per_interval=10):
        """Write all necessary submit files, transfer files to HDFS, and submit DAG.
        Also prints out info for user.

        Parameters
        ----------
        force : bool, optional
            Force condor_submit_dag
        submit_per_interval : int, optional
            Number of DAGMan submissions per interval. The default 10 every 5 seconds.

        Raises
        ------
        CalledProcessError
            If condor_submit_dag returns non-zero exit code.
        """
        self.write()
        for manager in self.get_jobsets():
            manager.transfer_to_hdfs()
        cmds = ['condor_submit_dag', self.dag_filename]
        if force:
            cmds.insert(1, '-f')
        # modify the env vars to modify DAGMan config settings
        # Not great, myabe should go for explciit config file instead?
        mod_env = deepcopy(os.environ)
        mod_env['_CONDOR_DAGMAN_MAX_SUBMITS_PER_INTERVAL'] = str(submit_per_interval)
        check_call(cmds, env=mod_env)
        log.info('Check DAG status:')
        log.info('DAGStatus %s', self.status_file)
