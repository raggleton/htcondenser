"""Functions/classes that are commonly used."""


import logging
import os
from subprocess import check_call, Popen, PIPE
import shutil
import datetime


log = logging.getLogger(__name__)


class FileMirror(object):
    """Simple class to store location of mirrored files: the original,
    the copy of HDFS, and the copy on the worker node."""
    def __init__(self, original, hdfs, worker):
        super(FileMirror, self).__init__()
        self.original = original
        self.hdfs = hdfs
        self.worker = worker

    def __repr__(self):
        arg_str = ', '.join(['%s=%s' % (k, v) for k, v in self.__dict__.iteritems()])
        return 'FileMirror(%s)' % (arg_str)

    def __str__(self):
        arg_str = ', '.join(['%s=%s' % (k, v) for k, v in self.__dict__.iteritems()])
        return 'FileMirror(%s)' % arg_str


def check_dir_create(directory):
    """Check to see if directory exists, if not create it.

    Parameters
    ----------
    directory : str
        Name of directory to check and create.

    Raises
    -------
    IOError
        If 'directory' already exists but is a file.
    """
    if not os.path.isdir(directory):
        if os.path.isfile(directory):
            raise IOError('%s is already a file, cannot make dir' % directory)
        log.info("Making directory %s", directory)
        if os.path.abspath(directory).startswith('/hdfs'):
            check_call(['hadoop', 'fs', '-mkdir', '-p', os.path.abspath(directory).replace('/hdfs', '')])
        else:
            os.makedirs(directory)


def cp_hdfs(src, dest, force=True):
    """Copy file between src and destination, allowing for one or both to
    be on HDFS.

    Uses the hadoop commands if possible to ensure safe transfer.

    Parameters
    ----------
    src : str
        Source filepath. For files on HDFS, use the full filepath, /hdfs/...

    dest : str
        Destination filepath. For files on HDFS, use the full filepath, /hdfs/...

    force : bool, optional
        If True, will overwrite destination file if it already exists.
    """
    # Check if source and/or destination reside on HDFS
    flag_src_hdfs = src.startswith("/hdfs")
    flag_dest_hdfs = dest.startswith("/hdfs")

    if flag_src_hdfs or flag_dest_hdfs:
        # Create HDFS-compatible paths
        src_hdfs = src.replace("/hdfs", "") if flag_src_hdfs else src
        dest_hdfs = dest.replace("/hdfs", "") if flag_dest_hdfs else dest

        # use hadoop command
        hadoop_cmd = '-cp'
        if not flag_dest_hdfs:
            hadoop_cmd = '-copyToLocal'
        elif not flag_src_hdfs:
            hadoop_cmd = '-copyFromLocal'
        cmds = ['hadoop', 'fs', hadoop_cmd]
        if force:
            cmds.append('-f')
        cmds.extend([src_hdfs, dest_hdfs])
        log.debug(cmds)
        check_call(cmds)
    else:
        # use normal copy command
        if os.path.isfile(src):
            shutil.copy2(src, dest)
        elif os.path.isdir(src):
            shutil.copytree(src, dest)


def date_time_now(fmt='%H:%M:%S %d %B %Y'):
    """Get current date and time as a string.

    Parameters
    ----------
    fmt : str, optional
        Format string for time. Default is %H:%M:%S %d %B %Y. See strftime docs.

    Returns
    -------
    str
        Current date and time.
    """
    return datetime.datetime.now().strftime(fmt)


def date_now(fmt='%d %B %Y'):
    """Get current date as a string.

    Parameters
    ----------
    fmt : str, optional
        Format string for time. Default is %d %B %Y. See strftime docs.

    Returns
    -------
    str
        Current date.
    """
    return datetime.datetime.now().strftime(fmt)


def time_now(fmt="%H:%M:%S"):
    """Get current time as a string.

    Parameters
    ----------
    fmt : str, optional
        Format string for time. Default is %H:%M:%S. See strftime docs.

    Returns
    -------
    str
        Current time.
    """
    return datetime.datetime.now().strftime(fmt)


def check_certificate():
    """Check the user's grid certificate is valid, and > 1 hour time left.

    Raises
    ------
    RuntimeError
        If certificate not valid.
        If certificate valid but has < 1 hour remaining.
    """
    # use Popen and not check_output as doesn't exist in py2.6
    proc = Popen(['voms-proxy-info'], stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if err == '':
        parts = [line.split(':', 1) for line in out.split('\n') if line]
        voms_dict = dict((x[0].strip(), x[1].strip()) for x in parts)
        if int(voms_dict['timeleft'].split(":")[0]) < 1:
            raise RuntimeError('Your certificate has less than 1 hour remaining, '
                               'please renew using `voms-proxy-init -voms cms --valid 168`')
    else:
        raise RuntimeError(err)


def check_good_filename(filename):
    """Checks the filename isn't rubbish e.g. blank, a period

    Raises
    ------
    OSError
        If bad filename
    """
    bad_filenames = ['', '.']
    if filename in bad_filenames:
        raise OSError('Bad filename %s' % f)
