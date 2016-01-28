"""Functions that are commonly used."""


import htcondenser.core.logging_config
import logging
import os
from subprocess import check_call
import shutil
import datetime


log = logging.getLogger(__name__)


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
