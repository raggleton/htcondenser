"""Functions that are commonly used."""


import logging_config
import logging
import os
from subprocess import call
import shutil


log = logging.getLogger(__name__)


def check_dir_create(directory):
    """Check to see if directory exists, if not create it.

    Params:
    -------
    directory: str
        Name of directory to check and create.

    Raises:
    -------
    IOError if 'directory' already exists but is a file.
    """
    if not os.path.isdir(directory):
        # if os.path.isfile(directory):
        #     log.error('%s exists but is a file' % directory)
        os.makedirs(directory)


def cp_hdfs(src, dest, force=True):
    """Copy file between src and destination, allowing for one or both to
    be on HDFS.

    Uses the hadoop commands to ensure same transfer.

    Params:
    -------
    src: str
        Source filepath. For files on HDFS, use the full filepath, /hdfs/...

    dest: str
        Destination filepath. For files on HDFS, use the full filepath, /hdfs/...

    force: bool
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
        call(cmds)
    else:
        # use normal copy command
        if os.path.isfile(src):
            shutil.copy2(src, dest)
        elif os.path.isdir(src):
            shutil.copytree(src, dest)
