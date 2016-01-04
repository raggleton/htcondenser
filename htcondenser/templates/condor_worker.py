#!/usr/bin/env python

"""
Script that runs on HTCondor worker node, that correctly handles the setting up
and execution of a job.
"""


import argparse
from subprocess import call
import sys
import shutil
import os


class WorkerArgParser(argparse.ArgumentParser):
    """Argument parser for worker node execution"""
    def __init__(self, *args, **kwargs):
        super(WorkerArgParser, self).__init__(*args, **kwargs)
        self.add_arguments()

    def add_arguments(self):
        self.add_argument("--copyToLocal", nargs=2, action='append',
                          help="Files to copy to local area on worker node "
                          "before running program. "
                          "Must be of the form <source> <destination>. "
                          "Repeat for each file you want to copy.")
        self.add_argument("--copyFromLocal", nargs=2, action='append',
                          help="Files to copy from local area on worker node "
                          "after running program. "
                          "Must be of the form <source> <destination>. "
                          "Repeat for each file you want to copy.")
        self.add_argument("--exe", help="Name of executable")
        self.add_argument("--args", nargs=argparse.REMAINDER,
                          help="Args to pass to executable")


def run_job(in_args=sys.argv[1:]):
    """Main function to run commands on worker node."""

    parser = WorkerArgParser(description=__doc__)
    args = parser.parse_args(in_args)
    print args

    # Make sandbox area to avoid names clashing, and stop auto transfer
    # back to submission node
    # -------------------------------------------------------------------------
    os.mkdir('scratch')
    os.chdir('scratch')

    # Do setup of programs & libs
    # -------------------------------------------------------------------------

    # TODO

    # Copy files to worker node area from /users, /hdfs, /storage, etc.
    # -------------------------------------------------------------------------
    for (source, dest) in args.copyToLocal:
        print source, dest
        if source.startswith('/hdfs'):
            source = source.replace('/hdfs', '')
            call(['hadoop', 'fs', '-copyToLocal', source, dest])
        else:
            if os.path.isfile(source):
                shutil.copy2(source, dest)
            elif os.path.isdir(source):
                shutil.copytree(source, dest)

    print os.listdir(os.getcwd())

    # Run the program
    # -------------------------------------------------------------------------
    os.chmod(args.exe, 0555)
    cmds = ["./" + args.exe] + args.args
    print cmds
    call(cmds)

    print os.listdir(os.getcwd())

    # Copy files from worker node area to /hdfs or /storage
    # -------------------------------------------------------------------------
    for (source, dest) in args.copyFromLocal:
        print source, dest
        if dest.startswith('/hdfs'):
            dest = dest.replace('/hdfs', '')
            call(['hadoop', 'fs', '-copyFromLocal', '-f', source, dest])
        else:
            if os.path.isfile(source):
                shutil.copy2(source, dest)
            elif os.path.isdir(source):
                shutil.copytree(source, dest)


if __name__ == "__main__":
    run_job()
