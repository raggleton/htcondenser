#!/usr/bin/env python

"""
Script that runs on HTCondor worker node, that correctly handles the setting up
and execution of a job.
"""


import argparse
from subprocess import check_call, Popen, PIPE
import sys
import shutil
import os
import glob
import json
import time

do_status = True
try:
    import psutil
except ImportError:
    print 'Cannot do snapshot monitoring'
    do_status = False


class WorkerArgParser(argparse.ArgumentParser):
    """Argument parser for worker node execution"""
    def __init__(self, *args, **kwargs):
        super(WorkerArgParser, self).__init__(*args, **kwargs)
        self.add_arguments()

    def add_arguments(self):
        self.add_argument("--setup",
                          help="Name of script to run to setup programs, etc")
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
    print '>>>> condor_worker.py logging:'
    proc = Popen(['hostname', '-f'], stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if err == '':
        print 'Running on', out
    else:
        raise RuntimeError(err)

    parser = WorkerArgParser(description=__doc__)
    args = parser.parse_args(in_args)
    print 'Args:'
    print args


    # Make sandbox area to avoid names clashing, and stop auto transfer
    # back to submission node
    # -------------------------------------------------------------------------
    tmp_dir = 'scratch'
    os.mkdir(tmp_dir)
    os.chdir(tmp_dir)
    try:
        # Copy files to worker node area from /users, /hdfs, /storage, etc.
        # ---------------------------------------------------------------------
        if args.copyToLocal:
            print 'PRE EXECUTION: Copy to local:'
            copy_list = []
            for (source, dest) in args.copyToLocal:
                # handle globbing
                for match in glob.iglob(source):
                    copy_list.append((match, dest))

            for (source, dest) in copy_list:
                print source, "-->", dest
                if not os.path.exists(source):
                    print 'File {0} does not exist - cannot copy to {1}'.format(source, dest)
                else:
                    if source.startswith('/hdfs'):
                        source = source.replace('/hdfs', '')
                        check_call(['hadoop', 'fs', '-copyToLocal', source, dest])
                    else:
                        if os.path.isfile(source):
                            shutil.copy2(source, dest)
                        elif os.path.isdir(source):
                            shutil.copytree(source, dest)

        print 'In current dir:'
        print os.listdir(os.getcwd())

        # Do setup of programs & libs, and run the program
        # We have to do this in one step to avoid different-shell-weirdness,
        # since env vars don't necessarily get carried over.
        # ---------------------------------------------------------------------
        print 'SETUP AND EXECUTION'
        setup_cmd = ''
        if args.setup:
            os.chmod(args.setup, 0555)
            setup_cmd = 'source ./' + args.setup + ' && '

        if os.path.isfile(os.path.basename(args.exe)):
            os.chmod(os.path.basename(args.exe), 0555)

        # run_cmd = args.exe

        # If it's a local file, we need to do ./ for some reason...
        # But we must determine this AFTER running setup script,
        # can't do it beforehand
        run_cmd = "if [[ -e {exe} ]];then /usr/bin/time -v ./{exe} {args};else /usr/bin/time -v {exe} {args};fi"
        run_args = ' '.join(args.args) if args.args else ''
        run_cmd = run_cmd.format(exe=args.exe, args=run_args)
        print 'Contents of dir before running:'
        print os.listdir(os.getcwd())
        total_cmd = setup_cmd + run_cmd
        print "Running:", total_cmd

        # check_call(total_cmd, shell=True)

        process = Popen(total_cmd, shell=True)

        if do_status:
            status_dict = {'process': None, 'logs': [], 'end': None}
            start_fields = ['pid', 'name', 'create_time']
            running_fields = ['name', 'exe', 'status',
                              'cpu_percent', 'cpu_times',
                              'memory_full_info', 'memory_percent', 'io_counters']
            the_proc = psutil.Process(process.pid)
            # Add process starting info
            if the_proc:
                status_dict['process'] = the_proc.as_dict(start_fields)
            while process.poll() is None:
                # need to loop through all children to find the interesting ones
                # as multiple layers, and parents don't accumulate child stats
                # add a filter for running only?
                children_dicts = [child.as_dict(running_fields)
                                  for child in the_proc.children(recursive=True)]
                status_dict['logs'].append({'time': time.time(), 'processes': children_dicts})
                time.sleep(15)
            status_dict['end'] = {'time': time.time(), 'return': process.returncode}

            with open("status.json", "w") as status_file:
                json.dump(status_dict, status_file)

            args.copyFromLocal = args.copyFromLocal or []
            args.copyFromLocal.append(['status.json', '/hdfs/user/ra12451/status_%s.json' % (time.strftime("%d_%b_%y_%H%M%S"))])

        # HOW TO RETURN process.returncode ???
        if int(process.returncode) != 0:
            raise RuntimeError('Process %s exited with error code %d' % (total_cmd, int(process.returncode)))

        print 'In current dir:'
        print os.listdir(os.getcwd())

        # Copy files from worker node area to /hdfs or /storage
        # ---------------------------------------------------------------------
        if args.copyFromLocal:
            print 'POST EXECUTION: Copy to HDFS:'
            copy_list = []
            for (source, dest) in args.copyFromLocal:
                # handle globbing
                for match in glob.iglob(source):
                    copy_list.append((match, dest))

            for (source, dest) in copy_list:
                if not os.path.exists(source):
                    print 'File {0} does not exist - cannot copy to {1}'.format(source, dest)
                else:
                    print source, "-->", dest
                    if dest.startswith('/hdfs'):
                        dest_folder = os.path.dirname(dest)
                        if not os.path.exists(dest_folder):
                            dest_folder = dest_folder.replace('/hdfs', '')
                            check_call(['hdfs', 'dfs', '-mkdir', '-p', dest_folder])
                        dest = dest.replace('/hdfs', '')
                        check_call(['hadoop', 'fs', '-copyFromLocal', '-f', source, dest])
                    else:
                        if os.path.isfile(source):
                            shutil.copy2(source, dest)
                        elif os.path.isdir(source):
                            shutil.copytree(source, dest)

    finally:
        # Cleanup
        # ---------------------------------------------------------------------
        print 'CLEANUP'
        os.chdir('..')
        shutil.rmtree(tmp_dir)

    return 0


def find_proc(process_name):
    the_proc = None
    for p in psutil.process_iter():
        if any([process_name in x for x in p.cmdline()]):
            print 'Found process:', p.name(), p.exe(), p.cmdline()
            the_proc = p
    return the_proc


if __name__ == "__main__":
    sys.exit(run_job())
