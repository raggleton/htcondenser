# htcondenser

## What is it?

A simple library for submitting simple jobs (& DAGs in future) on the Bristol machines.

Designed to allow easy setting up of jobs and deployment on worker node, without the user worrying too much about writing custom scripts, or copying across files to HDFS.

Note that this probably won't work for more custom or complicated workflows, but may be a useful starting point.

## What do I need?

An area on `/hdfs/users` that you have read/write permission. Python >= 2.6 (default on soolin), but untested with Python 3.

## How do I get/install it?

For now, run `setup.sh`. This will just add the current directory to `PYTHONPATH`. This required every time you login (or add to `~/.bashrc`/`~/.bash_profile`). Needs a better way (pip...).

## How do I get started?

Look in the `examples` directory. There are several directories, each designed to show off some features:

- [`simple_job/simple_job.py`](examples/simple_job/simple_job.py): submits 3 jobs, each running a simple shell script, but with different arguments. Designed to show off how to use the `htcondenser` classes.

- [`simple_exe_job/simple_exe_job.py`](examples/simple_exe_job/simple_exe_job.py): submits a job using a user-compiled exe, `showsize`. Before submission, you must compile the exe: `gcc showsize.c -o showsize`. Test it runs ok by doing: `./showsize`.

- [`simple_cmssw_job/simple_cmssw_job.py`](examples/simple_cmssw_job/simple_cmssw_job.py): setup a CMSSW environment and run `edmDumpEventContent` inside it.

## But I want XYZ!

Log an Issue, make a PR, or email me directly.

## I want to help

Take a look at [CONTRIBUTING.md](CONTRIBUTING.md).