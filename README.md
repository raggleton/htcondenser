# htcondenser

## What is it?

A simple library for submitting simple jobs (& DAGs in future) on the Bristol machines.

Designed to allow easy setting up of jobs and deployment on worker node, without the user worrying too much about writing custom scripts, or copying across files to HDFS.

Note that this probably won't work for more custom or complicated workflows, but may be a useful starting point.

## What do I need?

An area on `/hdfs/users` that you have read/write permission. Python >= 2.6 (default on soolin), but untested with Python 3.

For building the docs, you'll need [sphinx](http://www.sphinx-doc.org/en/stable/index.html) (`pip install sphinx`).

## How do I get/install it?

For now, run `setup.sh`. This will just add the current directory to `PYTHONPATH`. This required every time you login (or add to `~/.bashrc`/`~/.bash_profile`). Needs a better way (pip...).

## How do I get started?

Look in the `examples` directory. There are several directories, each designed to show off some features:

- [`simple_job/simple_job.py`](examples/simple_job/simple_job.py): submits 3 jobs, each running a simple shell script, but with different arguments. Designed to show off how to use the `htcondenser` classes.

- [`simple_exe_job/simple_exe_job.py`](examples/simple_exe_job/simple_exe_job.py): submits a job using a user-compiled exe, `showsize`. Before submission, you must compile the exe: `gcc showsize.c -o showsize`. Test it runs ok by doing: `./showsize`.

- [`simple_cmssw_job/simple_cmssw_job.py`](examples/simple_cmssw_job/simple_cmssw_job.py): setup a CMSSW environment and run `edmDumpEventContent` inside it.

- [`dag_example/dag_example.py`](dag_example/dag_example.py): run a DAG (directed-acyclic-graph) - this allows you to schedule jobs that rely on other jobs to run first.

If you want to run all examples, use [`examples/runAllExamples.sh`](examples/runAllExamples.sh).

## A bit more detail

The aim of this library is to make submitting jobs to HTCondor a breeze. In particular, it is designed to make the setting up of libraries & programs, as well as transport of any input/output files, as simple as possible.

Each job is represented by a `Job` object. A group of `Job`s is governed by a `JobSet` object. All `Job`s in the group share common settings: they run the same executable, same setup commands, output to same log directory, and require the same resources. 1 `JobSet` = 1 HTCondor job description file. Individual `Job`s within a `JobSet` can have different arguments, and different input/output files.

## But I want XYZ!

Log an Issue, make a PR, or email me directly.

## I want to help

Take a look at [CONTRIBUTING.md](CONTRIBUTING.md).