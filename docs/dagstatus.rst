DAGstatus
=========

A handy tool for monitor jobs in a DAG: `DAGstatus.py <https://github.com/raggleton/htcondenser/blob/master/htcondenser/exe/DAGstatus.py>`_


.. image:: DAGstatus_full.png


Usage
-----
Ensure that you set the `DAGMan.status_filename` attribute. Then pass that filename to `DAGstatus.py`.

If you are not using the `htcondenser` library then ensure you have the following line in your DAG description file: ::

    NODE_STATUS_FILE <filename> <refresh interval in seconds>

See `2.10.12 Capturing the Status of Nodes in a File <https://research.cs.wisc.edu/htcondor/manual/current/2_10DAGMan_Applications.html#SECTION0031012000000000000000>`_ for more details.

General usage instructions:::

    usage: DAGstatus.py [-h] [-v] [-s] [statusFile [statusFile ...]]

    Code to present the DAGman status output in a more user-friendly manner. Add
    this directory to PATH to run DAGstatus.py it from anywhere.

    positional arguments:
      statusFile     name(s) of DAG status file(s), separated by spaces

    optional arguments:
      -h, --help     show this help message and exit
      -v, --verbose  enable debugging mesages
      -s, --summary  only printout very short summary of all jobs


Installation
------------

Running `setup.sh <https://github.com/raggleton/htcondenser/blob/master/setup.sh>`_ will add the script to your `$PATH`. To avoid having to remember to run it each time you login, you can manually add the `exe` directory to your `$PATH` in `~/.bashrc` or `~/.bash_profile`.


