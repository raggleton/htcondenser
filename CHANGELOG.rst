Changelog
=========

v0.3.0 (27th October 2016)
--------------------------

- Globbing (aka wildcards) now supported in input_output file args. This allows for e.g. `CMSSW_*/src/Hist.root`

- `DAGstatus`  now has options for colouring based on the detailed status column. This means one can now differentiate between idle and running jobs


v0.2.0 (14th June 2016)
-----------------------

- Move setup to ``pip`` with big thanks to @kreczko https://github.com/raggleton/htcondenser/pull/4:

    - Move python classes out of ``htcondenser/core`` into just ``htcondenser``

    - Rename/move ``exe/DAGStatus.py`` to ``bin/DAGStatus`` to aid ``pip`` deployment

- Use hadoop command to mkdir on HDFS, not ``os.makedirs``

- Add check for output file on worker node before transfer

- Add in check to make output dir on HDFS if it doesn't already exist

- Change the readthedocs theme


v0.1.0 (12th May 2016)
----------------------

- Initial release.

- Includes classes for jobs and dags.

- Handles transfers to/from HDFS.

- DAG monitoring tool included.

- Basic documentation on readthedocs with examples.