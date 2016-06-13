"""A simple library for submitting jobs on the DICE system at Bristol."""
from htcondenser.jobset import JobSet
from htcondenser.job import Job
from htcondenser.dagman import DAGMan
from htcondenser.common import FileMirror
# flake8: noqa
# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

"""Hold all the main classes, as well as helper functions/classes."""
# from htcondenser.core.job_classes import JobSet, Job, DAGMan
