#!/usr/bin/env python
"""
Code to present the DAGman status output in a more user-friendly manner.

Add this directory to your PATH to run DAGstatus.py from anywhere.
"""


import argparse
import logging
import os
from collections import OrderedDict, namedtuple
import json


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


def strip_comments(line):
    return line.replace("/*", "").replace("*/", "").strip()


def strip_doublequotes(line):
    return line.replace('"', '')


class TColors:
    """Handle terminal coloured output.
    Use TColors.COLORS['ENDC'] to stop the colour.

    Also returns colours based on job/DAG status, and for various other parts.

    e.g.:
    >>> print TColors.COLORS['GREEN'] + "It's not easy being green" + TColors.COLORS['ENDC']

    or better:

    TColors.printc("It's not easy being green", TColors.COLORS['GREEN'])
    """
    fmt_dict = {}
    with open(os.path.join(os.path.dirname(__file__), 'DAGstatus_config.json')) as js:
        fmt_dict = json.load(js)

    COLORS = fmt_dict['colors']
    for k, v in COLORS.iteritems():
        COLORS[k] = str(v).decode('string_escape')
    STATUS_COLORS = fmt_dict['statuses']
    FMT_COLORS = fmt_dict['formatting']

    @classmethod
    def printc(cls, text, color_code):
        """Print coloured output, and reset the colour after the output"""
        print color_code + text + cls.COLORS['ENDC']

    @classmethod
    def status_color(cls, status):
        """Return color code based on status string.
        If no matching status string, returns end-color.
        """
        if status in cls.fmt_dict['statuses'].keys():
            try:
                return ''.join([cls.COLORS[part.strip()] for part in cls.STATUS_COLORS[status].split("+")])
            except KeyError:
                log.exception('Cannot find colour with name %s', cls.fmt_dict['statuses'][status])
        else:
            return cls.COLORS['ENDC']

    @classmethod
    def formatting_color(cls, section):
        """Return color code based on section.
        If no matching section label, returns end-color.
        """
        if section in cls.FMT_COLORS.keys():
            try:
                return ''.join([cls.COLORS[part.strip()] for part in cls.FMT_COLORS[section].split("+")])
            except KeyError:
                log.exception('Cannot find colour with name %s', cls.FMT_COLORS[section])
        else:
            return cls.COLORS['ENDC']


# To hold info about a given line
Line = namedtuple('Line', 'key value comment')


class ClassAd(object):
    """Base class for ClassAds."""
    def __init__(self):
        pass


class DagStatus(ClassAd):
    """Class to describe status of DAG as a whole."""
    def __init__(self,
                 timestamp,
                 dag_status,
                 nodes_total,
                 nodes_done,
                 nodes_pre,
                 nodes_queued,
                 nodes_post,
                 nodes_ready,
                 nodes_unready,
                 nodes_failed,
                 job_procs_held,
                 job_procs_idle,
                 node_statuses=None):
        super(ClassAd, self).__init__()
        self.timestamp = timestamp
        self.dag_status = strip_doublequotes(dag_status)
        self.nodes_total = int(nodes_total)
        self.nodes_done = int(nodes_done)
        self.nodes_pre = int(nodes_pre)
        self.nodes_queued = int(nodes_queued)
        self.nodes_post = int(nodes_post)
        self.nodes_ready = int(nodes_ready)
        self.nodes_unready = int(nodes_unready)
        self.nodes_failed = int(nodes_failed)
        self.job_procs_held = int(job_procs_held)
        self.job_procs_idle = int(job_procs_idle)
        self.nodes_done_percent = "{0:.1f}".format(100. * self.nodes_done / self.nodes_total)
        self._job_procs_running = 0
        # self.job_procs_running = 0
        self.node_statuses = node_statuses if node_statuses else []

    @property
    def job_procs_running(self):
        return len([n for n in self.node_statuses
                    if n.node_status == "STATUS_SUBMITTED" and
                    n.status_details == "not_idle"])

    @property
    def nodes_running_percent(self):
        return "{0:.1f}".format(100. * self.job_procs_running / self.nodes_total)


class NodeStatus(ClassAd):
    """Class to describe state of individual job node in the DAG."""
    def __init__(self,
                 node,
                 node_status,
                 status_details,
                 retry_count,
                 job_procs_queued,
                 job_procs_held):
        super(NodeStatus, self).__init__()
        self.node = strip_doublequotes(node)
        self.node_status = strip_doublequotes(node_status)
        self.status_details = status_details.replace('"', '')
        self.retry_count = int(retry_count)
        self.job_procs_queued = int(job_procs_queued)
        self.job_procs_held = int(job_procs_held)


class StatusEnd(ClassAd):
    """Class to describe state of reporting."""
    def __init__(self,
                 end_time,
                 next_update):
        super(StatusEnd, self).__init__()
        self.end_time = strip_doublequotes(end_time)
        self.next_update = strip_doublequotes(next_update)


def process(status_filename, only_summary):
    """Main function to process the status file

    Parameters
    ----------
    status_filename : str
        Name of status file to process.

    only_summary : bool
        If True, only prints out summary of DAG. Otherwise prints out info about
        each job in DAG.

    Raises
    ------
    KeyError
        If processing encounters block with unknown type
        (i.e. not DagStatus, NodeStatus or StatusEnd).
    """
    dag_status, node_statuses, status_end = interpret_status_file(status_filename)
    print_table(status_filename, dag_status, node_statuses, status_end, only_summary)


def interpret_status_file(status_filename):
    """Interpret the DAG status file, return objects with DAG & node statuses.

    Parameters
    ----------
    status_filename : str
        Filename of status file to interpret.

    Returns
    -------
    DagStatus, list[NodeStatus], StatusEnd
        Objects with info abotu DAG, all nodes, and end info (update times).

    """
    dag_status = None
    node_statuses = []
    status_end = None

    with open(status_filename) as sfile:
        contents = {}
        store_contents = False
        for line in sfile:
            if line.startswith("[") or "}" in line:
                store_contents = True
                continue
            elif line.startswith("]"):
                log.debug(contents)
                # do something with contents here, depending on Type key
                if contents['Type'].value == 'DagStatus':
                    dag_status = generate_DagStatus(contents)
                elif contents['Type'].value == 'NodeStatus':
                    node = generate_NodeStatus(contents)
                    node_statuses.append(node)
                elif contents['Type'].value == 'StatusEnd':
                    status_end = generate_StatusEnd(contents)
                else:
                    log.debug(contents)
                    log.debug(contents['Type'])
                    raise KeyError("Unknown block Type")
                contents = {}
                store_contents = False
                continue
            elif "{" in line:
                store_contents = False
                continue
            elif store_contents:
                # Actually handle the line
                line_parsed = interpret_line(line)
                contents[line_parsed.key] = line_parsed
    dag_status.node_statuses = node_statuses

    return dag_status, node_statuses, status_end


def interpret_line(line):
    """Interpret raw string corresponding to a line, then return as Line obj.

    Parameters
    ----------
    line : str
        Line to be interpreted.
    """
    raw = line.replace('\n', '').strip()
    parts = [x.strip() for x in raw.split('=')]
    other = [x.strip() for x in parts[1].split(";")]
    value = strip_doublequotes(other[0])
    if len(other) == 2:
        comment = strip_doublequotes(strip_comments(other[1]))
    else:
        comment = ''
    return Line(key=parts[0], value=value, comment=comment)


def generate_DagStatus(contents):
    """Create, fill, and return a DagStatus object with info in contents dict."""
    return DagStatus(timestamp=contents['Timestamp'].comment,
                     dag_status=contents['DagStatus'].comment,
                     nodes_total=contents['NodesTotal'].value,
                     nodes_done=contents['NodesDone'].value,
                     nodes_pre=contents['NodesPre'].value,
                     nodes_queued=contents['NodesQueued'].value,
                     nodes_post=contents['NodesPost'].value,
                     nodes_ready=contents['NodesReady'].value,
                     nodes_unready=contents['NodesUnready'].value,
                     nodes_failed=contents['NodesFailed'].value,
                     job_procs_held=contents['JobProcsHeld'].value,
                     job_procs_idle=contents['JobProcsIdle'].value)


def generate_NodeStatus(contents):
    """Create, fill, and return a NodeStatus object with info in contents dict."""
    return NodeStatus(node=contents['Node'].value,
                      node_status=contents['NodeStatus'].comment,
                      status_details=contents['StatusDetails'].value,
                      retry_count=contents['RetryCount'].value,
                      job_procs_queued=contents['JobProcsQueued'].value,
                      job_procs_held=contents['JobProcsHeld'].value)


def generate_StatusEnd(contents):
    """Create, fill, and return a StatusEnd object with info in contents dict."""
    return StatusEnd(end_time=contents['EndTime'].comment,
                     next_update=contents['NextUpdate'].comment)


def print_table(status_filename, dag_status, node_statuses, status_end, only_summary):
    """Print a pretty-ish table with important info

    Parameters
    ----------
    status_filename : str
        Filename of status file

    dag_status : DagStatus
        Object holding info about overall status of DAG.

    node_statuses : list[NodeStatus]
        List of objects holding info about each job.

    status_end : StatusEnd
        Object holding info about reporting.

    only_summary : bool
        If True, only prints out summary of DAG. Otherwise prints out info about
        each job in DAG.
    """
    # Here we auto-create the formatting strings for each row,
    # and auto-size each column based on max size of contents

    # For info about each node:
    job_dict = OrderedDict()  # holds column title as key and object attribute name as value
    job_dict["Node"] = "node"
    job_dict["Status"] = "node_status"
    job_dict["Retries"] = "retry_count"
    job_dict["Detail"] = "status_details"
    # Auto-size each column - find maximum of column header and column contents
    job_col_widths = [max([len(str(getattr(x, v))) for x in node_statuses] + [len(k)])
                      for k, v in job_dict.iteritems()]
    # make formatter string to be used for each row, auto calculates number of columns
    # note that the %d are required for python 2.6, which doesn't allow just {}
    job_format_parts = ["{%d:<%d}" % (i, l) for i, l in zip(range(len(job_dict.keys())), job_col_widths)]
    job_format = " | ".join(job_format_parts)
    job_header = job_format.format(*job_dict.keys())

    # For info about summary of all jobs:
    summary_dict = OrderedDict()
    summary_dict["DAG Status"] = "dag_status"
    summary_dict["Total"] = "nodes_total"
    summary_dict["Queued"] = "nodes_queued"
    summary_dict["Idle"] = "job_procs_idle"
    summary_dict["Running"] = "job_procs_running"
    summary_dict["Running %"] = "nodes_running_percent"
    summary_dict["Failed"] = "nodes_failed"
    summary_dict["Done"] = "nodes_done"
    summary_dict["Done %"] = "nodes_done_percent"
    summary_col_widths = [max(len(str(getattr(dag_status, v))), len(k))
                          for k, v in summary_dict.iteritems()]
    summary_format_parts = ["{%d:<%d}" % (i, l) for i, l in zip(range(len(summary_dict.keys())), summary_col_widths)]
    summary_format = "  |  ".join(summary_format_parts)
    summary_header = summary_format.format(*summary_dict.keys())

    # Now figure out how many char columns to occupy for the *** and ---
    columns = len(summary_header) if only_summary else max(len(job_header), len(summary_header))
    columns += 1
    term_rows, term_columns = os.popen('stty size', 'r').read().split()
    term_rows = int(term_rows)
    term_columns = int(term_columns)
    if columns > term_columns:
        columns = term_columns

    # Now actually print the table
    TColors.printc(status_filename, TColors.formatting_color('FILENAME'))

    if not only_summary:
        # Print info for each job.
        print "~" * columns
        print job_header
        print "-" * columns
        for n in node_statuses:
            TColors.printc(job_format.format(*[n.__dict__[v] for v in job_dict.values()]),
                           TColors.status_color(n.node_status))
        print "-" * columns
    # print summary of all jobs
    print "~" * columns
    print summary_header
    print "-" * columns
    TColors.printc(summary_format.format(*[getattr(dag_status, v) for v in summary_dict.values()]),
                   TColors.status_color(dag_status.dag_status.split()[0]))
    if not only_summary:
        # print time of next update
        print "-" * columns
        print "Status recorded at:", status_end.end_time
        TColors.printc("Next update:        %s" % status_end.next_update,
                       TColors.formatting_color('NEXT_UPDATE'))
    print "~" * columns


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose",
                        help="enable debugging mesages",
                        action='store_true')
    parser.add_argument("-s", "--summary",
                        help="only printout very short summary of all jobs",
                        action='store_true')
    parser.add_argument("statusFile",
                        help="DAG status file(s), separated by spaces",
                        nargs="*")
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    if len(args.statusFile) == 0:
        parser.print_help()
        exit()

    for f in args.statusFile:
        process(f, args.summary)
