#!/usr/bin/env python

"""
Quick n dirty plotter for status JSON files to showoff info.

Usage:

python analysis.py status.json status2.json ...

Requires plot.ly package (via pip)
"""


import sys
import json
import plotly
import plotly.graph_objs as go

def main():
    log_dicts = []
    for f in sys.argv[1:]:
        with open(f) as j:
            log_dicts.append(json.load(j))

    log_dicts.sort(key=lambda x: x['process']['create_time'])

    start_time = float(log_dicts[0]['process']['create_time'])

    cpu_traces = []
    ram_traces = []

    for ind, log_dict in enumerate(log_dicts):

        cpu_data = {}
        ram_data = {}

        for entry in log_dict['logs']:

            tdiff = float(entry['time']) - start_time

            for p in entry['processes']:
                if p['status'] == 'running':
                    pname = p['name'] + str(ind)
                    if pname not in cpu_data:
                        cpu_data[pname] = {'x': [], 'y': []}
                        ram_data[pname] = {'x': [], 'y': []}
                    cpu_data[pname]['x'].append(tdiff)
                    cpu_data[pname]['y'].append(float(p['cpu_percent']))
                    ram_data[pname]['x'].append(tdiff)
                    ram_data[pname]['y'].append(float(p['memory_percent']))

        for k, v in cpu_data.iteritems():
            cpu_traces.append(go.Scatter(name=k+' (CPU)', x=v['x'], y=v['y'], xaxis='x', yaxis='y2'))

        for k, v in ram_data.iteritems():
            ram_traces.append(go.Scatter(name=k+' (RAM)', x=v['x'], y=v['y'], xaxis='x', yaxis='y'))

    layout = dict(
        xaxis=dict(title='Time from start [seconds]'),
        yaxis2=dict(title='% CPU usage',
                   rangemode='tozero',
                   domain=[0.6, 1]),
        yaxis=dict(title='% RAM usage',
                    rangemode='tozero',
                    domain=[0, 0.5]),
        legend=dict(traceorder='reversed')
    )

    fig = go.Figure(data=ram_traces+cpu_traces, layout=layout)
    plotly.offline.plot(fig, filename='analysis.html', auto_open=False)


if __name__ == '__main__':
    main()