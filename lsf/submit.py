#!/usr/bin/env python
from __future__ import print_function, division

from job import Job
from error import LSFError

import sys
import re
from subprocess import Popen, PIPE


def submit(data, shell=False):
    """Submit a job to LSF"""
    if "Command" not in data:
        print("no command given", file=sys.stderr)
        return False
    if "Job Name" in data:
        data["-J"] = data["Job Name"]
    if "Output File" in data:
        data["-o"] = data["Output File"]
    if "-o" not in data and "-J" in data:
        data["-o"] = data["-J"] + ".%J.out"
    if "Error File" in data:
        data["-e"] = data["Error File"]
    if "Mail" in data:
        data["-u"] = data["Mail"]
    if "Notify when job ends" in data and data["Notify when job ends"]:
        data["-N"] = True
    if "Notify when job begins" in data and data["Notify when job begins"]:
        data["-B"] = True
    if "Exclusive Execution" in data and data["Exclusive Execution"]:
        data["-x"] = True
    if "Processors Requested" in data:
        data["-n"] = str(int(data["Processors Requested"]))
    if "RUNLIMIT" in data:
        data["-W"] = str(data["RUNLIMIT"] // 60)
    if "MEMLIMIT" in data:
        data["-M"] = str(data["MEMLIMIT"] // 1024)
    if "CORELIMIT" in data:
        data["-C"] = str(data["CORELIMIT"] // 1024)
    if "Project" in data:
        data["-P"] = data["Project"]
    if "Resource Request" in data:
        if "-R" in data:
            data["-R"] = [data["-R"]]
        else:
            data["-R"] = []
        data["-R"].append(data["Resource Request"])
    if "Dependency Condition" in data:
        data["-w"] = data["Dependency Condition"]
    cmd = ["bsub"]
    for key, value in data.iteritems():
        if key[0] != "-":
            continue
        val = data[key]
        if type(val) is bool:
            cmd += [key]
        elif type(val) is str:
            cmd += [key, val]
        elif type(val) is list:
            for v in val:
                cmd += [key, str(v)]
    if shell:
        script = '#!/bin/bash -l\n'
    else:
        script = ''
    script += data["Command"]
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    out, err = p.communicate(script)
    out = out.decode()
    err = err.decode()
    match = re.search("Job <(.*?)> is submitted", out)
    if match:
        return Job(str(match.groups()[0]))
    else:
        match = re.search("Error: (.*)\n", err)
        if match:
            raise LSFError(1, match.groups()[0])
        else:
            raise LSFError(1, err)
