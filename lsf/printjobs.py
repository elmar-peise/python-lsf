#!/usr/bin/env python
from __future__ import print_function, division

from utility import color, format_duration, format_mem

import os
import sys
import re
from time import time
from subprocess import check_output
from collections import defaultdict


def printjobs(jobs, long=False, wide=False, title=None, header=True,
              file=sys.stdout):
    """list the jobs"""
    if len(jobs) == 0:
        return
    # begin output
    whoami = os.getenv("USER")
    lens = {
        "jobid": 14,
        "name": 20,
        "stat": 6,
        "user": 10,
        "time": 12,
    }
    if wide:
        lens["name"] = 32
        lens["queue"] = 8
        lens["project"] = 8
    if header:
        h = "".join(n.ljust(lens[n]) for n in ("jobid", "name", "stat",
                                               "user"))
        if wide:
            h += "".join(n.ljust(lens[n]) for n in ("queue", "project"))
        h += "wait/runtime".rjust(lens["time"]) + "  resources"
        h = h.upper()
        if title:
            h += "  (( " + title + " ))"
        print(h, file=file)
    for job in jobs:
        # jobid
        l = (job["jobid"] + " ").ljust(lens["jobid"])
        # Job Name
        jobname = job["name"] if job["name"] else ""
        if not wide:
            if len(jobname) >= lens["name"]:
                jobname = jobname[:lens["name"] - 2] + "*"
        l += jobname.ljust(lens["name"])
        # Status
        stat = job["stat"]
        c = "r" if stat == "PEND" else "g" if stat == "RUN" else "y"
        l += color(stat.ljust(lens["stat"]), c)
        # User
        c = "g" if sumjob["user"] == whoami else 0
        l += color((sumjob["user"] + " ").ljust(lens["user"]), c)
        # Queue and Project
        if wide:
            l += job["queue"].ljust(lens["queue"])
            l += job["project"].ljust(lens["project"])
        # Wait/Runtime
        if job["stat"] == "PEND":
            t = time() - job["submit_time"]
        else:
            t = job["run_time"]
        s = format_duration(t)
        l += s.rjust(lens["time"])
        # Resources
        # Time
        if job["runlimit"]:
            l += "  " + format_duration(job["runlimit"])
        if job["%complete"]:
            ptime = int(job["%complete"])
            c = "r" if ptime > 90 else "y" if ptime > 75 else 0
            l += " " + color("%3d" % ptime, c) + "%t"
        # Memory
        if job["memlimit"] and job["mem"] and job["slots"]:
            memlimit = job["memlimit"] * job["slots"]
            pmem = int(100 * job["mem"] / memlimit)
            c = "r" if pmem > 90 else "y" if pmem > 75 else 0
            l += " " + color("%3d" % pmem, c) + "%m"
        if job["mem"]:
            l += " " + format_mem(job["mem"]).rjust(9)
        else:
            l += "          "
        # Hosts
        if job["exec_host"]:
            if wide or len(job["exec_host"]) == 1:
                d = job["exec_host"]
            else:
                d = defaultdict(int)
                for key, val in job["exec_host"].iteritems():
                    d[re.match("(.*?)\d+", key).groups()[0]] += val
            for key, val in d.iteritems():
                c = "r" if val >= 100 else "y" if val >= 20 else 0
                l += color(" %3d" % val, c) + "*%s" % key
        print(l, file=file)
        # if job["stat"] in ("EXIT", "DONE"):
        #     print(sorted([(k, v) for k, v in job.iteritems() if v]))
        file.flush()
