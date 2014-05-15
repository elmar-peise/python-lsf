#!/usr/bin/env python
from __future__ import print_function, division

from utility import color, format_duration, format_mem
from groupjobs import groupjobs

import os
import sys
import re
from time import time
from subprocess import check_output
from collections import defaultdict


def printhosts(hosts, jobs=[], wide=False, header=True, file=sys.stdout):
    """list the hosts"""
    if len(hosts) == 0:
        return
    jobsbyhost = groupjobs(jobs, "exec_host")
    # begin output
    screencols = int(check_output(["tput", "cols"]))
    whoami = os.getenv("USER")
    lens = {
        "host_name": 14,
        "status": 8,
    }
    if header:
        h = "".join(n.ljust(lens[n]) for n in ("host_name", "status"))
        h += " cpus     mem (free/total)"
        if wide:
            h += "  " + "model".ljust(14)
        h = h.upper()
        print(h, file=file)
    for host in hosts:
        # host_name
        l = host["host_name"].ljust(lens["host_name"])
        # status
        if host["status"] == "ok":
            l += color("ok".ljust(lens["status"]), "g")
        elif "closed_" in host["status"]:
            l += color(host["status"][7:].ljust(lens["status"]), "r")
        else:
            l += color(host["status"].ljust(lens["status"]), "y")
        # cpus
        total = host["max"]
        used = host["njobs"]
        free = total - used
        c = "r" if free == 0 else "y" if free < total else 0
        l += color("%2d" % free, c) + "/%2d" % total
        # mem
        if "mem" in host["load"]:
            free, used = host["load"]["mem"]
            total = free + used
            if "maxmem" in host and host["maxmem"]:
                total = host["maxmem"]
            f = used / total
            c = "r" if f > .75 else "y" if f > .5 else 0
            l += "  " + format_mem(free, c) + "/" + format_mem(total)
        if wide:
            l += "  " + host["model"].ljust(14)
        l += " "
        if host["rsv"] > 0:
            l += " %2d*" % host["rsv"] + color("reserved", "y")
        if host["host_name"] in jobsbyhost:
            jobs = jobsbyhost[host["host_name"]]
            for job in jobs:
                times = color("x", "r") if job["exclusive"] else "*"
                c = "g" if job["user"] == whoami else 0
                l += " %2d" % job["exec_host"][host["host_name"]] + times
                l += color(job["user"].ljust(8), c)
                if wide:
                    if job["mem"]:
                        l += format_mem(job["mem"])
                    else:
                        l += "         "
                    if job["%complete"] and job["runlimit"]:
                        ptime = job["%complete"]
                        c = "r" if ptime > 90 else "y" if ptime > 75 else 0
                        l += color("%3d" % ptime, c) + "% "
                        l += format_duration(job["runlimit"])
        print(l, file=file)
        file.flush()
