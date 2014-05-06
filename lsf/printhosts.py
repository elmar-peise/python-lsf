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
            if host["maxmem"]:
                total = host["maxmem"]
                free = total - used
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
                c = "g" if job["user"] == whoami else 0
                l += " %2d*" % job["exec_host"][host["host_name"]]
                l += color(job["user"].ljust(8), c)
                if wide:
                    if job["mem"]:
                        l += format_mem(job["mem"])
                    if job["%complete"] and job["runlimit"]:
                        ptime = job["%complete"]
                        c = "r" if ptime > 90 else "y" if ptime > 75 else 0
                        l += color("%3d" % ptime, c) + "% "
                        l += format_duration(job["runlimit"])
        print(l, file=file)
        file.flush()
        continue

        # host Name
        hostname = host["host_name"] if host["host_name"] else ""
        if not wide:
            if len(hostname) >= lens["host_name"]:
                hostname = hostname[:lens["host_name"] - 2] + "*"
            hostname += " "
        l += hostname.ljust(lens["host_name"])
        # Status
        if host["stat"] == "PEND":
            c = "r"
        elif host["stat"] == "RUN":
            c = "g"
        else:
            c = "y"
        l += color((host["stat"] + " ").ljust(lens["stat"]), c)
        # User
        username = host["user"]
        if host["user"] == whoami:
            c = "g"
        else:
            c = 0
        l += color((username + " ").ljust(lens["user"]), c)
        # Project
        if wide:
            l += host["queue"].ljust(lens["queue"])
            l += host["proj_name"].ljust(lens["proj_name"])
        # Wait/Runtime
        if host["stat"] == "PEND":
            t = time() - host["submit_time"]
        else:
            t = host["run_time"]
        s = format_duration(t)
        l += s.rjust(lens["time"])
        # Resources
        # Time
        if host["stat"] == "RUN":
            if host["%complete"]:
                runlimit = host["run_time"] / host["%complete"] * 100
                # rounding
                if runlimit > 10 * 60 * 60:
                    runlimit = round(runlimit / (60 * 60)) * 60 * 60
                else:
                    runlimit = round(runlimit / 60) * 60
                l += "  " + format_duration(runlimit)
                ptime = int(host["%complete"])
                s = ("%d" % ptime).rjust(3)
                c = "r" if ptime > 90 else "y" if ptime > 75 else 0
                l += " " + color(s, c) + "%t"
            if host["memlimit"] and host["mem"]:
                pmem = int(100 * host["mem"] / host["memlimit"])
                l += " " + ("%d%%m" % pmem).rjust(5)
            if host["mem"]:
                l += " " + format_mem(host["mem"]).rjust(9)
            else:
                l += "          "
            if host["exec_host"]:
                if wide or len(host["exec_host"]) == 1:
                    d = host["exec_host"]
                else:
                    d = defaultdict(int)
                    for key, val in host["exec_host"].iteritems():
                        d[re.match("(.*?)\d+", key).groups()[0]] += val
                for key, val in d.iteritems():
                    c = "r" if val >= 100 else "y" if val >= 20 else 0
                    l += color(" %3d" % val, c) + "*%s" % key
        print(l, file=file)
        # if host["stat"] in ("EXIT", "DONE"):
        #     print(sorted([(k, v) for k, v in host.iteritems() if v]))
        sys.stdout.flush()
