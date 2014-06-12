#!/usr/bin/env python
from __future__ import print_function, division

from utility import color, format_duration, format_mem, findstringpattern

import os
import sys
import re
from time import time
from subprocess import check_output
from collections import defaultdict


def printjobssum(jobs, long=False, wide=False, title=None, header=True,
                 file=sys.stdout):
    """summarize the jobs in one line"""
    if len(jobs) == 0:
        return
    whoami = os.getenv("USER")
    sumjob = {}
    for key in jobs[0]:
        if key in ("job_name", "job_description", "input_file", "output_file",
                   "error_file", "output_dir", "sub_cwd", "exec_home",
                   "exec_cwd", "exit_reson", "application", "command",
                   "pre_exec_command", "post_exec_command",
                   "resize_notification_command", "effective_resreq"):
            # find string pattern
            sumjob[key] = findstringpattern([job[key] for job in jobs
                                             if job[key]])
        elif key in ("runlimit", "swaplimit", "stacklimi", "memlimit",
                     "filelimit", "processlimit", "corelimit", "run_time",
                     "swap", "slots", "mem", "max_mem", "avg_mem",
                     "nexec_host"):
            # sum
            sumjob[key] = sum(job[key] for job in jobs if job[key])
        elif key in ("%complete", "job_priority", "idle_factor"):
            # compute average
            pcomp = [job[key] for job in jobs if job[key]]
            if pcomp:
                sumjob[key] = sum(pcomp) / len(pcomp)
            else:
                sumjob[key] = None
        elif key in ("exec_host", "rsvd_host"):
            # collect host counts
            sumjob[key] = defaultdict(int)
            for job in jobs:
                if job[key]:
                    for host, count in job[key].iteritems():
                        sumjob[key][host] += count
        elif key == "pids":
            # collect
            sumjob[key] = sum((job[key] for job in jobs if job[key]), [])
        elif key == "pend_reason":
            # collect
            sumjob[key] = []
            for job in jobs:
                if job[key] and job[key] not in sumjob[key]:
                    sumjob[key].append(job[key])
        else:
            # collect and count
            sumjob[key] = defaultdict(int)
            for job in jobs:
                sumjob[key][job[key]] += 1
    # begin output
    namelen = max(map(len, (job["name"] for job in jobs)))
    lens = {
        "name": min(20, max(6, namelen + 1)),
        "stat": 12,
        "user": 10,
        "time": 12,
        "title": 10
    }
    if wide:
        lens["title"] = 20
        lens["name"] = max(6, namelen + 1)
        lens["queue"] = 8
        lens["project"] = 8
    if header and printjobssum.header:
        h = ""
        if title:
            h += "group".ljust(lens["title"])
        h += "".join(n.ljust(lens[n]) for n in ("name", "stat", "user"))
        if wide:
            h += "".join(n.ljust(lens[n]) for n in ("queue", "project"))
        h += "runtime".rjust(lens["time"]) + "  resources"
        h = h.upper()
        print(h, file=file)
        printjobssum.header = False
    l = ""
    # title
    if title:
        if not wide:
            if len(title) >= lens["title"]:
                title = title[:lens["title"] - 2] + "*"
        l += color(title.ljust(lens["title"]), "b")
    # Job Name
    jobname = sumjob["job_name"]
    if not wide:
        if len(jobname) >= lens["name"]:
            jobname = jobname[:lens["name"] - 2] + "*"
    l += jobname.ljust(lens["name"])
    # Status
    l += color("%3d " % sumjob["stat"]["PEND"], "r")
    l += color("%3d " % sumjob["stat"]["RUN"], "g")
    done = sumjob["stat"]["EXIT"] + sumjob["stat"]["DONE"]
    if done:
        l += color("%3d " % done, "y")
    else:
        l += "    "
    # User
    if len(sumjob["user"]) == 1:
        user = sumjob["user"].keys()[0]
        c = "g" if user == whoami else 0
        l += color(user.ljust(lens["user"]), c)
    else:
        l += color(str(len(sumjob["user"])).ljust(lens["user"]), "b")
    # Project
    if wide:
        if len(sumjob["queue"]) == 1:
            l += sumjob["queue"].keys()[0].ljust(lens["queue"])
        else:
            l += color(str(len(sumjob["queue"])).ljust(lens["queue"]), "b")
        if len(sumjob["project"]) == 1:
            l += sumjob["project"].keys()[0].ljust(lens["project"])
        else:
            l += color(str(len(sumjob["project"])).ljust(lens["project"]), "b")
    # Wait/Runtime
    if sumjob["run_time"] > 0:
        l += format_duration(sumjob["run_time"]).rjust(lens["time"])
    else:
        l += "".rjust(lens["time"])
    # Resources
    # Time
    if sumjob["runlimit"]:
        l += "  " + format_duration(sumjob["runlimit"]).rjust(lens["time"])
    if sumjob["%complete"]:
        ptime = sumjob["%complete"]
        c = "r" if ptime > 90 else "y" if ptime > 75 else 0
        l += " " + color("%3d" % ptime, c) + "%t"
        if wide:
            s = "%6.2f" % round(ptime, 2)
        else:
            s = "%3d" % int(round(ptime))
        l += " " + color(s, c) + "%t"
    # Memory
    if sumjob["memlimit"] and sumjob["mem"] and sumjob["slots"]:
        memlimit = sumjob["memlimit"] * sumjob["slots"]
        pmem = 100 * sumjob["mem"] / memlimit
        c = "r" if pmem > 90 else "y" if pmem > 75 else 0
        if wide:
            s = "%6.2f" % round(pmem, 2)
        else:
            s = "%3d" % int(round(pmem))
        l += " " + color(s, c) + "%m"
    if sumjob["mem"]:
        l += " " + format_mem(sumjob["mem"]).rjust(9)
    else:
        l += "          "
    # Hosts
    if sumjob["exec_host"]:
        if wide or len(sumjob["exec_host"]) == 1:
            d = sumjob["exec_host"]
        else:
            d = defaultdict(int)
            for key, val in sumjob["exec_host"].iteritems():
                d[re.match("(.*?)\d+", key).groups()[0] + "*"] += val
        for key, val in d.iteritems():
            c = "r" if val >= 100 else "y" if val >= 20 else 0
            exclusive = sumjob["exclusive"]
            exclusive = len(exclusive) == 1 and True in exclusive
            times = color("x", "r") if exclusive else "*"
            l += color(" %3d" % val, c) + times + "%s" % key
    if sumjob["rsvd_host"]:
        l += color("  rsvd:", "y")
        if wide or len(sumjob["rsvd_host"]) == 1:
            d = sumjob["rsvd_host"]
        else:
            d = defaultdict(int)
            for key, val in sumjob["rsvd_host"].iteritems():
                d[re.match("(.*?)\d+", key).groups()[0] + "*"] += val
        for key, val in d.iteritems():
            c = "r" if val >= 100 else "y" if val >= 20 else 0
            l += color(" %3d" % val, c) + "*%s" % key
    print(l, file=file)
    file.flush()

printjobssum.header = True
