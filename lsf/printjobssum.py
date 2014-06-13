#!/usr/bin/env python
from __future__ import print_function, division

from utility import color, format_duration, format_mem, findstringpattern

import os
import sys
import re
from time import time
from subprocess import check_output
from collections import defaultdict


def sumjobs(jobs):
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
    return sumjob


def printjobssum(jobs, long=False, wide=False, title=None,
                 header=True, file=sys.stdout):
    """summarize the jobs in one line"""
    if len(jobs) == 0:
        return
    whoami = os.getenv("USER")
    job = sumjobs(jobs)
    namelen = max(map(len, (job["name"] for job in jobs)))
    lens = {
        "title": 10,
        "name": min(20, max(6, namelen + 1)),
        "stat": 12,
        "user": 10,
        "time": 12
    }
    if wide:
        lens["title"] = 20
        lens["name"] = max(6, namelen + 1)
        lens["queue"] = 8
        lens["project"] = 8
    # header
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
    # job name
    jobname = job["job_name"] if job["job_name"] else ""
    if not wide:
        if len(jobname) >= lens["name"]:
            jobname = jobname[:lens["name"] - 2] + "*"
    l += jobname.ljust(lens["name"])
    # status
    l += color("%3d " % job["stat"]["PEND"], "r")
    l += color("%3d " % job["stat"]["RUN"], "g")
    done = job["stat"]["EXIT"] + job["stat"]["DONE"]
    if done:
        l += color("%3d " % done, "y")
    else:
        l += "    "
    # user
    if len(job["user"]) == 1:
        user = job["user"].keys()[0]
        c = "g" if user == whoami else 0
        l += color(user.ljust(lens["user"]), c)
    else:
        l += color(str(len(job["user"])).ljust(lens["user"]), "b")
    if wide:
        # queue
        if len(job["queue"]) == 1:
            l += job["queue"].keys()[0].ljust(lens["queue"])
        else:
            l += color(str(len(job["queue"])).ljust(lens["queue"]), "b")
        # project
        if len(job["project"]) == 1:
            l += job["project"].keys()[0].ljust(lens["project"])
        else:
            n = len(job["project"])
            l += color(str(n).ljust(lens["project"]), "b")
    # runtime
    if job["run_time"] > 0:
        l += format_duration(job["run_time"]).rjust(lens["time"])
    else:
        l += "".rjust(lens["time"])
    # resources
    # %t
    if job["%complete"]:
        ptime = job["%complete"]
        c = "r" if ptime > 90 else "y" if ptime > 75 else 0
        if wide:
            s = "%6.2f" % round(ptime, 2)
        else:
            s = "%3d" % int(round(ptime))
        l += " " + color(s, c) + "%t"
    # %m
    if job["memlimit"] and job["mem"] and job["slots"]:
        memlimit = job["memlimit"] * job["slots"]
        pmem = 100 * job["mem"] / memlimit
        c = "r" if pmem > 90 else "y" if pmem > 75 else 0
        if wide:
            s = "%6.2f" % round(pmem, 2)
        else:
            s = "%3d" % int(round(pmem))
        l += " " + color(s, c) + "%m"
    # time
    if job["runlimit"]:
        l += "  " + format_duration(job["runlimit"])
    # memory
    if job["memlimit"]:
        l += format_mem(job["memlimit"]).rjust(10)
    else:
        l += "".rjust(10)
    # Hosts
    if job["exec_host"]:
        if wide or len(job["exec_host"]) == 1:
            d = job["exec_host"]
        else:
            d = defaultdict(int)
            for key, val in job["exec_host"].iteritems():
                d[re.match("(.*?)\d+", key).groups()[0] + "*"] += val
        for key in sorted(d.keys()):
            val = d[key]
            c = "r" if val >= 100 else "y" if val >= 20 else 0
            exclusive = job["exclusive"]
            exclusive = len(exclusive) == 1 and True in exclusive
            times = color("x", "r") if exclusive else "*"
            l += color(" %3d" % val, c) + times + "%s" % key
    else:
        if job["rsvd_host"]:
            l += color("  rsvd:", "y")
            if wide or len(job["rsvd_host"]) == 1:
                d = job["rsvd_host"]
            else:
                d = defaultdict(int)
                for key, val in job["rsvd_host"].iteritems():
                    d[re.match("(.*?)\d+", key).groups()[0] + "*"] += val
            for key, val in d.iteritems():
                c = "r" if val >= 100 else "y" if val >= 20 else 0
                l += color(" %3d" % val, c) + "*%s" % key
    print(l, file=file)
    file.flush()

printjobssum.header = True
