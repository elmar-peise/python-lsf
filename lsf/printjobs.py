#!/usr/bin/env python
from __future__ import print_function, division

from utility import color, format_duration, format_mem, format_time

import os
import sys
import re
from time import time
from subprocess import check_output
from collections import defaultdict


def printjoblong(job, file=sys.stdout):
    keys = ("jobid", "stat", "user", "mail", "queue", "job_name",
            "job_description", "proj_name", "application", "service_class",
            "job_group", "job_priority", "dependency", "command",
            "pre_exec_command", "post_exec_command",
            "resize_notification_command", "pids", "exit_code", "exit_reason",
            "from_host", "first_host", "exec_host", "nexec_host",
            "submit_time", "start_time", "estimated_start_time",
            "specified_start_time", "specified_terminate_time", "time_left",
            "finish_time", "runlimit", "%complete", "warning_action",
            "action_warning_time", "cpu_used", "run_time", "idle_factor",
            "exception_status", "slots", "mem", "max_mem", "avg_mem",
            "memlimit", "swap", "swaplimit", "min_req_proc", "max_req_proc",
            "resreq", "combined_resreq", "effective_resreq", "network_req",
            "filelimit", "corelimit", "stacklimit", "processlimit",
            "input_file", "output_file", "error_file", "output_dir", "sub_cwd",
            "exec_home", "exec_cwd", "forward_cluster", "forward_time",
            "pend_reason")
    for key in keys:
        if job[key]:
            print(key.ljust(20), file=file, end="")
            if key in ("swap", "mem", "avg_mem", "max_mem", "memlimit",
                       "swaplimit", "corelimit", "stacklimit"):
                print(format_mem(job[key]), file=file)
            elif key in ("submit_time", "start_time", "finish_time"):
                print(format_time(job[key]), file=file)
            elif key in ("cpu_used", "time_left", "runlimit"):
                print(format_duration(job[key]), file=file)
            elif key in ("pend_reason", "exec_host"):
                items = job[key]
                if isinstance(items, dict):
                    items = items.items()
                key2, val = items[0]
                print("%4d * %s" % (val, key2), file=file)
                for key2, val in items[1:]:
                    print(20 * " " + "%4d * %s" % (val, key2), file=file)
            elif key in ("command", "pre_exec_command", "post_exec_command",
                         "resize_notification_command"):
                script = job[key]
                for _ in xrange(3):
                    script = script.replace("; ", ";;")
                script = script.replace(";;;; ", "; ")
                script = script.replace(";", "\n")
                script = re.sub("for \(\((.*?)\n\n(.*?)\n\n(.*?)\)\)",
                                "for ((\\1; \\2; \\3))", script)
                script = script.splitlines()
                print(script[0], file=file)
                for line in script[1:]:
                    print(20 * " " + line, file=file)
            elif key == "pids":
                print(" ".join(map(str, job[key])), file=file)
            else:
                print(job[key], file=file)
    print(file=file)


def printjobs(jobs, wide=False, long=False, title=None,
              header=True, file=sys.stdout):
    """list the jobs"""
    if len(jobs) == 0:
        return
    if long:
        for job in jobs:
            printjoblong(job, file=file)
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
            h += "  " + color(title, "b")
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
        c = "g" if job["user"] == whoami else 0
        l += color((job["user"] + " ").ljust(lens["user"]), c)
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
        elif job["memlimit"]:
            l += " " + format_mem(job["memlimit"]).rjust(9)
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
                times = color("x", "r") if job["exclusive"] else "*"
                l += color(" %3d" % val, c) + times + "%s" % key
        else:
            if job["min_req_proc"]:
                times = color("x", "r") if job["exclusive"] else "*"
                l += " %3d" % job["min_req_proc"] + times
            elif job["exclusive"]:
                l += "   1" + color("x", "r")
            if job["resreq"]:
                match = re.search("model==(\w+)", job["resreq"])
                if match:
                    l += match.groups()[0]
        print(l, file=file)
        file.flush()
