#!/usr/bin/env python
from __future__ import print_function, division

from utility import color, format_duration, format_mem, format_time
from useraliases import getuseralias

import os
import sys
import re
from time import time
from subprocess import check_output
from collections import defaultdict


def printjoblong(job, sumjob=False, file=sys.stdout):
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
            "pend_reason", "rsvd_host")
    for key in keys:
        if not job[key]:
            continue
        if sumjob and isinstance(job[key], dict):
            if len(job[key]) == 1 and job[key].keys()[0] is None:
                continue
        print(key.ljust(20), file=file, end="")
        if key in ("swap", "mem", "avg_mem", "max_mem", "memlimit",
                   "swaplimit", "corelimit", "stacklimit"):
            print(format_mem(job[key]), file=file)
        elif key in ("submit_time", "start_time", "finish_time"):
            print(format_time(job[key]), file=file)
        elif key in ("cpu_used", "time_left", "runlimit", "run_time"):
            print(format_duration(job[key]), file=file)
        elif key in ("pend_reason", "exec_host", "rsvd_host"):
            if sumjob:
                items = sum(job[key], [])
            else:
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
            if sumjob:
                if isinstance(job[key], dict):
                    if len(job[key]) == 1:
                        print(job[key].keys()[0], file=file)
                    else:
                        items = job[key].items()
                        print("%4d * %s" % (val, key2), file=file)
                        for key2, val in items[1:]:
                            print(20 * " " + "%4d * %s" % (val, key2),
                                  file=file)
                elif isinstance(job[key], list):
                    print(" ".join(job[key]), file=file)
                else:
                    print(job[key], file=file)
            else:
                print(job[key], file=file)
    print(file=file)


def printjobs(jobs, wide=False, long=False, title=None,
              header=True, file=sys.stdout):
    """list the jobs"""
    if len(jobs) == 0:
        return
    sumjob = not isinstance(jobs[0]["jobid"], str)
    if long:
        for job in jobs:
            printjoblong(job, sumjob=sumjob, file=file)
        return
    # begin output
    whoami = os.getenv("USER")
    namelen = max(map(len, (job["name"] for job in jobs)))
    lens = {
        "title": 10,
        "jobid": 10,
        "name": min(20, max(6, namelen + 1)),
        "stat": 6,
        "user": 10,
        "time": 12
    }
    if sumjob:
        lens["stat"] = 12
    else:
        if any(job["jobid"][-1] == "]" for job in jobs):
            lens["jobid"] = 14
    if wide:
        lens["title"] = 20
        lens["name"] = max(6, namelen + 1)
        lens["queue"] = 8
        lens["project"] = 8
        lens["prio."] = 6
    # header
    if header:
        h = ""
        if sumjob and "title" in jobs[0]:
            h += "group".ljust(lens["title"])
        if not sumjob:
            h += "jobid".ljust(lens["jobid"])
        h += "".join(n.ljust(lens[n]) for n in ("name", "stat", "user"))
        if wide:
            h += "".join(n.ljust(lens[n]) for n in ("queue", "project"))
            if not sumjob:
                h += "prio.".ljust(lens["prio."])
        if sumjob:
            h += "runtime".rjust(lens["time"])
        else:
            h += "wait/runtime".rjust(lens["time"])
        h += "  resources"
        h = h.upper()
        if title:
            h += "  " + color(title, "b")
        print(h, file=file)
    for job in jobs:
        l = ""
        if sumjob and "title" in job:
            # title
            title = job["title"]
            if not wide:
                if len(title) >= lens["title"]:
                    title = title[:lens["title"] - 2] + "*"
            l += color(title.ljust(lens["title"]), "b")
        if not sumjob:
            # jobid
            l += (job["jobid"] + " ").ljust(lens["jobid"])
        # job name
        jobname = job["job_name"] if job["job_name"] else ""
        if not wide:
            if len(jobname) >= lens["name"]:
                jobname = jobname[:lens["name"] - 2] + "*"
        l += jobname.ljust(lens["name"])
        # status
        if sumjob:
            l += color("%3d " % job["stat"]["PEND"], "r")
            l += color("%3d " % job["stat"]["RUN"], "g")
            done = job["stat"]["EXIT"] + job["stat"]["DONE"]
            if done:
                l += color("%3d " % done, "y")
            else:
                l += "    "
        else:
            stat = job["stat"]
            c = "r" if stat == "PEND" else "g" if stat == "RUN" else "y"
            l += color(stat.ljust(lens["stat"]), c)
        # user
        if sumjob:
            if len(job["user"]) == 1:
                user = job["user"].keys()[0]
                c = "g" if user == whoami else 0
                username = getuseralias(user)
                l += color(username.ljust(lens["user"]), c)
            else:
                l += color(str(len(job["user"])).ljust(lens["user"]), "b")
        else:
            c = "g" if job["user"] == whoami else 0
            username = getuseralias(job["user"])
            l += color((username + " ").ljust(lens["user"]), c)
        if wide:
            # queue
            if sumjob:
                if len(job["queue"]) == 1:
                    l += job["queue"].keys()[0].ljust(lens["queue"])
                else:
                    nqueues = len(job["queue"])
                    l += color(str(nqueues).ljust(lens["queue"]), "b")
            else:
                l += job["queue"].ljust(lens["queue"])
            # project
            if sumjob:
                if len(job["project"]) == 1:
                    l += job["project"].keys()[0].ljust(lens["project"])
                else:
                    n = len(job["project"])
            else:
                l += job["project"].ljust(lens["project"])
            if not sumjob:
                # priority
                l += str(job["priority"]).rjust(lens["prio."] - 1) + " "
        # wait/runtime
        t = job["run_time"]
        if not sumjob and job["stat"] == "PEND":
                t = time() - job["submit_time"]
        s = format_duration(t)
        l += s.rjust(lens["time"])
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
        elif not sumjob and job["stat"] == "RUN":
            l += "      "
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
        elif not sumjob and job["stat"] == "RUN":
            l += "      "
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
                if sumjob:
                    exclusive = len(exclusive) == 1 and True in exclusive
                times = color("x", "r") if exclusive else "*"
                l += color(" %3d" % val, c) + times + "%s" % key
        else:
            if not sumjob:
                if job["min_req_proc"]:
                    times = color("x", "r") if job["exclusive"] else "*"
                    l += " %3d" % job["min_req_proc"] + times
                elif job["exclusive"]:
                    l += "   1" + color("x", "r")
                else:
                    l += "   1*"
                if job["resreq"]:
                    match = re.search("model==(\w+)", job["resreq"])
                    if match:
                        l += match.groups()[0]
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
