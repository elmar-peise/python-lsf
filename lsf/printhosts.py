"""Print a list of hosts."""

from __future__ import print_function, division

import os
import sys
import re
from time import time
from subprocess import check_output
from collections import defaultdict

from utility import color, fractioncolor, format_duration, format_mem
from groupjobs import groupjobs
from sumjobs import sumjobs
from useraliases import getuseralias


def printhosts(hosts, jobs=[], wide=False, header=True, file=sys.stdout):
    """Print a list of hosts."""
    if len(hosts) == 0:
        return
    sumhosts = not isinstance(hosts[0]["status"], str)
    jobsbyhost = groupjobs(jobs, "exec_host")
    # begin output
    screencols = int(check_output(["tput", "cols"]))
    whoami = os.getenv("USER")
    namelen = max(map(len, (host["host_name"] for host in hosts)))
    lens = {
        "host_name": min(20, max(6, namelen + 1)),
        "status": 8,
        "title": 15,
        "cpus": 10
    }
    if wide:
        lens["title"] = 20
        lens["host_name"] = max(6, namelen + 1)
        lens["model"] = 14
    if sumhosts:
        lens["status"] = 12
        lens["cpus"] = 14
    if header:
        h = ""
        if sumhosts and "title" in hosts[0]:
            h += "group".ljust(lens["title"])
        h += "".join(n.ljust(lens[n]) for n in ("host_name", "status", "cpus"))
        h += "mem (free/total)"
        if wide:
            h += "  " + "model".ljust(lens["model"])
        h = h.upper()
        print(h, file=file)
    for host in hosts:
        l = ""
        if sumhosts and "title" in host:
            # title
            title = host["title"]
            if not wide:
                if len(title) >= lens["title"]:
                    title = title[:lens["title"] - 2] + "*"
            l += color(title.ljust(lens["title"]), "b")
        # host_name
        l += host["host_name"].ljust(lens["host_name"])
        # status
        if sumhosts:
            l += color("%3d " % host["status"]["ok"], "g")
            closed = sum(n for stat, n in host["status"].iteritems() if
                         stat.startswith("closed_"))
            l += color("%3d " % closed, "r")
            other = len(host["host_names"]) - host["status"]["ok"] - closed
            if other:
                l += color("%3d " % other, "y")
            else:
                l += "    "
        else:
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
        c = fractioncolor(free, total)
        if sumhosts:
            l += color("%4d" % free, c) + "/%4d" % total
        else:
            l += color("%2d" % free, c) + "/%2d" % total
        # mem
        if "mem" in host["load"]:
            free, used = host["load"]["mem"]
            total = free
            if used:  # used can be None
                total += used
            if "maxmem" in host and host["maxmem"]:
                total = host["maxmem"]
            c = fractioncolor(free, total)
            l += "  " + format_mem(free, c) + "/" + format_mem(total)
        if wide:
            if sumhosts:
                if len(host["model"]) == 1:
                    l += host["model"][0].ljust(lens["model"])
                else:
                    nmodel = len(host["model"])
                    l += color(("  %d" % nmodel).ljust(lens["model"]), "b")
            else:
                hoststr = host["model"]
                # Xeon Phi(s)
                phis = 0
                if "mic0" in host["load"]:
                    phis += int(bool(host["load"]["mic0"][0]))
                    phis += int(bool(host["load"]["mic0"][1]))
                if "mic1" in host["load"]:
                    phis += int(bool(host["load"]["mic1"][0]))
                    phis += int(bool(host["load"]["mic1"][1]))
                if phis > 0:
                    hoststr += "+%dPhi" % phis
                # GPU
                if "gpu" in host["resources"]:
                    hoststr += "+GPU"
                l += "  " + hoststr.ljust(14)
        l += " "
        if host["rsv"] > 0:
            l += " %3d*" % host["rsv"] + color("reserved", "y")
        if sumhosts:
            hostnames = host["host_names"]
        else:
            hostnames = [host["host_name"]]
        jobs = []
        for hostname in hostnames:
            if hostname in jobsbyhost:
                for job in jobsbyhost[hostname]:
                    if job not in jobs:
                        jobs.append(job)
        if sumhosts:
            jobgroups = groupjobs(jobs, "user")
            jobs = []
            for user in sorted(jobgroups.keys()):
                jobs.append(sumjobs(jobgroups[user]))
        if jobs:
            for job in jobs:
                exclusive = job["exclusive"]
                if sumhosts:
                    exclusive = len(exclusive) == 1 and True in exclusive
                times = color("x", "r") if exclusive else "*"
                nslots = sum(job["exec_host"][hn] for hn in hostnames
                             if hn in job["exec_host"])
                c = "r" if nslots >= 100 else "y" if nslots >= 20 else 0
                l += color(" %3d" % nslots, c)
                user = job["user"]
                if sumhosts:
                    user = user.keys()[0]
                c = "g" if user == whoami else 0
                l += times + color(getuseralias(user).ljust(8), c)
                if wide and not sumhosts:
                    if job["mem"]:
                        l += format_mem(job["mem"])
                    else:
                        l += "         "
                    if job["%complete"] and job["runlimit"]:
                        ptime = job["%complete"]
                        c = fractioncolor(1 - ptime / 100)
                        l += color("%3d" % ptime, c) + "% "
                        l += format_duration(job["runlimit"])
        if host["comment"]:
            if sumhosts:
                for key, val in host["comment"].iteritems():
                    if key:
                        l += " %3dx" % val + color(key, "b")
            else:
                l += "   " + color(host["comment"], "b")
        print(l, file=file)
        file.flush()
