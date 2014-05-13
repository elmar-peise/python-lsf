#!/usr/bin/env python
from __future__ import print_function, division

from utility import color, format_duration, format_mem, findstringpattern
from groupjobs import groupjobs

import os
import sys
import re
from time import time
from subprocess import check_output
from collections import defaultdict


def printhostssum(hosts, jobs=[], wide=False, title=None, header=True,
                  file=sys.stdout):
    """summarize the hosts in one line"""
    if len(hosts) == 0:
        return
    hostnames = [host["host_name"] for host in hosts]
    # begin output
    screencols = int(check_output(["tput", "cols"]))
    whoami = os.getenv("USER")
    lens = {
        "host_name": 14,
        "status": 12,
        "title": 10
    }
    if wide:
        lens["title"] = 20
        lens["model"] = 14
    sumhost = {}
    for key in hosts[0]:
        if key in ("host_name"):
            # find string pattern
            sumhost[key] = findstringpattern([host[key] for host in hosts
                                              if host[key]])
        elif key in ("max", "njobs", "run", "ssusp", "ususp", "rsv", "ncpus",
                     "maxmem", "maxswp"):
            # sum
            sumhost[key] = sum(host[key] for host in hosts if host[key])
        elif key in ("status", "server", "type"):
            sumhost[key] = defaultdict(int)
            for host in hosts:
                sumhost[key][host[key]] += 1
        elif key in ("load", "threshold"):
            # sum up free/used pairs
            sumhost[key] = dict()
            for key2 in host[key]:
                free, used = zip(*[host[key][key2] for host in hosts])
                if all(x is None for x in free):
                    free = None
                else:
                    free = sum(x for x in free if x)
                if all(x is None for x in used):
                    used = None
                else:
                    used = sum(x for x in used if x)
                sumhost[key][key2] = [free, used]
        else:
            # colect
            sumhost[key] = []
            for host in hosts:
                if host[key] and host[key] not in sumhost[key]:
                    sumhost[key].append(host[key])
    # begin output
    if header and printhostssum.header:
        h = ""
        if title:
            h += "group".ljust(lens["title"])
        h += "".join(n.ljust(lens[n]) for n in ("host_name", "status"))
        h += " cpus         mem (free/total)"
        if wide:
            h += "  " + "model".ljust(lens["model"])
        h = h.upper()
        print(h, file=file)
        printhostssum.header = False
    l = ""
    # title
    if title:
        if not wide:
            if len(title) >= lens["title"]:
                title = title[:lens["title"] - 2] + "*"
        l += color(title.ljust(lens["title"]), "b")
    # host_name
    l = host["host_name"].ljust(lens["host_name"])
    # status
    l += color("%3d " % sumhost["status"]["ok"], "g")
    closed = sum(n for stat, n in sumhost["status"].iteritems()
                 if stat.startswith("closed_"))
    l += color("%3d " % closed, "r")
    other = len(hosts) - sumhost["status"]["ok"] - closed
    if other:
        l += color("%3d " % other, "y")
    else:
        l += "    "
    # cpus
    total = sumhost["max"]
    used = sumhost["njobs"]
    free = total - used
    c = "r" if free == 0 else "y" if free < total else 0
    l += color("%4d" % free, c) + "/%4d" % total
    # mem
    if "mem" in sumhost["load"]:
        free, used = sumhost["load"]["mem"]
        total = free + used
        if sumhost["maxmem"]:
            total = sumhost["maxmem"]
        f = used / total
        c = "r" if f > .75 else "y" if f > .5 else 0
        l += "  " + format_mem(free, c) + "/" + format_mem(total)
    if wide:
        if len(sumhost["model"]) == 1:
            l += sumhost["model"][0].ljust(lens["model"])
        else:
            l += color(str(len(sumhost["model"])).ljust(lens["model"]), "b")
    l += " "
    if sumhost["rsv"] > 0:
        l += " %2d*" % sumhost["rsv"] + color("reserved", "y")
    if wide:
        for job in jobs:
            c = "g" if job["user"] == whoami else 0
            l += " %3d*" % sum(job["exec_host"][hn] for hn in hostnames
                               if hn in job["exec_host"])
            l += color(job["user"].ljust(8), c)
    else:
        userhosts = defaultdict(int)
        for job in jobs:
            userhosts[job["user"]] += sum(job["exec_host"][hn]
                                          for hn in hostnames
                                          if hn in job["exec_host"])

        for user, count in userhosts.iteritems():
            c = "g" if user == whoami else 0
            l += " %3d*" % count + color(user.ljust(8), c)

    print(l, file=file)
    file.flush()

printhostssum.header = True
