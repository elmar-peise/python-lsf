#!/usr/bin/env python
from __future__ import print_function, division

from utility import *
import job as modulejob

import sys
import os
import re
from time import time
from subprocess import Popen, check_output, PIPE

import threading
from time import strptime


class Joblist(list):
    """List of LSF jobs"""
    alljobs = set()

    def __init__(self, args=None, jobs=None):
        """Read joblist from bjobs or form another list"""
        list.__init__(self)
        if jobs:
            self += jobs
        if args is None:
            return
        if type(args) is str:
            args = [args]
        self.readjobs(args)

    def __setitem__(self, key, value):
        """Access jobs"""
        if not isinstance(value, modulejob.Job):
            raise TypeError("Joblist elements must be Job not " +
                            value.__class__.__name__)
        list.__setitem__(self, key, value)
        Joblist.alljobs.add(value)

    def append(self, value):
        """Access jobs"""
        if not isinstance(value, modulejob.Job):
            raise TypeError("Joblist elements must be Job not " +
                            value.__class__.__name__)
        list.append(self, value)
        Joblist.alljobs.add(value)

    def __setslice__(self, i, j, sequence):
        """Access jobs"""
        for k, value in enumerate(sequence):
            if not isinstance(value, modulejob.Job):
                raise TypeError("item " + value.__class__.__name__ +
                                ": Joblist elements must be Job not " + k)
            else:
                Joblist.alljobs.add(value)
        list.__setslice__(self, i, j, sequence)

    def __add__(self, other):
        """Access jobs"""
        return Joblist(jobs=self + other)

    def __radd__(self, other):
        """Access jobs"""
        return Joblist(jobs=other + self)

    def __iadd__(self, sequence):
        """Access jobs"""
        for k, value in enumerate(sequence):
            if not isinstance(value, modulejob.Job):
                raise TypeError("item " + value.__class__.__name__ +
                                ": Joblist elements must be Job not " + k)
            else:
                Joblist.alljobs.add(value)
        for job in sequence:
            self.append(job)

    def readjobs(self, args):
        """Read jobs from LSF"""
        p = Popen(["bjobs", "-w"] + args, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if "No unfinished job found" in err:
            return
        for line in out.split("\n")[1:-1]:
            line = line.split()
            data = {
                "Job": line[0],
                "User": line[1],
                "Status": line[2]
            }
            match = re.search("(\[\d+\])$", line[-4])
            if match:
                data["Job"] += match.groups()[0]
            found = False
            for job in Joblist.alljobs:
                if job["Job"] == data["Job"]:
                    self.append(job)
                    found = True
                    break
            if not found:
                self.append(modulejob.Job(data))

    def groupby(self, key=None):
        """sort the jobs in groups by attributes"""
        if not key:
            return {None: self}
        result = {}
        for job in self:
            if not key in job:
                value = None
            else:
                value = job[key]
            vlist = [value]
            if type(value) is list:
                vlist = value
            if type(value) is dict:
                vlist = value.keys()
            if key == "PENDING REASONS":
                vlist = [tuple(value.items())]
            for value in vlist:
                if not value in result:
                    result[value] = Joblist()
                result[value].append(job)
        return result

    def display(self, long=False, wide=False, title=None, parallel=True):
        """list the jobs"""
        if len(self) == 0:
            return
        # read job data in parallel
        threads = {}
        if parallel:
            strptime("", "")  # hack to make pseude thread-safe
            for job in self:
                if not job.initialized and not job.initializing:
                    t = threading.Thread(target=job.init)
                    t.start()
                    threads[job["Job"]] = t
        # begin output
        screencols = int(check_output(["tput", "cols"]))
        if long:
            if title:
                print(title.center(screencols, "-"))
            for job in self:
                f = " {Job} --- {Job Name} --- {User} --- {Status} "
                header = f.format(**job)
                print(header.center(screencols, "-"))
                print(job)
            return
        whoami = os.getenv("USER")
        lens = {
            "id": 12,
            "name": 16,
            "status": 8,
            "user": 10,
            "time": 12,
        }
        if wide:
            lens["name"] = 32
            lens["queue"] = 8
        h = "Job".ljust(lens["id"]) + "Job Name".ljust(lens["name"])
        h += "Status".ljust(lens["status"]) + "User".ljust(lens["user"])
        if wide:
            h += "Queue".ljust(lens["queue"])
        h += "Wait/Runtime".rjust(lens["time"]) + "  Resources"
        h = h.replace(" ", "-")
        if title:
            h += (" " + title + " ").center(screencols - len(h), "-")
        else:
            h += (screencols - len(h)) * "-"
        print(h)
        for job in self:
            if job["Job"] in threads:
                threads[job["Job"]].join()
            # Job
            l = (job["Job"] + " ").ljust(lens["id"])
            # Job Name
            jobname = job["Job Name"]
            if not wide:
                if len(jobname) >= lens["name"]:
                    jobname = jobname[:lens["name"] - 2] + "*"
                jobname += " "
            l += jobname.ljust(lens["name"])
            # Status
            if job["Status"] == "PEND":
                c = "r"
            elif job["Status"] == "RUN":
                c = "g"
            else:
                c = "y"
            l += color((job["Status"] + " ").ljust(lens["status"]), c)
            # User
            if wide:
                username = job["Userstr"]
            else:
                username = job["User"]
            if job["User"] == whoami:
                c = "g"
            else:
                c = 0
            l += color((username + " ").ljust(lens["user"]), c)
            # Queue
            if wide:
                l += job["Queue"].ljust(lens["queue"])
            # Wait/Runtime
            if "endtime" in job:
                t = int(job["endtime"] - job["starttime"])
            elif "starttime" in job:
                t = int(time() - job["starttime"])
            elif "submittime" in job:
                t = int(time() - job["submittime"])
            else:
                t = False
            if t:
                s = format_duration(t)
            else:
                s = ""
            l += s.rjust(lens["time"])
            # Resources
            # Time
            l += "  " + format_duration(job["RUNLIMIT"]) + "  "
            if job["Status"] == "RUN":
                # Execution hosts
                if wide:
                    l += job["Processorsstr"]
                else:
                    l += job["Hostgroupsstr"]
            elif job["Status"] == "PEND":
                # #cores
                l += str(job["Processors Requested"]).rjust(2)
                if "Exclusive Execution" in job and job["Exclusive Execution"]:
                    l += " excl "
                else:
                    l += " cores"
                # Mmeory
                l += format_mem(job["MEMLIMIT"]).rjust(8)
                # Hosts or architecture
                if "Specified Hosts" in job:
                    l += "  " + job["Specified Hosts"].ljust(16)
                else:
                    match = re.search("\(model==(.*?)\)",
                                      job["Requested Resources"])
                    if match:
                        l += "  " + match.groups()[0].ljust(16)
                if "Reserved" in job:
                    l += "rsvd:"
                    for proc, n in job["Reserved"].iteritems():
                        l += " " + str(n) + "*" + proc
            print(l)
            sys.stdout.flush()
