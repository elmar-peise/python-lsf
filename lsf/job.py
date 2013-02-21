#!/usr/bin/env python
from __future__ import print_function, division

from host import Hostlist
from utility import color, format_time

import sys
import os
import re
import time
from subprocess import Popen, check_output, PIPE


class Job():
    """representation of a single LSF batch job"""
    strregexps = {
        "Job Name": "Job Name <(.*?)>, ?User",
        "User": "User <(.*?)>[,;]",
        "Project": "Project <(.*?)>[,;]",
        "User Group": "User Group <(.*?)>[,;]",
        "Mail": "Mail <(.*?)>[,;]",
        "Status": "Status <(.*?)>[,;]",
        "Queue": "Queue <(.*?)>[,;]",
        "Command": "Command <(.*?)>, ?(?:Job Description|Share group charged)",
        "Submitted from host": "Submitted from host <(.*?)>[,;]",
        "CWD": "CWD <(.*?)>[,;]",
        "Output File": "Output File <(.*?)>[,;]",
        "Error File": "Error File <(.*?)>[,;]",
        "Requested Resources": "Requested Resources <(.*?) ?>[,;]",
        "Dependency Condition": "Dependency Condition <(.*?)>[,;]",
        "Share group charged": "Share group charged <(.*?)>[,;]",
        "Job Description": "Job Description <(.*?)>",
        "Specified Hosts": "Specified Hosts <(.*?)>[,;]",
        "Execution Home": "Execution Home <(.*?)>[,;]",
        "Execution CWD": "Execution CWD <(.*?)>[,;]",
        "Processors": "Hosts/Processors <(.*?)>[,;]",
        "Started on": "Started on <(.*?)>[,;]",
        "Complete": "Completed <(.*?)>",
        "PENDING REASONS": "PENDING REASONS:\n(.*?)\n\n",
        "RUNLIMIT": "RUNLIMIT\s*\n (.*?) min of",
        "MEMLIMIT": "MEMLIMIT\s*\n (.*?)\n",
    }
    numregexps = {
        "Job Priority": "Job Priority <(\d+)>,",
        "Processors Requested": ", (\d+) Processors Requested,",
        "CPU time": "The CPU time used is (.*?) seconds.",
    }
    tfregexps = {
        "Notify when job ends": "Notify when job (begins/)?ends",
        "Notify when job begins": "Notify when job begins",
        "Exclusive Execution": "Exclusive Execution",
    }
    timeregexp = "([A-Z][a-z]{2} +\d+ \d+:\d+:\d+)"
    timeregexps = {
        "submittime": timeregexp + ": Submitted",
        "starttime": timeregexp + ": (?:\[\d+\] )?[sS]tarted",
        "resourcetime": timeregexp + ": Resource",
        "endtime": timeregexp + ": Done",
        "exittime": timeregexp + ": Exited",
        "completetime": timeregexp + ": Completed",
    }

    def __init__(self, init):
        """Init job from LSF or data"""
        self.initialized = False
        self.initializing = False
        if type(init) in [int, str]:
            self.data = {"Job": str(init)}
        elif type(init) == dict:
            if not "Job" in init:
                raise KeyError("No 'Job' in Job init data")
            self.data = init
        else:
            raise TypeError("Job init argument must be int, str or dict")

    def init(self):
        """Read the full job information from LSF"""
        if self.initializing or self.initialized:
            return True
        self.initializing = True
        if self.read():
            self.initialized = True
        self.initializing = False
        return True

    def kill(self):
        """Kill the job"""
        p = Popen(["bkill", str(self["Job"])], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if "User permission denied" in err:
            self.initialized = False
            return False
        else:
            return True

    def read(self):
        """read information on the job from LSF"""
        if not self["Job"]:
            return False
        self.data = {"Job": self["Job"]}
        p = Popen(["bjobs", "-l", self["Job"]], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err and not out:
            print(self["Job"] + " is not a job", file=sys.stderr)
            return False
        if 78 * "-" in out:
            print(self["Job"] + " is an array job", file=sys.stderr)
            return False
        outjoin = out.replace("\n                     ", "")
        #data retrieval
        for key, regexp in Job.strregexps.iteritems():
            match = re.search(regexp, outjoin, re.DOTALL)
            if match:
                self[key] = match.groups()[0]
        for key, regexp in Job.numregexps.iteritems():
            match = re.search(regexp, outjoin)
            if match:
                try:
                    self[key] = int(match.groups()[0])
                except:
                    self[key] = float(match.groups()[0])
        for key, regexp in Job.tfregexps.iteritems():
            self[key] = not (re.search(regexp, outjoin) is None)
        for key, regexp in Job.timeregexps.iteritems():
            match = re.search(regexp, outjoin)
            if match:
                tstr = time.strftime("%Y ") + match.groups()[0]
                t = time.strptime(tstr, "%Y %b %d %H:%M:%S")
                self[key] = time.mktime(t)
        #processing
        if "User" in self:
            if "Mail" in self:
                self["Userstr"] = "{User} <{Mail}>".format(**self)
            else:
                self["Userstr"] = "{User}".format(**self)
        if "Command" in self:
            for _ in xrange(3):
                self["Command"] = self["Command"].replace("; ", ";;")
            self["Command"] = self["Command"].replace(";;;; ", ";    ")
            self["Command"] = self["Command"].replace(";", "\n")
        if "Started on" in self:
            self["Processors"] = self["Started on"]
            del self["Started on"]
        if "Processors" in self:
            procs = {}
            for proc in self["Processors"].split("> <"):
                proc = proc.split("*")
                if len(proc) == 1:
                    procs[proc[0]] = 1
                else:
                    procs[proc[1]] = int(proc[0])
            self["Processors"] = procs
            strs = (str(c) + "*" + p for p, c in procs.iteritems())
            self["Processorsstr"] = " ".join(strs)
            self["Hosts"] = Hostlist([p for p in procs])
            hostgroups = {}
            for proc, count in procs.iteritems():
                hostgroup = re.match("([^\d]+)\d+", proc).groups()[0] + "*"
                if not hostgroup in hostgroups:
                    hostgroups[hostgroup] = 0
                hostgroups[hostgroup] += count
            self["Hostgroups"] = hostgroups
            strs = (str(c) + "*" + p for p, c in hostgroups.iteritems())
            self["Hostgroupsstr"] = " ".join(strs)
        if not "Processors Requested" in self:
            self["Processors Requested"] = 1
        if "PENDING REASONS" in self:
            reasons = self["PENDING REASONS"]
            match = re.findall(" (.*?): (\d+) hosts?;", reasons)
            d = {r: int(n) for r, n in match}
            match = re.findall("^ ([^:]*?);", reasons)
            d.update((m, True) for m in match)
            self["PENDING REASONS"] = d
        if "RUNLIMIT" in self:
            self["RUNLIMIT"] = int(60 * float(self["RUNLIMIT"]))
        if "MEMLIMIT" in self:
            groups = re.search("(.*) ([BKMGT])", self["MEMLIMIT"]).groups()
            units = {"B": 0, "K": 1, "M": 2, "G": 3, "T": 4}
            self["MEMLIMIT"] = int(groups[0]) * 1024 ** units[groups[1]]
        return True

    def __str__(self):
        """long format job details"""
        self.init()
        h, w = map(int, check_output(["stty", "size"]).split())
        wk = 25
        wv = w - wk
        result = ""
        for k in self.data.keys():
            strs = (s[i:i + wv]
                    for s in str(self[k]).splitlines()
                    for i in range(0, len(s) + 1, wv))
            s = ("\n" + wk * " ").join(strs)
            result += k.ljust(wk) + s + "\n"
        return result

    def __getitem__(self, key):
        """Access job attributes"""
        if key in self.data:
            return self.data[key]
        self.init()
        return self.data.__getitem__(key)

    def __setitem__(self, key, value):
        """Access job attributes"""
        return self.data.__setitem__(key, value)

    def __contains__(self, key):
        """Access job attributes"""
        if key in self.data:
            return True
        self.init()
        return self.data.__contains__(key)

    def keys(self):
        """Access job attributes"""
        self.init()
        return self.data.keys()

    def __delitem__(self, key):
        """Access job attributes"""
        return self.data.__delitem__(key)


def submit(data):
    """Submit a job to LSF"""
    if not "Command" in data:
        print("no command given", file=sys.stderr)
        return False
    if "Job Name" in data:
        data["-J"] = data["Job Name"]
    if "Output File" in data:
        data["-o"] = data["Output File"]
    if not "-o" in data and "-J" in data:
        data["-o"] = data["-J"] + ".%J.out"
    if "Error File" in data:
        data["-e"] = data["Error File"]
    if "Mail" in data:
        data["-u"] = data["Mail"]
    if "Notify when job ends" in data and data["Notify when job ends"]:
        data["-N"] = True
    if "Notify when job begins" in data and data["Notify when job begins"]:
        data["-B"] = True
    if "Exclusive Execution" in data and data["Exclusive Execution"]:
        data["-x"] = True
    if "Processors Requested" in data:
        data["-n"] = str(int(data["Processors Requested"]))
    if "RUNLIMIT" in data:
        data["-W"] = str(data["RUNLIMIT"] // 60)
    if "MEMLIMIT" in data:
        data["-M"] = str(data["MEMLIMIT"] // 1024)
    if "Project" in data:
        data["-P"] = data["Project"]
    if "Resource Request" in data:
        if "-R" in data:
            data["-R"] = [data["-R"]]
        else:
            data["-R"] = []
        data["-R"].append(data["Resource Request"])
    if "Dependency Condition" in data:
        data["-w"] = data["Dependency Condition"]
    cmd = ["bsub"]
    for key, value in data.iteritems():
        if key[0] != "-":
            continue
        val = data[key]
        if type(val) is bool:
            cmd += [key]
        elif type(val) is str:
            cmd += [key, val]
        elif type(val) is list:
            for v in val:
                cmd += [key, str(v)]
    script = "#!/bin/bash -l\n" + data["Command"]
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    out, err = p.communicate(script)
    match = re.search("Job <(.*?)> is submitted", out)
    if match:
        return Job(match.groups()[0])
    else:
        print("problem with job submission:\n" + err, file=sys.stderr)
        return False


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
        if not isinstance(value, Job):
            raise TypeError("Joblist elements must be Job not " +
                            value.__class__.__name__)
        list.__setitem__(self, key, value)
        Joblist.alljobs.add(value)

    def append(self, value):
        """Access jobs"""
        if not isinstance(value, Job):
            raise TypeError("Joblist elements must be Job not " +
                            value.__class__.__name__)
        list.append(self, value)
        Joblist.alljobs.add(value)

    def __setslice__(self, i, j, sequence):
        """Access jobs"""
        for k, value in enumerate(sequence):
            if not isinstance(value, Job):
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
            if not isinstance(value, Job):
                raise TypeError("item " + value.__class__.__name__ +
                                ": Joblist elements must be Job not " + k)
            else:
                Joblist.alljobs.add(value)
        for job in sequence:
            self.append(job)

    def readjobs(self, args):
        """Read jobs from LSF"""
        p = Popen(["bjobs"] + args, stdout=PIPE, stderr=PIPE)
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
                self.append(Job(data))

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

    def display(self, long=False, wide=False, title=None):
        """list the jobs"""
        if len(self) == 0:
            return
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
            "id": 16,
            "name": 16,
            "status": 8,
            "user": 12,
            "time": 16
        }
        h = "Job".ljust(lens["id"]) + "Job Name".ljust(lens["name"])
        h += "Status".ljust(lens["status"]) + "User".ljust(lens["user"])
        h += "Wait/Runtime".rjust(lens["time"]) + "    Resources"
        h = h.replace(" ", "-")
        if title:
            h += (" " + title + " ").center(screencols - len(h), "-")
        else:
            h += (screencols - len(h)) * "-"
        print(h)
        for job in self:
            # Job
            l = (job["Job"] + " ").ljust(lens["id"])
            # Job Name
            jobname = job["Job Name"]
            if wide:
                jobname += "\t"
            else:
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
            # Wait/Runtime
            if "endtime" in job:
                t = int(job["endtime"] - job["starttime"])
            elif "starttime" in job:
                t = int(time.time() - job["starttime"])
            elif "submittime" in job:
                t = int(time.time() - job["submittime"])
            else:
                t = False
            if t:
                s = format_time(t)
            else:
                s = ""
            l += s.rjust(lens["time"])
            # Resources
            # Time
            l += "    " + format_time(job["RUNLIMIT"]) + "    "
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
                m = job["MEMLIMIT"]
                i = 0
                while m >= 1024:
                    m //= 1024
                    i += 1
                l += str(m).rjust(5)
                l += ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"][i]
                # Hosts or architecture
                if "Specified Hosts" in job:
                    l += "    " + job["Specified Hosts"]
                else:
                    match = re.match("[model==(.*?)]",
                                     job["Requested Resources"])
                    if match:
                        l += "    " + match.groups()[0]
            print(l)
            sys.stdout.flush()
