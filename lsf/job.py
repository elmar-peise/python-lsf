#!/usr/bin/env python
from __future__ import print_function, division

import hostlist as modulehostlist
from utility import *
from error import LSFError

import sys
import re
from time import strftime, strptime, mktime
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
        "Processors": "(?:Hosts/Processors|\[\d+\] started on) <(.*?)>[,;]",
        "Reserved": "Reserved <\d+> job slots? on host(?:\(s\))? <(.*?)>[,;]",
        "Started on": "Started on <(.*?)>[,;]",
        "Complete": "Completed <(.*?)>",
        "PENDING REASONS": "PENDING REASONS:\n(.*?)\n\n",
        "RUNLIMIT": "RUNLIMIT\s*\n (.*?) min of",
        "MEMLIMIT": "MEMLIMIT\s*\n .*? (\d+ [BKMGT])\s+\n",
        "CORELIMIT": "CORELIMIT\s*\n (\d+ [BKMGT])",
    }
    numregexps = {
        "Job Priority": "Job Priority <(\d+)>,",
        "Processors Requested": ", (\d+) Processors Requested,",
        "nReserved": "Reserved <(\d+)> job slots? on host",
        "CPU time": "The CPU time used is (.*?) seconds.",
        "ptile": "span\[ptile=(\d+)\]",
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
        if type(init) in (int, str):
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
        out = out.decode()
        err = err.decode()
        if "User permission denied" in err:
            self.initialized = False
            return False
        else:
            return True

    def read(self):
        """read information on the job from LSF.
        Return true on successful reading of the job.
        """
        if not self["Job"]:
            return False
        self.data = {"Job": self["Job"]}
        p = Popen(["bjobs", "-l", self["Job"]], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        out = str(out.decode(errors="ignore"))
        err = str(err.decode(errors="ignore"))
        if err and not out:
            return False
        if 78 * "-" in out:
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
                tstr = strftime("%Y ") + match.groups()[0]
                t = strptime(tstr, "%Y %b %d %H:%M:%S")
                self[key] = mktime(t)
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
            self["Command"] = re.sub("for \(\((.*?)\n\n(.*?)\n\n(.*?)\)\)",
                                     "for ((\\1; \\2; \\3))", self["Command"])
        if "Started on" in self:
            self["Processors"] = self["Started on"]
            del self["Started on"]
        if "Processors" in self and isinstance(self['Processors'], str):
            procs = {}
            self["Processors"] = self["Processors"].replace("><", "> <")
            for proc in re.split("> ?<", self["Processors"]):
                proc = proc.split("*")
                if len(proc) == 1:
                    procs[proc[0]] = 1
                else:
                    procs[proc[1]] = int(proc[0])
            self["Processors"] = procs
            strs = (str(c).rjust(3) + "*" + p for p, c in procs.iteritems())
            self["Processorsstr"] = " ".join(strs)
            self["Hosts"] = modulehostlist.Hostlist([p for p in procs])
            hgs = {}
            for host in self["Hosts"]:
                hg = host["Hostgroup"]
                if not hg in hgs:
                    hgs[hg] = 0
                hgs[hg] += procs[host["HOST"]]
            self["Hostgroups"] = hgs
            strs = (str(c).rjust(3) + "*" + p + "*"
                    for p, c in hgs.iteritems())
            self["Hostgroupsstr"] = " ".join(strs)
        if not "Processors Requested" in self:
            self["Processors Requested"] = 1
        self["Nodes Requested"] = self["Processors Requested"]
        if "ptile" in self:
            self["Nodes Requested"] //= self["ptile"]
        if "nReserved" in self:
            if self["nReserved"] == 1:
                self["Reserved"] = {self["Reserved"]: 1}
            else:
                procs = {}
                for proc in re.split("> ?<", self["Reserved"]):
                    proc = proc.split("*")
                    procs[proc[1]] = int(proc[0])
                self["Reserved"] = procs
        if "PENDING REASONS" in self:
            reasons = self["PENDING REASONS"]
            match = re.findall(" (.*?): (\d+) hosts?;", str(reasons))
            d = {r: int(n) for r, n in match}
            match = re.findall("^ ([^:]*?);", str(reasons))
            d.update((m, True) for m in match)
            self["PENDING REASONS"] = d
        if "RUNLIMIT" in self:
            self["RUNLIMIT"] = int(60 * float(self["RUNLIMIT"]))
        if "MEMLIMIT" in self:
            groups = re.search("(.*) ([BKMGT])", self["MEMLIMIT"]).groups()
            units = {"B": 0, "K": 1, "M": 2, "G": 3, "T": 4}
            self["MEMLIMIT"] = int(groups[0]) * 1024 ** units[groups[1]]
        if "CORELIMIT" in self:
            groups = re.search("(.*) ([BKMGT])", self["CORELIMIT"]).groups()
            units = {"B": 0, "K": 1, "M": 2, "G": 3, "T": 4}
            self["CORELIMIT"] = int(groups[0]) * 1024 ** units[groups[1]]
        return True

    def __str__(self):
        """long format job details"""
        self.init()
        h, w = map(int, check_output(("stty", "size")).split())
        wk = 25
        wv = w - wk
        data = {}
        for k in self.data.keys():
            val = self[k]
            if k in Job.timeregexps:
                val = format_time(val)
            elif k in ("RUNLIMIT", "CPU time"):
                val = format_duration(val)
            elif k == "MEMLIMIT":
                val = format_mem(val)
            elif type(val) is dict:
                val = "\n".join("{}\t{}".format(dv, dk)
                                for dk, dv in val.iteritems())
            elif isinstance(val, list):
                val = "\n".join(str(v) for v in val)
            elif k in ("Processorsstr", "Userstr"):
                continue
            strs = (s[i:i + wv]
                    for s in str(val).splitlines()
                    for i in range(0, len(s) + 1, wv))
            s = ("\n" + wk * " ").join(strs)
            data[k] = k.ljust(wk) + s + "\n"
        result = ""
        for k in ("Job", "Job Name", "User", "Status", "Command", "submittime",
                  "starttime", "endtime", "Pending Reasons", "RUNLIMIT",
                  "MEMLIMIT", "Processors Requested", "Processors", "ptile",
                  "Exclusive Execution", "Requested Resources", "Reserved",
                  "Job Description"):
            if k in data:
                result += data[k]
                del data[k]
        for v in data.itervalues():
            result += v
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


def submit(data, shell=False):
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
    if "CORELIMIT" in data:
        data["-C"] = str(data["CORELIMIT"] // 1024)
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
    if shell:
        script = '#!/bin/bash -l\n'
    else:
        script = ''
    script += data["Command"]
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    out, err = p.communicate(script)
    out = out.decode()
    err = err.decode()
    match = re.search("Job <(.*?)> is submitted", out)
    if match:
        return Job(str(match.groups()[0]))
    else:
        match = re.search("Error: (.*)\n", err)
        if match:
            raise LSFError(1, match.groups()[0])
        else:
            raise LSFError(1, err)
