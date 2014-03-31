#!/usr/bin/env python
from __future__ import print_function, division

import hostlist as modulehostlist
from utility import format_time, format_duration, format_mem

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
        "Dependency Condition": "Dependency Condition <(.*?)>[,;]",
        "Share group charged": "Share group charged <(.*?)>[,;]",
        "Job Description": "Job Description <(.*?)>",
        "Specified Hosts": "Specified Hosts <(.*?)>;",
        "Execution Home": "Execution Home <(.*?)>[,;]",
        "Execution CWD": "Execution CWD <(.*?)>[,;]",
        "Processors": "(?:Hosts/Processors|\[\d+\] started on) <(.*?)>[,;]",
        "Reserved": "Reserved <\d+> job slots? on host(?:\(s\))? <(.*?)>[,;]",
        "Started on": "Started on <(.*?)>[,;]",
        "Complete": "Completed <(.*?)>",
        "PENDING REASONS": "PENDING REASONS:\n(.*?)\n\n",
        "RUNLIMIT": "RUNLIMIT\s*\n (.*?) min of",
        "limitline": "(STACKLIMIT.*?|[^\n]*?MEMLIMIT)\n",
        "limitvline": "(?:STACKLIMIT.*?|[^\n]*?MEMLIMIT)\n(.*?)\n",
        "Requested Resources":
        "RESOURCE REQUIREMENT DETAILS:\n Combined: (.*?)\n Effective",
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
            if "Job" not in init:
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
        # data retrieval
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
        # processing
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
            if self["Exclusive Execution"]:
                strs = (p for p in procs)
            else:
                strs = (str(c).rjust(3) + "*" + p
                        for p, c in procs.iteritems())
            self["Processorsstr"] = " ".join(strs)
            self["Hosts"] = modulehostlist.Hostlist([p for p in procs])
            if self["Exclusive Execution"]:
                for host in self["Hosts"]:
                    procs[host["HOST"]] = host["MAX"]
            hgs = {}
            for host in self["Hosts"]:
                hg = host["Hostgroup"]
                if hg not in hgs:
                    hgs[hg] = 0
                hgs[hg] += procs[host["HOST"]]
            self["Hostgroups"] = hgs
            strs = (str(c).rjust(3) + "*" + p + "*"
                    for p, c in hgs.iteritems())
            self["Hostgroupsstr"] = " ".join(strs)
        if "Processors Requested" not in self:
            self["Processors Requested"] = 1
        self["Nodes Requested"] = self["Processors Requested"]
        if "ptile" in self:
            self["Nodes Requested"] //= self["ptile"]
        if "nReserved" in self:
            procs = {}
            if self["nReserved"] == 1:
                procs[self["Reserved"]] = 1
            else:
                for proc in re.split("> ?<", self["Reserved"]):
                    proc = proc.split("*")
                    procs[proc[1]] = int(proc[0])
            self["Reserved"] = procs
            strs = (str(c).rjust(3) + "*" + p for p, c in procs.iteritems())
            self["Reservedstr"] = " ".join(strs)
            self["Reserved Hosts"] = modulehostlist.Hostlist([p for p in
                                                              procs])
            rgs = {}
            for host in self["Reserved Hosts"]:
                rg = host["Hostgroup"]
                if rg not in rgs:
                    rgs[rg] = 0
                rgs[rg] += procs[host["HOST"]]
            self["Reserved Hostgroups"] = rgs
            strs = (str(c).rjust(3) + "*" + p + "*"
                    for p, c in rgs.iteritems())
            self["Reserved Hostgroupsstr"] = " ".join(strs)
        if "Specified Hosts" in self:
            self["Specified Hosts"] = self["Specified Hosts"].split(">, <")
            self["Specified Hostsstr"] = " ".join(self["Specified Hosts"])
            hgs = {}
            for host in self["Specified Hosts"]:
                g = re.match("(.*?)\d+", host).groups()[0]
                if g not in hgs:
                    hgs[g] = 0
                hgs[g] += 1
            self["Specified Hostgroups"] = hgs
            strs = (str(c).rjust(3) + "*" + h + "*"
                    for h, c in hgs.iteritems())
            self["Specified Hostgroupsstr"] = " ".join(strs)
        if "PENDING REASONS" in self:
            reasons = self["PENDING REASONS"]
            match = re.findall(" (.*?): (\d+) hosts?;", str(reasons))
            d = {r: int(n) for r, n in match}
            match = re.findall("^ ([^:]*?);", str(reasons))
            d.update((m, True) for m in match)
            self["PENDING REASONS"] = d
        if "RUNLIMIT" in self:
            self["RUNLIMIT"] = int(60 * float(self["RUNLIMIT"]))
        units = {"B": 0, "K": 1, "M": 2, "G": 3, "T": 4}
        limits = self["limitline"].split()
        limitvs = self["limitvline"].split()
        for i in range(len(limits)):
            self[limits[i]] = int(float(limitvs[2 * i]) * 1024 **
                                  units[limitvs[2 * i + 1]])
        del self["limitline"]
        del self["limitvline"]
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
            elif k in ("STACKLIMIT", "CORELIMIT", "MEMLIMIT", "SWAPLIMIT"):
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
                  "starttime", "endtime", "CPU time", "Pending Reasons",
                  "RUNLIMIT", "STACKLIMIT", "MEMLIMIT", "CORELIMIT",
                  "SWAPLIMIT", "Processors Requested", "Processors",
                  "ptile", "Exclusive Execution", "Requested Resources",
                  "Reserved", "Job Description"):
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
