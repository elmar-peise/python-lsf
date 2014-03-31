from __future__ import print_function, division

from utility import color
import host as modulehost

import sys
import os
import re
from subprocess import Popen, PIPE
from operator import itemgetter

import threading
from time import strptime


class Hostlist(list):
    """List of LSF hosts"""
    allhosts = dict()

    def __init__(self, args=None, names=None):
        """Init list from LSF or othr host list"""
        list.__init__(self)
        if names:
            for name in names:
                self.append(modulehost.Host(name))
        if args is None:
            return
        if type(args) is str:
            args = [args]
        self.readhosts(args)

    def sort(self, key=itemgetter("HOST")):
        """by default, sort by name"""
        list.sort(self, key=key)

    def readhosts(self, args):
        """read hosts from LSF"""
        p = Popen(["bhosts", "-X", "-w"] + args, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        out = out.decode()
        err = err.decode()
        for line in out.split("\n")[1:-1]:
            line = line.split()
            data = {
                "HOST": line[0],
                "HOST_NAME": line[0],
                "Hostgroup": re.match("(.*?)\d+", line[0]).groups()[0],
                "STATUS": line[1],
                "MAX": int(line[3]),
                "NJOBS": int(line[4]),
                "RUN": int(line[5]),
                "SSUSP": int(line[6]),
                "USUSP": int(line[7]),
                "RSV": int(line[8]),
            }
            if data["HOST"] in Hostlist.allhosts:
                self.append(Hostlist.allhosts[data["HOST"]])
            else:
                self.append(modulehost.Host(data))

    def append(self, value):
        """Access hosts"""
        if not isinstance(value, modulehost.Host):
            raise TypeError("Hostlist elements must be Host not " +
                            value.__class__.__name__)
        list.append(self, value)
        Hostlist.allhosts[value["HOST"]] = value

    def display(self, wide=False, indent="", parallel=True):
        """list the hosts"""
        if len(self) == 0:
            return
        users = {}
        whoami = os.getenv("USER")
        # read job data in parallel
        threads = {}
        if parallel:
            strptime("", "")  # hack to make pseude thread-safe
            for host in self:
                if not any((host["STATUS"] in ["unavail", "cosed_Excl"],
                            host["RUN"] == 0,
                            len(host["Jobs"]) == host["RUN"],
                            len(host["Jobs"]) == 1)):
                    for job in host["Jobs"]:
                        if not job.initialized and not job.initializing:
                            t = threading.Thread(target=job.init)
                            t.deamon = True
                            t.start()
                            threads[job["Job"]] = t
        for host in self:
            # display
            hn = host["HOST"]
            hg = host["Hostgroup"]
            l = indent + hn.ljust(14)
            if host["STATUS"] == "unavail":
                l += color(" unavail", "r")
            elif host["STATUS"][0:7] == "closed_":
                l += color(host["STATUS"][7:].rjust(8), "r")
            else:
                free = max(0, host["MAX"] - host["RUN"] - host["SSUSP"] -
                           host["USUSP"] - host["RSV"])
                if free == 0:
                    c = "r"
                elif free == host["MAX"]:
                    c = "g"
                else:
                    c = "y"
                l += color("{:>3}*free".format(free), c)
            if host["SSUSP"] > 0:
                l += "  {:>3}*".format(host["SSUSP"]) + color("ssusp", "r")
            if host["USUSP"] > 0:
                l += "  {:>3}*".format(host["USUSP"]) + color("ususp", "r")
            if host["RSV"] > 0:
                l += "  {:>3}*".format(host["RSV"]) + color("reserved", "y")
            print(l, end="")
            if host["RUN"] > 0:
                jobsleft = len(host["Jobs"])
                runleft = host["RUN"]
                for job in host["Jobs"]:
                    l = "  "
                    un = job["User"]
                    if un not in users:
                        if job["Job"] in threads:
                            threads[job["Job"]].join()
                            del threads[job["Job"]]
                        users[un] = {
                            "Userstr": job["Userstr"],
                            "Hosts": {},
                            "Hostgroups": {},
                        }
                    if hn not in users[un]["Hosts"]:
                        users[un]["Hosts"][hn] = 0
                    if hg not in users[un]["Hostgroups"]:
                        users[un]["Hostgroups"][hg] = 0
                    if host["STATUS"] == "closed_Excl":
                        users[un]["Hosts"][hn] += host["MAX"]
                        users[un]["Hostgroups"][hg] += host["MAX"]
                        l += " -x "
                    elif jobsleft == 1:
                        l += "{:>3}*".format(runleft)
                        users[un]["Hosts"][hn] += runleft
                        users[un]["Hostgroups"][hg] += runleft
                    elif jobsleft == runleft:
                        l += "  1*"
                        users[un]["Hosts"][hn] += 1
                        users[un]["Hostgroups"][hg] += 1
                        runleft -= 1
                    else:
                        if job["Job"] in threads:
                            threads[job["Job"]].join()
                            del threads[job["Job"]]
                        l += "{:>3}*".format(job["Processors"][hn])
                        users[un]["Hosts"][hn] += job["Processors"][hn]
                        users[un]["Hostgroups"][hg] += job["Processors"][hn]
                        runleft -= job["Processors"][hn]
                    if un == whoami:
                        l += color(un, "g")
                    else:
                        l += un
                    if wide:
                        l += ": " + job["Job Name"]
                    jobsleft -= 1
                    print(l, end="")
                    sys.stdout.flush()
            print()
        if len(users):
            print("Users:")
            bysum = lambda u: sum(-h[1] for h in
                                  u[1]["Hostgroups"].iteritems())
            for un, user in sorted(users.items(), key=bysum):
                if un == whoami:
                    c = "g"
                else:
                    c = 0
                l = "    " + color(user["Userstr"].ljust(48), c)
                for hn, count in user["Hostgroups"].iteritems():
                    l += "  {:>3}*{}*".format(count, hn)
                print(l)
