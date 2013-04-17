from __future__ import print_function, division

from utility import *
import host as modulehost

import os
import re
from subprocess import Popen, PIPE
from operator import itemgetter

import threading
from time import strptime


class Hostlist(list):
    """List of LSF hosts"""
    allhosts = set()

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
            found = False
            for host in Hostlist.allhosts:
                if host["HOST"] == data["HOST"]:
                    self.append(host)
                    found = True
                    break
            if not found:
                self.append(modulehost.Host(data))

    def append(self, value):
        """Access hosts"""
        if not isinstance(value, modulehost.Host):
            raise TypeError("Hostlist elements must be Host not " +
                            value.__class__.__name__)
        list.append(self, value)
        Hostlist.allhosts.add(value)

    def display(self, wide=False, indent=""):
        """list the hosts"""
        if len(self) == 0:
            return
        users = {}
        whoami = os.getenv("USER")
        threads = {}
        strptime("", "")  # hack to make pseude thread-safe
        for host in self:
            # read job data in parallel
            if host["STATUS"] != "cosed_Excl" and (len(host["Jobs"]) !=
                                                   host["RUN"]):
                for job in host["Jobs"]:
                    if not job.initialized and not job.initializing:
                        t = threading.Thread(target=job.init)
                        t.start()
                        threads[job["Job"]] = t
            # display
            hn = host["HOST"]
            hg = host["Hostgroup"]
            print(indent + hn.ljust(12), end="")
            free = host["MAX"] - host["RUN"] - host["RSV"]
            if host["STATUS"] == "closed_Excl":
                free = 0
            if free == 0:
                c = "r"
            elif free == host["MAX"]:
                c = "g"
            else:
                c = "y"
            print(color("{:>3}*free".format(free), c), end="")
            if host["RSV"] > 0:
                print("  {:>3}*".format(host["RSV"]), end="")
                print(color("reserved", "y"), end="")
            for job in host["Jobs"]:
                if job["Job"] in threads:
                    threads[job["Job"]].join()
                print("  ", end="")
                un = job["User"]
                if not un in users:
                    users[un] = {
                        "Userstr": job["Userstr"],
                        "Hosts": {},
                        "Hostgroups": {},
                    }
                if not hn in users[un]["Hosts"]:
                    users[un]["Hosts"][hn] = 0
                if not hg in users[un]["Hostgroups"]:
                    users[un]["Hostgroups"][hg] = 0
                if host["STATUS"] == "closed_Excl":
                    users[un]["Hosts"][hn] += host["MAX"]
                    users[un]["Hostgroups"][hg] += host["MAX"]
                    print(" -x ", end="")
                elif len(host["Jobs"]) == host["RUN"]:
                    print("  1*", end="")
                    users[un]["Hosts"][hn] += 1
                    users[un]["Hostgroups"][hg] += 1
                else:
                    print("{:>3}*".format(job["Processors"][hn]), end="")
                    users[un]["Hosts"][hn] += job["Processors"][hn]
                    users[un]["Hostgroups"][hg] += job["Processors"][hn]
                if un == whoami:
                    print(color(un, "g"), end="")
                else:
                    print(un, end="")
                if wide:
                    print(": " + job["Job Name"], end="")
                sys.stdout.flush()
            print()
        if len(users):
            print("Users:")
            for un, user in users.iteritems():
                if un == whoami:
                    c = "g"
                else:
                    c = 0
                print("\t" + color(user["Userstr"].ljust(40), c), end="")
                for hn, count in user["Hostgroups"].iteritems():
                    print("\t{:>3}*{}*".format(count, hn), end="")
                print()
