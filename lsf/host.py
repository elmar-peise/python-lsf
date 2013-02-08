from __future__ import print_function, division

import sys
import re
from subprocess import Popen, check_output, PIPE


class Host():
    def __init__(self, init):
        """Init Host from LSF or data"""
        self.initialized = False
        self.initializing = False
        if type(init) is str:
            self.data = {
                    "HOST": init,
                    "HOST_NAME": init,
                    }
        elif type(init) == dict:
            if "HOST" in init:
                init["HOST_NAME"] = init["HOST"]
            elif "HOST_NAME" in init:
                init["HOS"] = init["HOST_NAME"]
            else:
                raise TypeError("No 'HOST' or 'HOST_NAME' in Host init data")
            self.data = init
        else:
            raise TypeError("Host init argument must be str or dict")

    def init(self):
        """Init Host from LSF"""
        if self.initializing or self.initialized:
            return True
        self.initializing = True
        if self.read():
            self.initialized = True
        self.initializing = False
        return True

    def read(self):
        """Read the full Host infroamtion from LSF"""
        if not self["HOST"]:
            return False
        self.data = {"HOST": self["HOST"]}
        p = Popen(["bhosts", "-l", self["HOST"]], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err and not out:
            print("{} is not a host".format(self["HOST"]), file=sys.stderr)
            return False
        groups = re.search("(STATUS.*)\n(.*)\n", out).groups()
        d = dict(zip(groups[0].split(), groups[1].split()))
        self.data.update(d)
        for key in self:
            try:
                self[key] = int(self[key])
            except:
                pass
        return True

    def __str__(self):
        """Access host attributes"""
        return self["HOST"]

    def __repr__(self):
        """Access host attributes"""
        return "Host(" + self["HOST"] + ")"

    def __getitem__(self, key):
        """Access host attributes"""
        if key in self.data:
            return self.data[key]
        self.init()
        return self.data.__getitem__(key)

    def __setitem__(self, key, value):
        """Access host attributes"""
        return self.data.__setitem__(key, value)

    def __contains__(self, key):
        """Access host attributes"""
        if key in self.data:
            return True
        self.init()
        return self.data.__contains__(key)

    def keys(self):
        """Access host attributes"""
        self.init()
        return self.data.keys()

    def __delitem__(self, key):
        """Access host attributes"""
        return self.data.__delitem__(key)


class Hostlist(list):
    """List of LSF hosts"""
    allhosts = set()

    def __init__(self, args=None, names=None):
        """Init list from LSF or othr host list"""
        list.__init__(self)
        if names:
            for name in names:
                self.append(Host(name))
        if args is None:
            return
        if type(args) is str:
            args = [args]
        self.readhosts(args)

    def readhosts(self, args):
        """read hosts from LSF"""
        p = Popen(["bhosts", "-X", "-w"] + args, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        for line in out.split("\n")[1:-1]:
            line = line.split()
            data = {
                    "HOST": line[0],
                    "HOST_NAME": line[0],
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
                self.append(Host(data))

    def append(self, value):
        """Access hosts"""
        if not isinstance(value, Host):
            raise TypeError("Hostlist elements must be Host not " + value.__class__.__name__)
        list.append(self, value)
        Hostlist.allhosts.add(value)
