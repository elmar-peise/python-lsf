from __future__ import print_function, division

from utility import *

import sys
import re
from subprocess import Popen, PIPE


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
        self["Hostgroup"] = re.match("(.*?)\d+", line[0]).groups()[0],
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
