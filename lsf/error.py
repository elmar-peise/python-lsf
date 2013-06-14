#!/usr/bin/env python
from __future__ import print_function, division


class LSFError(EnvironmentError):
    """Errors from LSF"""

    def __init__(self, *t):
        EnvironmentError.__init__(self, *t)
