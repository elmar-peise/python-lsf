#!/usr/bin/env python
from __future__ import print_function, division

from collections import defaultdict


def groupjobs(jobs, key):
    """sort the jobs in groups by attributes"""
    result = defaultdict(list)
    for job in jobs:
        if key == "pend_reason":
            result[repr(sorted(job[key]))].append(job)
        elif isinstance(job[key], dict):
            for val in job[key]:
                result[val].append(job)
        else:
            result[job[key]].append(job)
    return dict(result)
