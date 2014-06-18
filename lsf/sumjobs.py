#!/usr/bin/env python
from __future__ import print_function, division

from utility import findstringpattern

from collections import defaultdict


def sumjobs(jobs):
    sumjob = {}
    for key in jobs[0]:
        if key in ("job_name", "job_description", "input_file", "output_file",
                   "error_file", "output_dir", "sub_cwd", "exec_home",
                   "exec_cwd", "exit_reson", "application", "command",
                   "pre_exec_command", "post_exec_command",
                   "resize_notification_command", "effective_resreq"):
            # find string pattern
            sumjob[key] = findstringpattern([job[key] for job in jobs
                                             if job[key]])
        elif key in ("runlimit", "swaplimit", "stacklimit", "memlimit",
                     "filelimit", "processlimit", "corelimit", "run_time",
                     "swap", "slots", "mem", "max_mem", "avg_mem",
                     "nexec_host"):
            # sum
            sumjob[key] = sum(job[key] for job in jobs if job[key])
        elif key in ("%complete", "job_priority", "idle_factor"):
            # compute average
            pcomp = [job[key] for job in jobs if job[key]]
            if pcomp:
                sumjob[key] = sum(pcomp) / len(pcomp)
            else:
                sumjob[key] = None
        elif key in ("exec_host", "rsvd_host"):
            # collect host counts
            sumjob[key] = defaultdict(int)
            for job in jobs:
                if job[key]:
                    for host, count in job[key].iteritems():
                        sumjob[key][host] += count
        elif key == "pids":
            # collect
            sumjob[key] = sum((job[key] for job in jobs if job[key]), [])
        elif key in ("jobid", "pend_reason"):
            # collect
            sumjob[key] = []
            for job in jobs:
                if job[key] and job[key] not in sumjob[key]:
                    sumjob[key].append(job[key])
        else:
            # collect and count
            sumjob[key] = defaultdict(int)
            for job in jobs:
                sumjob[key][job[key]] += 1
    return sumjob
