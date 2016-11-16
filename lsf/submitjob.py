"""Submit a job to LSF."""

from __future__ import print_function, division

import sys
import re
from subprocess import Popen, PIPE


def submitjob(data, shell=False):
    """Submit a job to LSF."""
    if "command" not in data:
        print("no command given", file=sys.stderr)
        return False
    aliases = (
        ("id", "jobid"),
        ("name", "job_name"),
        ("description", "job_description"),
        ("proj", "proj_name"),
        ("project", "proj_name"),
        ("app", "application"),
        ("sla", "service_class"),
        ("group", "job_group"),
        ("priority", "job_priority"),
        ("cmd", "command"),
        ("pre_cmd", "pre_exec_command"),
        ("post_cmd", "post_exec_command"),
        ("resize_cmd", "resize_notification_command"),
        ("estart_time", "estimated_start_time"),
        ("sstart_time", "specified_start_time"),
        ("sterminate_time", "specified_terminate_time"),
        ("warn_act", "warning_action"),
        ("warn_time", "action_warning_time"),
        ("except_stat", "exception_status"),
        ("eresreq", "effective_resreq"),
        ("fwd_cluster", "forward_cluster"),
        ("fwd_time", "forward_time")
    )
    strargs = {
        "job_name": "-J",
        "job_description": "-Jd",
        "input_file": "-i",
        "output_file": "-o",
        "error_file": "-e",
        "project": "-P",
        "dependency": "-w"
    }
    intargs = {
        "slots": "-n"
    }
    memargs = {
        "memlimit": "-M",
        "corelimit": "-C",
        "stacklimit": "-S"
    }
    timeargs = {
        "runlimit": "-W"
    }
    args = []
    for key, val in data.iteritems():
        if key[0] == "-":
            if val is True:
                args += [key]
            if not isinstance(val, bool):
                args += [key, val]
            continue
        if key in aliases:
            key = aliases[key]
        if key in strargs:
            args += [key, val]
        if key in intargs:
            args += [key, str(val)]
        if key in memargs:
            args += [key, str(val // 1024)]
        if key in timeargs:
            args += [key, str(val // 60)]
    # output file from jobname
    if "-o" not in args and "-J" in args:
        args += ["-o", args[args.index["-J"] + 1] + ".%J.out"]
    cmd = ["bsub"] + args
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    if shell:
        command = '#!/bin/bash -l\n'
    else:
        command = ''
    command += data["command"]
    out, err = p.communicate(command)
    match = re.search("Job <(.*?)> is submitted", out)
    if match:
        return match.groups()[0]
    else:
        match = re.search("Error: (.*)\n", err)
        if match:
            err = match.groups()[0]
        raise EnvironmentError(1, err)
