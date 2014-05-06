#!/usr/bin/env python
from __future__ import print_function, division

import re
from time import strptime, strftime, mktime, time
from subprocess import Popen, check_output, PIPE


def readjobs(args):
    """Read jobs from bjobs"""
    keys = ("jobid", "stat", "user", "queue", "job_name", "job_description",
            "proj_name", "application", "service_class", "job_group",
            "job_priority", "dependency", "command", "pre_exec_command",
            "post_exec_command", "resize_notification_command", "pids",
            "exit_code", "exit_reason", "from_host", "first_host", "exec_host",
            "nexec_host", "submit_time", "start_time", "estimated_start_time",
            "specified_start_time", "specified_terminate_time", "time_left",
            "finish_time", "%complete", "warning_action",
            "action_warning_time", "cpu_used", "run_time", "idle_factor",
            "exception_status", "slots", "mem", "max_mem", "avg_mem",
            "memlimit", "swap", "swaplimit", "min_req_proc", "max_req_proc",
            "effective_resreq", "network_req", "filelimit", "corelimit",
            "stacklimit", "processlimit", "input_file", "output_file",
            "error_file", "output_dir", "sub_cwd", "exec_home", "exec_cwd",
            "forward_cluster", "forward_time")
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
    delimiter = "\7"
    # get detailed job information
    cmd = ["bjobs", "-noheader", "-X", "-o",
           " ".join(keys) + " delimiter='" + delimiter + "'"] + args
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if err:
        return []
    joborder = []
    jobs = {}
    for line in out.splitlines():
        job = dict(zip(keys, line.split(delimiter)))
        for key, val in job.iteritems():
            if val == "-":
                job[key] = None
            elif key in ("exit_code", "nexec_host", "slots", "job_priority"):
                job[key] = int(val)
            elif key in ("cpu_used", "run_time"):
                job[key] = float(val.split()[0])
            elif key in ("submit_time", "start_time", "finish_time"):
                if val[-1] in "ELXA":
                    val = val[:-2]
                year = strftime("%Y")
                job[key] = mktime(strptime(year + " " + val,
                                           "%Y %b %d %H:%M"))
                if key != "finish_time" and job[key] > time():
                    year = str(int(year) - 1)
                    job[key] = mktime(strptime(year + " " + val,
                                               "%Y %b %d %H:%M"))
            elif key in ("time_left"):
                if val[-1] in "ELXA":
                    val = val[:-2]
                try:
                    v = val.split(":")
                    job[key] = 60 * (60 * int(v[0]) + int(v[1]))
                except:
                    job[key] = mktime(strptime(year + " " + val,
                                               "%Y %b %d %H:%M"))
            elif key in ("%complete"):
                job[key] = float(val.split("%")[0])
            elif key in ("exec_host"):
                val = val.split(":")
                hosts = {}
                for v in val:
                    if "*" in v:
                        v = v.split("*")
                        hosts[v[1]] = int(v[0])
                    else:
                        hosts[v] = 1
                job[key] = hosts
            elif key in ("swap", "mem", "memlimit", "corelimit", "stacklimit"):
                val = val.split()
                e = {"K": 1, "M": 2, "G": 3, "T": 4}[val[1][0]]
                job[key] = int(float(val[0]) * 1024 ** e)
            elif key in ("pids"):
                if val:
                    job[key] = map(int, val.split(","))
                else:
                    job[key] = []
        job["pend_reason"] = None
        job["runlimit"] = None
        if job["run_time"] and job["%complete"]:
            t = job["run_time"] / job["%complete"] * 100
            # rounding
            if t > 10 * 60 * 60:
                job["runlimit"] = round(t / (60 * 60)) * 60 * 60
            else:
                job["runlimit"] = round(t / 60) * 60
        joborder.append(job["jobid"])
        jobs[job["jobid"]] = job
    if not joborder:
        return []
    # get more accurate timestamps from -W output
    out = check_output(["bjobs", "-noheader", "-W"] + joborder)
    for line in out.splitlines():
        line = line.split()
        job = jobs[line[0]]
        for n, key in (
                (7, "submit_time"),
                (13, "start_time"),
                (14, "finish_time")
                ):
            if line[n] != "-":
                try:
                    year = strftime("%Y")  # guess year
                    t = mktime(strptime(year + " " + line[n],
                                        "%Y %m/%d-%H:%M:%S"))
                    if t > time():
                        # adjust guess for year
                        year = str(int(year) - 1)
                        t = mktime(strptime(year + " " + line[n],
                                            "%Y %m/%d-%H:%M:%S"))
                    job[key] = t
                except:
                    pass

    # get pending reasons (if any)
    pids = [jid for jid in joborder if jobs[jid]["stat"] == "PEND"]
    if pids:
        out = check_output(["bjobs", "-p"] + pids)
        job = None
        for line in out.split("\n")[1:-1]:
            if line[0] == " ":
                # pending reason
                if ":" in line:
                    match = re.match(" (.*): (\d+) hosts?;", line).groups()
                    job["pend_reason"][match[0]] = int(match[1])
                else:
                    match = re.match(" (.*);", line).groups()
                    job["pend_reason"][match[0]] = True
            else:
                # next job
                job = jobs[line.split()[0]]
                job["pend_reason"] = {}
    # aliases
    for job in jobs.values():
        for alias, key in aliases:
            job[alias] = job[key]
    return [jobs[jid] for jid in joborder]
