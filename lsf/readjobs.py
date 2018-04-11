"""Read jobs from bjobs."""

from __future__ import division

import re
from time import strptime, strftime, mktime, time
from subprocess import Popen, check_output, PIPE, CalledProcessError


def parsemem(value, unit):
    """Parse a memory size value and unit to int."""
    e = {"B": 0, "K": 1, "M": 2, "G": 3, "T": 4}[unit]
    return int(float(value) * 1024 ** e)


def readjobs(args, fast=False):
    """Read jobs from bjobs."""
    keys = ("jobid", "stat", "user", "user_group", "queue", "job_name",
            "job_description", "proj_name", "application", "service_class",
            "job_group", "job_priority", "dependency", "command",
            "pre_exec_command", "post_exec_command",
            "resize_notification_command", "pids", "exit_code", "exit_reason",
            "from_host", "first_host", "exec_host", "nexec_host", "alloc_slot",
            "nalloc_slot", "host_file", "submit_time", "start_time",
            "estimated_start_time", "specified_start_time",
            "specified_terminate_time", "time_left", "finish_time",
            "%complete", "warning_action", "action_warning_time", "pend_time",
            "cpu_used", "run_time", "idle_factor", "exception_status", "slots",
            "mem", "max_mem", "avg_mem", "memlimit", "swap", "swaplimit",
            "min_req_proc", "max_req_proc", "effective_resreq", "network_req",
            "filelimit", "corelimit", "stacklimit", "processlimit",
            "input_file", "output_file", "error_file", "output_dir", "sub_cwd",
            "exec_home", "exec_cwd", "forward_cluster", "forward_time")
    aliases = (
        ("id", "jobid"),
        ("ugroup", "user_group"),
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
    cmd = ["bjobs", "-X", "-o",
           " ".join(keys) + " delimiter='" + delimiter + "'"] + args
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    # ignore certain errors
    err = [line for line in err.splitlines() if line]
    # (fix for bjobs display_flexibleOutput bug)
    err = [line for line in err if "display_flexibleOutput: Failed to get the "
           "value of job_name" not in line]
    if err:
        return []
    out = out.splitlines()[1:]  # get rid of header
    joborder = []
    jobs = {}
    for line in out:
        job = dict(zip(keys, line.split(delimiter)))
        for key, val in job.iteritems():
            if val == "-":
                job[key] = None
            elif key in ("exit_code", "nexec_host", "slots", "job_priority",
                         "min_req_proc", "max_req_proc"):
                job[key] = int(val)
            elif key in ("cpu_used", "run_time", "idle_factor"):
                job[key] = float(val.split()[0])
            elif key in ("submit_time", "start_time", "finish_time"):
                if val[-1] in "ELXA":
                    val = val[:-2]
                job[key] = mktime(strptime(val,
                                           "%b %d %H:%M:%S %Y"))
            elif key == "time_left":
                if val[-1] in "ELXA":
                    val = val[:-2]
                try:
                    v = val.split(":")
                    job[key] = 60 * (60 * int(v[0]) + int(v[1]))
                except:
                    job[key] = mktime(strptime(year + " " + val,
                                               "%Y %b %d %H:%M"))
            elif key == "%complete":
                job[key] = float(val.split("%")[0])
            elif key in ("exec_host", "alloc_slot"):
                val = val.split(":")
                hosts = {}
                for v in val:
                    if "*" in v:
                        v = v.split("*")
                        hosts[v[1]] = int(v[0])
                    else:
                        hosts[v] = 1
                job[key] = hosts
            elif key in ("swap", "mem", "avg_mem", "max_mem", "memlimit",
                         "swaplimit", "corelimit", "stacklimit"):
                val = val.split()
                job[key] = parsemem(val[0], val[1][0])
            elif key == "pids":
                if val:
                    job[key] = map(int, val.split(","))
                else:
                    job[key] = []

        # set jet unknown keys
        for key in ("pend_reason", "runlimit", "mail", "exclusive", "resreq",
                    "combined_resreq", "notify_begin", "notify_end"):
            job[key] = None
        # info from resreq
        if job["effective_resreq"]:
            job["exclusive"] = "exclusive=1" in job["effective_resreq"]
            if "runlimit" in job["effective_resreq"]:
                match = re.match("runlimit=\d+", job["effective_resreq"])
                job["runlimit"] = int(match.groups()[0])
        elif job["run_time"] and job["%complete"]:
            t = job["run_time"] / job["%complete"] * 100
            # rounding
            if t > 10 * 60 * 60:
                job["runlimit"] = round(t / (60 * 60)) * 60 * 60
            else:
                job["runlimit"] = round(t / 60) * 60
        # extract array id
        if job["job_name"]:
            match = re.match(".*(\[\d+\])$", job["job_name"])
            if match:
                job["jobid"] += match.groups()[0]
        joborder.append(job["jobid"])
        jobs[job["jobid"]] = job
    if not joborder:
        return []
    # set some keys
    for job in jobs.values():
        job.update({
            "interactive": None,
            "pend_reason": [],
            "host_req": []
        })
    if fast:
        for job in jobs.values():
            job.update({alias: job[key] for alias, key in aliases})
        return [jobs[jid] for jid in joborder]
    # get more accurate timestamps from -W output
    try:
        out = check_output(["bjobs", "-noheader", "-W"] + joborder)
    except CalledProcessError as e:
        out = e.output
    for line in out.splitlines():
        line = line.split()
        if len(line) != 15:
            continue
        jobid = line[0]
        match = re.match(".*(\[\d+\])$", line[-9])
        if match:
            jobid += match.groups()[0]
        job = jobs[jobid]
        for n, key in (
                (-8, "submit_time"),
                (-2, "start_time"),
                (-1, "finish_time")
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
        try:
            out = check_output(["bjobs", "-p"] + pids)
        except CalledProcessError as e:
            out = e.output
        job = None
        for line in out.split("\n")[1:-1]:
            if line[0] == " " or line[:4] == "JOBS":
                # pending reason
                if ":" in line:
                    match = re.match(" ?(.*): (\d+) hosts?;", line).groups()
                    job["pend_reason"].append((match[0], int(match[1])))
                else:
                    match = re.match(" ?(.*);", line).groups()
                    job["pend_reason"].append((match[0], True))
            else:
                if job:
                    job["pend_reason"].sort(key=lambda p: -p[1])
                # next job
                line = line.split()
                jobid = line[0]
                match = re.match(".*(\[\d+\])$", " ".join(line[5:-3]))
                if match:
                    jobid += match.groups()[0]
                job = jobs[jobid]
                job["pend_reason"] = []
    # get -UF (long) output (may be restricted)
    try:
        out = check_output(["bjobs", "-UF"] + joborder)
    except CalledProcessError as e:
        out = e.output
    out = out.split(78 * "-" + "\n")
    for jobout in out:
        lines = [line.strip() for line in jobout.splitlines()]
        jobid = re.match("Job <(\d+(?:\[\d+\])?)>", lines[1]).groups()[0]
        job = jobs[jobid]
        # name  (fix for bjobs display_flexibleOutput bug)
        match = re.search("Name <(.*?)>", lines[1])
        if match:
            job["job_name"] = match.groups()[0]
        # mail
        match = re.search("Mail <(.*?)>", lines[1])
        if match:
            job["mail"] = match.groups()[0]
        # flags
        job["exclusive"] = "Exclusive Execution" in lines[2]
        job["notify_begin"] = "Notify when job begins" in lines[2]
        job["notify_end"] = bool(re.search("Notify when job (?:begins/)?ends",
                                           lines[2]))
        job["interactive"] = "Interactive pseudo-terminal shell" in lines[1]
        job["X11"] = "ssh X11 forwarding mode" in lines[1]
        # resource request
        match = re.search("Requested Resources <(.*?)>[,;]", lines[2])
        if match:
            job["resreq"] = match.groups()[0]
        if lines[-2].startswith("Combined: "):
            job["combined_resreq"] = lines[-2].split(": ", 1)[1]
        # requested hosts
        match = re.search("Specified Hosts <(.*?)>(?:;|, [^<])", lines[2])
        if match:
            job["host_req"] = match.groups()[0].split(">, <")
        # runlimit
        idx = lines.index("RUNLIMIT")
        job["runlimit"] = int(float(lines[idx + 1].split()[0]) * 60)
        # memlimits
    # aliases
    for job in jobs.values():
        job.update({alias: job[key] for alias, key in aliases})
    return [jobs[jid] for jid in joborder]
