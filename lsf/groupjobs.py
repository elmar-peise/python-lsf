"""Sort the jobs in groups by attributes."""

from collections import defaultdict


def groupjobs(jobs, key):
    """Sort the jobs in groups by attributes."""
    result = defaultdict(list)
    for job in jobs:
        if key == "pend_reason":
            if len(job[key]) == 1:
                group = repr(job[key])
            else:
                group = job["resreq"]
                group += repr(sorted(job[key]))
                group += repr(sorted(job["host_req"]))
            result[group].append(job)
        elif isinstance(job[key], dict):
            for val in job[key]:
                result[val].append(job)
        else:
            result[job[key]].append(job)
    return dict(result)
