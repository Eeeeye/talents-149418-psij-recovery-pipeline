#!/usr/bin/env python3
from datetime import timedelta

from psij import JobAttributes, JobSpec, ResourceSpecV1


spec = JobSpec(
    executable="/bin/true",
    resources=ResourceSpecV1(process_count=1),
    attributes=JobAttributes(duration=timedelta(minutes=1)),
)
assert spec.resources.computed_process_count == 1
print("PSI/J starter import is healthy")
