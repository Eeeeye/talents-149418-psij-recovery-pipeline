#!/usr/bin/env python3
"""Terminal probes for the restart and submission regression."""

import argparse
import io
import json
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Dict, Type

from psij import Export, Import, Job, JobAttributes, JobSpec, JobState, JobStatus, ResourceSpecV1
from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutor
from psij.executors.batch.lsf import LsfExecutorConfig, LsfJobExecutor
from psij.executors.batch.pbspro import PBSProExecutorConfig, PBSProJobExecutor
from psij.executors.batch.slurm import SlurmExecutorConfig, SlurmJobExecutor
from psij.launchers.single import SingleLauncher


def make_spec() -> JobSpec:
    return JobSpec(
        name="restartable-cfd",
        executable="/bin/false",
        arguments=["--case", "wing 17"],
        directory=Path("/tmp/cfd work"),
        inherit_environment=False,
        environment={"OMP_NUM_THREADS": "6"},
        stdout_path=Path("/tmp/cfd work/stdout.log"),
        stderr_path=Path("/tmp/cfd work/stderr.log"),
        resources=ResourceSpecV1(
            node_count=2,
            process_count=12,
            processes_per_node=6,
            cpu_cores_per_process=6,
            gpu_cores_per_process=1,
            exclusive_node_use=True,
        ),
        attributes=JobAttributes(
            duration=timedelta(days=1, hours=2, minutes=3, seconds=4),
            queue_name="gpu",
            project_name="aero",
            reservation_id="maint-17",
            custom_attributes={
                "slurm.qos": "normal",
                "pbs.place": "scatter",
                "lsf.spool": "y",
            },
        ),
        pre_launch=Path("/tmp/cfd work/pre.sh"),
        post_launch=Path("/tmp/cfd work/post.sh"),
        launcher="single",
    )


def roundtrip_probe() -> None:
    spec = make_spec()
    with tempfile.TemporaryDirectory() as td:
        manifest = Path(td) / "job.json"
        Export().export(spec, str(manifest))
        restored = Import().load(str(manifest))
        print("resources_type", type(restored.resources).__name__)
        print("duration_type", type(restored.attributes.duration).__name__)
        print("project_name", restored.attributes.project_name)
        print("pre_launch", restored.pre_launch)
        print("post_launch", restored.post_launch)
        print("launcher", restored.launcher)
        assert isinstance(restored, JobSpec)
        assert isinstance(restored.resources, ResourceSpecV1)
        assert isinstance(restored.attributes.duration, timedelta)
        assert restored.resources.processes_per_node == 6
        assert restored.attributes.project_name == "aero"
        assert restored.pre_launch == spec.pre_launch
        assert restored.post_launch == spec.post_launch
        assert restored.launcher == "single"
        print(json.dumps({"manifest": str(manifest), "restored": True}))


def render(executor_type: Type[BatchSchedulerExecutor], config: object, name: str) -> str:
    setattr(executor_type, "_NAME_", name)
    executor = executor_type(url=None, config=config)  # type: ignore[call-arg]
    job = Job(make_spec())
    out = io.StringIO()
    executor.generate_submit_script(job, executor._create_script_context(job), out)
    return out.getvalue()


def batch_probe() -> None:
    expected: Dict[str, tuple[str, str]] = {
        "slurm": ("#SBATCH --time=1-02:03:04", '#SBATCH --qos="normal"'),
        "pbs": ("#PBS -l walltime=26:03:04", '#PBS -place "scatter"'),
        "lsf": ("#BSUB -W 26:04", '#BSUB -spool "y"'),
    }
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        rendered = {
            "slurm": render(SlurmJobExecutor, SlurmExecutorConfig(work_directory=root), "slurm"),
            "pbs": render(PBSProJobExecutor, PBSProExecutorConfig(work_directory=root), "pbs"),
            "lsf": render(LsfJobExecutor, LsfExecutorConfig(work_directory=root), "lsf"),
        }
    for scheduler, script in rendered.items():
        interesting = [
            line for line in script.splitlines()
            if any(part in line for part in ("--time", "walltime", "#BSUB -W", "qos", "place", "spool"))
        ]
        print(f"{scheduler}: {json.dumps(interesting)}")
        for directive in expected[scheduler]:
            assert directive in script, f"{scheduler} missing {directive!r}"
        print(f"{scheduler}: directives OK")


def wait_probe() -> None:
    completed = Job()
    completed.status = JobStatus(JobState.COMPLETED, exit_code=0)
    status = completed.wait(timeout=timedelta(milliseconds=50), target_states=[JobState.QUEUED])
    assert status is not None and status.state == JobState.COMPLETED

    failed = Job()
    failed.status = JobStatus(JobState.FAILED, exit_code=9)
    status = failed.wait(timeout=timedelta(milliseconds=50), target_states=JobState.ACTIVE)
    assert status is not None and status.state == JobState.FAILED
    print("fast and failed terminal states are observable")


def launcher_probe() -> None:
    launcher = SingleLauncher()
    normal = "_PSI_J_LAUNCHER_DONE\n"
    classified_failure = launcher.is_launcher_failure(normal)
    print("normal_marker_is_failure", classified_failure)
    assert classified_failure is False
    assert launcher.is_launcher_failure("wrapper failed\n") is True
    print("launcher marker classification OK")


PROBES = {
    "roundtrip": roundtrip_probe,
    "batch": batch_probe,
    "wait": wait_probe,
    "launcher": launcher_probe,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("probe", choices=sorted(PROBES))
    args = parser.parse_args()
    PROBES[args.probe]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
