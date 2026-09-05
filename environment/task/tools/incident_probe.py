#!/usr/bin/env python3
"""Terminal probes for the restart and submission regression."""

import argparse
import multiprocessing
import io
import json
import os
import subprocess
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Collection, Dict, Type

from psij import (
    Export,
    Import,
    Job,
    JobAttributes,
    JobExecutor,
    JobExecutorConfig,
    JobSpec,
    JobState,
    JobStatus,
    ResourceSpecV1,
    SubmitException,
)
from psij.executors.batch.batch_scheduler_executor import (
    BatchSchedulerExecutor,
    BatchSchedulerExecutorConfig,
    _QueuePollThread,
)
from psij.executors.batch.lsf import LsfExecutorConfig, LsfJobExecutor
from psij.executors.batch.pbspro import PBSProExecutorConfig, PBSProJobExecutor
from psij.executors.batch.slurm import SlurmExecutorConfig, SlurmJobExecutor
from psij.launchers.single import SingleLauncher


def _fork_local_child(connection: object) -> None:
    try:
        executor = JobExecutor.get_instance("local")
        job = Job(JobSpec(executable="/bin/true", launcher="single"))
        executor.submit(job)
        status = job.wait(timeout=timedelta(seconds=2))
        connection.send({  # type: ignore[attr-defined]
            "pid": os.getpid(),
            "state": None if status is None else str(status.state),
            "exit_code": None if status is None else status.exit_code,
        })
    except BaseException as error:
        connection.send({"error": repr(error)})  # type: ignore[attr-defined]
    finally:
        connection.close()  # type: ignore[attr-defined]


class _RecoveryProbeExecutor(BatchSchedulerExecutor):
    _NAME_ = "recovery-probe"

    def __init__(self, root: Path) -> None:
        self.parse_error = False
        config = BatchSchedulerExecutorConfig(
            work_directory=root,
            initial_queue_polling_delay=0,
            queue_polling_interval=1,
            queue_polling_error_threshold=2,
            completion_grace_period=0.5,
        )
        super().__init__(config=config)

    def _start_queue_poll_thread(self) -> _QueuePollThread:
        return _QueuePollThread("recovery probe poller", self.config, self)

    def _run_command(self, cmd: list[str]) -> str:
        return ""

    def generate_submit_script(self, job: Job, context: Dict[str, object],
                               submit_file: object) -> None:
        raise NotImplementedError()

    def get_submit_command(self, job: Job, submit_file_path: Path) -> list[str]:
        return ["probe-submit"]

    def job_id_from_submit_output(self, out: str) -> str:
        return out

    def get_cancel_command(self, native_id: str) -> list[str]:
        return ["probe-cancel", native_id]

    def process_cancel_command_output(self, exit_code: int, out: str) -> None:
        return None

    def get_status_command(self, native_ids: Collection[str]) -> list[str]:
        return ["probe-status", *native_ids]

    def parse_status_output(self, exit_code: int, out: str) -> Dict[str, JobStatus]:
        if self.parse_error:
            raise ValueError("synthetic malformed scheduler status")
        return {}


class _SubmissionProbeExecutor(BatchSchedulerExecutor):
    """Scheduler-free submitter used to expose publication rollback."""

    _NAME_ = "submission-probe"

    def __init__(self, root: Path) -> None:
        self.fail_submit = True
        self.expected_native_id = "probe-native-17"
        super().__init__(config=BatchSchedulerExecutorConfig(work_directory=root))

    def _start_queue_poll_thread(self) -> _QueuePollThread:
        return _QueuePollThread("submission probe poller", self.config, self)

    def _create_script_context(self, job: Job) -> Dict[str, object]:
        return {"job": job}

    def generate_submit_script(self, job: Job, context: Dict[str, object],
                               submit_file: object) -> None:
        submit_file.write("#!/bin/sh\n")  # type: ignore[attr-defined]

    def get_submit_command(self, job: Job, submit_file_path: Path) -> list[str]:
        return ["probe-submit", str(submit_file_path)]

    def job_id_from_submit_output(self, out: str) -> str:
        return self.expected_native_id

    def get_cancel_command(self, native_id: str) -> list[str]:
        return ["probe-cancel", native_id]

    def process_cancel_command_output(self, exit_code: int, out: str) -> None:
        return None

    def get_status_command(self, native_ids: Collection[str]) -> list[str]:
        return ["probe-status", *native_ids]

    def parse_status_output(self, exit_code: int, out: str) -> Dict[str, JobStatus]:
        return {}

    def _run_command(self, cmd: list[str]) -> str:
        if self.fail_submit:
            raise subprocess.CalledProcessError(73, cmd, output="scheduler unavailable")
        return "accepted"


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


def recovery_probe() -> None:
    if "fork" not in multiprocessing.get_all_start_methods():
        raise RuntimeError("the recovery probe requires the POSIX fork start method")

    JobExecutor.get_instance("local")
    context = multiprocessing.get_context("fork")
    parent_connection, child_connection = context.Pipe(duplex=False)
    child = context.Process(target=_fork_local_child, args=(child_connection,))
    child.start()
    child_connection.close()
    try:
        if not parent_connection.poll(4):
            raise AssertionError("fork child local job did not report a terminal state")
        result = parent_connection.recv()
        child.join(2)
        assert child.exitcode == 0, result
        assert "error" not in result, result
        assert result["state"] == "COMPLETED", result
        assert result["exit_code"] == 0, result
        print("fork child local job completed with a child-owned reaper")
    finally:
        parent_connection.close()
        if child.is_alive():
            child.terminate()
        child.join(2)

    with tempfile.TemporaryDirectory() as td:
        executor = _RecoveryProbeExecutor(Path(td))
        executor.work_directory.mkdir(parents=True)
        native_id = "recovered-17"
        job = Job(JobSpec(executable="/bin/true", launcher="single"))
        job._native_id = native_id
        job.executor = executor
        job.status = JobStatus(JobState.ACTIVE)
        executor._queue_poll_thread.register_job(job)

        executor._queue_poll_thread._poll()
        assert job.status.state == JobState.ACTIVE
        record = executor.work_directory / f"{native_id}.ec"
        record.write_text("0\n", encoding="ascii")
        executor._queue_poll_thread._poll()
        assert job.status.state == JobState.COMPLETED
        assert job.status.exit_code == 0
        print("delayed completion evidence avoided false success and then finalized")


def lifecycle_probe() -> None:
    """Exercise atomic submit publication and whole-poll error accounting."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        executor = JobExecutor.get_instance(
            "local", config=JobExecutorConfig(work_directory=root / "launcher"))
        observed: list[tuple[JobState, str | None]] = []
        spec = JobSpec(
            executable="/bin/true",
            directory=root / "missing-directory",
            launcher="single",
        )
        job = Job(spec)
        job.set_job_status_callback(
            lambda current, status: observed.append((status.state, current.native_id)))
        try:
            executor.submit(job)
        except SubmitException:
            pass
        else:
            raise AssertionError("local spawn in a missing directory unexpectedly succeeded")
        assert job.status.state == JobState.NEW
        assert job.executor is None
        assert job.native_id is None
        assert observed == []

        spec.directory = root
        executor.submit(job)
        status = job.wait(timeout=timedelta(seconds=3))
        assert status is not None and status.state == JobState.COMPLETED
        assert status.exit_code == 0
        assert job.native_id is not None
        assert all(native_id == job.native_id for _state, native_id in observed)
        print("local submit rollback allowed a clean retry with stable callback identity")

        batch = _SubmissionProbeExecutor(root / "batch")
        batch_job = Job(JobSpec(executable="/bin/true", launcher="single"))
        submit_file = batch.work_directory / f"{batch_job.id}.job"
        try:
            batch.submit(batch_job)
        except SubmitException:
            pass
        else:
            raise AssertionError("synthetic failed batch command unexpectedly succeeded")
        assert batch_job.status.state == JobState.NEW
        assert batch_job.executor is None
        assert batch_job.native_id is None
        assert not submit_file.exists()
        batch.fail_submit = False
        batch.submit(batch_job)
        assert batch_job.status.state == JobState.QUEUED
        assert batch_job.native_id == batch.expected_native_id
        print("batch submit rollback removed unpublished state and the stale submit file")

        recovery = _RecoveryProbeExecutor(root / "poll")
        native_id = "parse-error-17"
        recovered = Job(JobSpec(executable="/bin/true", launcher="single"))
        recovered._native_id = native_id
        recovered.executor = recovery
        recovered.status = JobStatus(JobState.ACTIVE)
        recovery._queue_poll_thread.register_job(recovered)
        recovery.parse_error = True
        recovery._queue_poll_thread._poll()
        recovery._queue_poll_thread._poll()
        assert recovered.status.state == JobState.ACTIVE
        assert recovery._queue_poll_thread._poll_error_count == 2
        recovery._queue_poll_thread._poll()
        assert recovered.status.state == JobState.FAILED
        assert native_id not in recovery._queue_poll_thread._jobs
        print("parser failures accumulated across complete polling attempts")


PROBES = {
    "roundtrip": roundtrip_probe,
    "batch": batch_probe,
    "wait": wait_probe,
    "launcher": launcher_probe,
    "lifecycle": lifecycle_probe,
    "recovery": recovery_probe,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("probe", choices=sorted(PROBES))
    args = parser.parse_args()
    PROBES[args.probe]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
