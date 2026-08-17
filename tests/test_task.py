#!/usr/bin/env python3
import io
import gc
import hashlib
import importlib.util
import json
import logging
import math
import multiprocessing
import os
import pkgutil
import re
import secrets
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import warnings
import weakref
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, Tuple, Type
from unittest import mock


ROOT = Path("/workspace")
sys.path.insert(0, str(ROOT / "src"))
warnings.filterwarnings("ignore", category=ResourceWarning)

from psij import (  # noqa: E402
    Export,
    Import,
    Job,
    JobAttributes,
    JobExecutor,
    JobSpec,
    JobState,
    JobStatus,
    InvalidJobException,
    ResourceSpecV1,
)
from psij.executors.batch.batch_scheduler_executor import (  # noqa: E402
    BatchSchedulerExecutor,
    BatchSchedulerExecutorConfig,
    _QueuePollThread,
)
from psij.executors.batch.lsf import LsfExecutorConfig, LsfJobExecutor  # noqa: E402
from psij.executors.batch.pbspro import PBSProExecutorConfig, PBSProJobExecutor  # noqa: E402
from psij.executors.batch.slurm import SlurmExecutorConfig, SlurmJobExecutor  # noqa: E402
from psij.launchers.single import SingleLauncher  # noqa: E402


def _run_local_job_after_fork(connection: object) -> None:
    """Child entry point for the inherited-thread regression."""
    try:
        executor = JobExecutor.get_instance("local")
        job = Job(JobSpec(executable="/bin/true", launcher="single"))
        executor.submit(job)
        status = job.wait(timeout=timedelta(seconds=2))
        result = {
            "pid": os.getpid(),
            "state": None if status is None else status.state.name,
            "exit_code": None if status is None else status.exit_code,
            "reaper_alive": executor._reaper.is_alive(),
        }
        connection.send(result)  # type: ignore[attr-defined]
    except BaseException as error:
        connection.send({"error": repr(error), "pid": os.getpid()})  # type: ignore[attr-defined]
    finally:
        connection.close()  # type: ignore[attr-defined]


class _OfflineRecoveryExecutor(BatchSchedulerExecutor):
    """A scheduler-free executor that exposes deterministic polling barriers."""

    _NAME_ = "offline-recovery"

    def __init__(self, root: Path, *, completion_grace_period: float = 1.0,
                 start_background: bool = False) -> None:
        self.status_map: Dict[str, JobStatus] = {}
        self.poll_started: threading.Event | None = None
        self.poll_release: threading.Event | None = None
        self._start_background = start_background
        config = BatchSchedulerExecutorConfig(
            work_directory=root,
            queue_polling_interval=0.02,
            initial_queue_polling_delay=0.0,
            completion_grace_period=completion_grace_period,
        )
        super().__init__(config=config)

    def _start_queue_poll_thread(self) -> _QueuePollThread:
        poller = _QueuePollThread("offline recovery poller", self.config, self)
        if self._start_background:
            poller.start()
        return poller

    def _run_command(self, cmd: list[str]) -> str:
        if self.poll_started is not None:
            self.poll_started.set()
        if self.poll_release is not None and not self.poll_release.wait(2):
            raise RuntimeError("poll barrier timed out")
        return ""

    def generate_submit_script(self, job: Job, context: Dict[str, object],
                               submit_file: object) -> None:
        raise NotImplementedError()

    def get_submit_command(self, job: Job, submit_file_path: Path) -> list[str]:
        return ["offline-submit"]

    def job_id_from_submit_output(self, out: str) -> str:
        return out

    def get_cancel_command(self, native_id: str) -> list[str]:
        return ["offline-cancel", native_id]

    def process_cancel_command_output(self, exit_code: int, out: str) -> None:
        return None

    def get_status_command(self, native_ids: Iterable[str]) -> list[str]:
        return ["offline-status", *native_ids]

    def parse_status_output(self, exit_code: int, out: str) -> Dict[str, JobStatus]:
        if exit_code != 0:
            raise RuntimeError(out)
        return dict(self.status_map)


def _register_recovered_job(executor: _OfflineRecoveryExecutor, native_id: str) -> Job:
    job = Job(JobSpec(executable="/bin/true", launcher="single"))
    job._native_id = native_id
    job.executor = executor
    job.status = JobStatus(JobState.ACTIVE)
    executor._queue_poll_thread.register_job(job)
    return job


def token(prefix: str) -> str:
    return prefix + secrets.token_hex(6)


def process_is_running(pid: int) -> bool:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return False
    fields_after_name = stat.rsplit(")", 1)[1].split()
    return bool(fields_after_name) and fields_after_name[0] != "Z"


def complete_spec(*, duration: timedelta | None = None,
                  custom: Dict[str, object] | None = None) -> JobSpec:
    suffix = secrets.token_hex(5)
    if duration is None:
        duration = timedelta(days=2, hours=7, minutes=11, seconds=13)
    if custom is None:
        custom = {
            "slurm.qos": token("q"),
            "pbs.place": token("p"),
            "lsf.spool": token("s"),
            "audit": {
                "enabled": True,
                "attempts": 3,
                "labels": ["one", None, False],
            },
        }
    base = Path(f"/scratch/recovery case {suffix}")
    return JobSpec(
        name=f"recover-{suffix}",
        executable="/opt/apps/solver",
        arguments=["--case", f"mesh {suffix}.dat", "--resume"],
        directory=base,
        inherit_environment=False,
        environment={"OMP_NUM_THREADS": "8", "RUN_ID": suffix},
        stdin_path=base / "stdin.txt",
        stdout_path=base / "stdout.log",
        stderr_path=base / "stderr.log",
        resources=ResourceSpecV1(
            node_count=3,
            process_count=12,
            processes_per_node=4,
            cpu_cores_per_process=8,
            gpu_cores_per_process=2,
            exclusive_node_use=False,
        ),
        attributes=JobAttributes(
            duration=duration,
            queue_name=f"queue-{suffix}",
            project_name=f"project-{suffix}",
            reservation_id=f"reservation-{suffix}",
            custom_attributes=custom,
        ),
        pre_launch=base / "pre launch.sh",
        post_launch=base / "post launch.sh",
        launcher="single",
    )


def assert_specs_equal(test: unittest.TestCase, expected: JobSpec, actual: JobSpec) -> None:
    for field in (
        "name",
        "executable",
        "arguments",
        "directory",
        "inherit_environment",
        "environment",
        "stdin_path",
        "stdout_path",
        "stderr_path",
        "pre_launch",
        "post_launch",
        "launcher",
    ):
        test.assertEqual(getattr(actual, field), getattr(expected, field), field)

    test.assertIsInstance(actual.directory, Path)
    test.assertIsInstance(actual.stdout_path, Path)
    test.assertIsInstance(actual.resources, ResourceSpecV1)
    expected_resources = expected.resources
    actual_resources = actual.resources
    assert isinstance(expected_resources, ResourceSpecV1)
    assert isinstance(actual_resources, ResourceSpecV1)
    for field in (
        "node_count",
        "process_count",
        "processes_per_node",
        "cpu_cores_per_process",
        "gpu_cores_per_process",
        "exclusive_node_use",
    ):
        test.assertEqual(getattr(actual_resources, field), getattr(expected_resources, field), field)

    test.assertIsInstance(actual.attributes.duration, timedelta)
    for field in ("duration", "queue_name", "project_name", "reservation_id"):
        test.assertEqual(getattr(actual.attributes, field), getattr(expected.attributes, field), field)
    test.assertEqual(actual.attributes._custom_attributes, expected.attributes._custom_attributes)


class PersistenceTests(unittest.TestCase):
    def test_complete_round_trip_preserves_values_and_types(self) -> None:
        original = complete_spec()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "manifest with spaces.json"
            self.assertIs(Export().export(original, str(path)), True)
            restored = Import().load(str(path))
            self.assertIsInstance(restored, JobSpec)
            assert isinstance(restored, JobSpec)
            assert_specs_equal(self, original, restored)

            document = json.loads(path.read_text(encoding="utf-8"))
            self.assertIsInstance(document["version"], (int, float))
            self.assertEqual(document["type"], "JobSpec")
            self.assertIsInstance(document["data"], dict)
            self.assertEqual(document["data"]["resources"]["processes_per_node"], 4)
            self.assertEqual(document["data"]["pre_launch"], str(original.pre_launch))
            self.assertEqual(document["data"]["post_launch"], str(original.post_launch))
            self.assertEqual(document["data"]["launcher"], "single")

    def test_json_custom_attribute_types_are_not_stringified(self) -> None:
        nested = {
            "flag": True,
            "count": 17,
            "ratio": 2.5,
            "unset": None,
            "list": [1, False, None, "值"],
            "dict": {"inner": 9},
        }
        original = complete_spec(custom={"slurm.extra": nested, "plain": [1, 2, 3]})
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "manifest.json"
            Export().export(original, str(path))
            restored = Import().load(str(path))
        assert isinstance(restored, JobSpec)
        self.assertEqual(restored.attributes._custom_attributes, original.attributes._custom_attributes)
        self.assertIs(restored.attributes._custom_attributes["slurm.extra"]["flag"], True)
        self.assertIsNone(restored.attributes._custom_attributes["slurm.extra"]["unset"])

    def test_repeated_export_publishes_the_latest_complete_spec(self) -> None:
        first = complete_spec(duration=timedelta(hours=1))
        second = complete_spec(duration=timedelta(days=1, seconds=1))
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "restart.json"
            Export().export(first, str(path))
            Export().export(second, str(path))
            Export().export(second, str(path))
            restored = Import().load(str(path))
        assert isinstance(restored, JobSpec)
        assert_specs_equal(self, second, restored)

    def test_manifest_survives_a_fresh_python_process(self) -> None:
        marker = token("process-")
        export_code = """
import sys
from datetime import timedelta
from pathlib import Path
from psij import Export, JobAttributes, JobSpec, ResourceSpecV1
path, marker = sys.argv[1:]
spec = JobSpec(
    name=marker,
    executable='/bin/echo',
    arguments=[marker],
    directory=Path('/scratch') / marker,
    resources=ResourceSpecV1(node_count=2, process_count=6, processes_per_node=3),
    attributes=JobAttributes(
        duration=timedelta(days=1, seconds=17),
        project_name='project-' + marker,
        custom_attributes={'slurm.qos': marker, 'attempt': 4},
    ),
    launcher='single',
)
Export().export(spec, path)
"""
        import_code = """
import json
import sys
from psij import Import
spec = Import().load(sys.argv[1])
print(json.dumps({
    'name': spec.name,
    'resource_type': type(spec.resources).__name__,
    'processes_per_node': spec.resources.processes_per_node,
    'duration': spec.attributes.duration.total_seconds(),
    'project': spec.attributes.project_name,
    'custom': spec.attributes._custom_attributes,
    'launcher': spec.launcher,
}))
"""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "process restart.json"
            subprocess.run([sys.executable, "-c", export_code, str(path), marker], check=True)
            result = subprocess.run(
                [sys.executable, "-c", import_code, str(path)],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            )
            self.assertEqual(list(path.parent.iterdir()), [path])
        restored = json.loads(result.stdout)
        self.assertEqual(restored["name"], marker)
        self.assertEqual(restored["resource_type"], "ResourceSpecV1")
        self.assertEqual(restored["processes_per_node"], 3)
        self.assertEqual(restored["duration"], 86417.0)
        self.assertEqual(restored["project"], "project-" + marker)
        self.assertEqual(restored["custom"], {"slurm.qos": marker, "attempt": 4})
        self.assertEqual(restored["launcher"], "single")

    def test_legacy_v01_manifest_and_historical_ppn_key(self) -> None:
        suffix = secrets.token_hex(4)
        duration = timedelta(days=3, hours=4, minutes=5, seconds=6)
        data = {
            "name": f"legacy-{suffix}",
            "executable": "/bin/echo",
            "arguments": [suffix],
            "directory": f"/scratch/legacy {suffix}",
            "inherit_environment": False,
            "environment": {"CASE": suffix},
            "stdin_path": None,
            "stdout_path": f"/scratch/legacy {suffix}/out",
            "stderr_path": f"/scratch/legacy {suffix}/err",
            "resources": {
                "node_count": 2,
                "process_count": 10,
                "process_per_node": 5,
                "cpu_cores_per_process": 3,
                "gpu_cores_per_process": 0,
                "exclusive_node_use": True,
            },
            "attributes": {
                "duration": str(duration),
                "queue_name": "debug",
                "project_name": f"project-{suffix}",
                "reservation_id": None,
                "custom_attributes": {"slurm.qos": "normal", "retry": 2},
            },
            "launcher": "single",
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "legacy.json"
            path.write_text(json.dumps({"version": 0.1, "type": "JobSpec", "data": data}),
                            encoding="utf-8")
            restored = Import().load(str(path))
        self.assertIsInstance(restored, JobSpec)
        assert isinstance(restored, JobSpec)
        self.assertIsInstance(restored.resources, ResourceSpecV1)
        assert isinstance(restored.resources, ResourceSpecV1)
        self.assertEqual(restored.resources.processes_per_node, 5)
        self.assertEqual(restored.attributes.duration, duration)
        self.assertIsInstance(restored.attributes.duration, timedelta)
        self.assertEqual(restored.attributes.project_name, f"project-{suffix}")
        self.assertEqual(restored.launcher, "single")
        self.assertIsNone(restored.pre_launch)
        self.assertIsNone(restored.post_launch)

    def test_legacy_missing_fields_use_new_jobspec_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "minimal-legacy.json"
            path.write_text(
                json.dumps({"version": 0.1, "type": "JobSpec", "data": {}}),
                encoding="utf-8",
            )
            restored = Import().load(str(path))

        self.assertIsInstance(restored, JobSpec)
        assert isinstance(restored, JobSpec)
        expected = JobSpec()
        for field in (
            "name",
            "executable",
            "arguments",
            "directory",
            "inherit_environment",
            "environment",
            "stdin_path",
            "stdout_path",
            "stderr_path",
            "resources",
            "pre_launch",
            "post_launch",
            "launcher",
        ):
            with self.subTest(field=field):
                self.assertEqual(getattr(restored, field), getattr(expected, field))
        self.assertIsInstance(restored.attributes, JobAttributes)
        self.assertEqual(restored.attributes.duration, expected.attributes.duration)
        self.assertIsNone(restored.attributes.queue_name)
        self.assertIsNone(restored.attributes.project_name)
        self.assertIsNone(restored.attributes.reservation_id)
        self.assertIsNone(restored.attributes._custom_attributes)

    def test_invalid_manifests_are_rejected(self) -> None:
        valid_data = {
            "name": "valid",
            "executable": "/bin/true",
            "arguments": [],
            "directory": None,
            "inherit_environment": True,
            "environment": {},
            "stdin_path": None,
            "stdout_path": None,
            "stderr_path": None,
            "resources": None,
            "attributes": None,
            "launcher": None,
        }
        invalid_documents = [
            {"version": 99, "type": "JobSpec", "data": valid_data},
            {"version": 0.1, "type": "Other", "data": valid_data},
            {"version": 0.1, "type": "JobSpec", "data": []},
            {"version": 0.1, "type": "JobSpec",
             "data": {**valid_data, "resources": {"version": 77}}},
            {"version": 0.1, "type": "JobSpec",
             "data": {**valid_data, "attributes": {
                 "duration": "not-a-duration",
                 "queue_name": None,
                 "project_name": None,
                 "reservation_id": None,
                 "custom_attributes": {},
             }}},
            {"version": 0.1, "type": "JobSpec",
             "data": {**valid_data, "attributes": {
                 "duration": "0:10:00",
                 "queue_name": None,
                 "project_name": None,
                 "reservation_id": None,
                 "custom_attributes": {"invalid": float("nan")},
             }}},
        ]
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            malformed = root / "malformed.json"
            malformed.write_text("{truncated", encoding="utf-8")
            with self.assertRaises((ValueError, TypeError)):
                Import().load(str(malformed))
            for index, document in enumerate(invalid_documents):
                path = root / f"invalid-{index}.json"
                path.write_text(json.dumps(document), encoding="utf-8")
                with self.subTest(index=index), self.assertRaises((ValueError, TypeError)):
                    Import().load(str(path))

    def test_failed_export_does_not_replace_existing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "existing.json"
            for invalid in (
                {1, 2, 3},
                {1: "non-string-key"},
                ("tuple",),
                float("nan"),
                float("inf"),
                float("-inf"),
            ):
                with self.subTest(invalid=repr(invalid)):
                    sentinel = secrets.token_bytes(96)
                    path.write_bytes(sentinel)
                    spec = complete_spec(custom={"invalid": invalid})
                    with self.assertRaises((TypeError, ValueError)):
                        Export().export(spec, str(path))
                    self.assertEqual(path.read_bytes(), sentinel)
                    self.assertEqual(list(path.parent.iterdir()), [path])


class ModificationBoundaryTests(unittest.TestCase):
    def test_candidate_visible_files_outside_src_are_unchanged(self) -> None:
        expected: Dict[str, str] = {}
        for line in Path("/tests/protected-files.sha256").read_text(encoding="utf-8").splitlines():
            digest, relative = line.split(maxsplit=1)
            expected[relative] = digest
        for relative, digest in expected.items():
            path = ROOT / relative
            self.assertTrue(path.is_file(), f"protected file missing: {relative}")
            observed = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(observed, digest, f"protected file changed: {relative}")


class PreservedBehaviorTests(unittest.TestCase):
    def test_plugin_discovery_debug_logging_survives_a_missing_descriptor(self) -> None:
        import psij

        class MissingModuleFinder:
            def find_spec(self, name: str, path: object = None) -> None:
                return None

        plugin_path = f"/tmp/{token('plugins-')}/psij-descriptors"
        module = pkgutil.ModuleInfo(
            MissingModuleFinder(), token("missing_descriptor_"), False
        )
        with self.assertLogs(psij.logger, level=logging.DEBUG) as captured:
            psij._load_plugins("/tmp/plugin-root", plugin_path, module)
        diagnostics = "\n".join(captured.output)
        self.assertIn(plugin_path, diagnostics)
        self.assertIn(module.name, diagnostics)

    def test_flux_stderr_path_is_assigned_to_the_submitted_jobspec(self) -> None:
        class FakeFluxJobspec:
            created: "FakeFluxJobspec | None" = None

            def __init__(self) -> None:
                self.stdin: Path | None = None
                self.stdout: Path | None = None
                self.stderr: Path | None = None
                self.duration: float | None = None

            @classmethod
            def from_command(cls, argv: list[str], **kwargs: object) -> "FakeFluxJobspec":
                cls.created = cls()
                return cls.created

        class FakeFluxExecutorFuture:
            pass

        flux_module = types.ModuleType("flux")
        flux_job_module = types.ModuleType("flux.job")
        flux_job_module.FluxExecutorFuture = FakeFluxExecutorFuture  # type: ignore[attr-defined]
        flux_job_module.JobspecV1 = FakeFluxJobspec  # type: ignore[attr-defined]
        flux_module.job = flux_job_module  # type: ignore[attr-defined]

        module_name = token("psij_flux_regression_")
        source = ROOT / "src/psij/executors/flux.py"
        module_spec = importlib.util.spec_from_file_location(module_name, source)
        self.assertIsNotNone(module_spec)
        assert module_spec is not None and module_spec.loader is not None
        flux_executor_module = importlib.util.module_from_spec(module_spec)
        with mock.patch.dict(
            sys.modules, {"flux": flux_module, "flux.job": flux_job_module}
        ):
            module_spec.loader.exec_module(flux_executor_module)

        submitted: list[FakeFluxJobspec] = []

        class RecordingExecutor:
            def submit(self, jobspec: FakeFluxJobspec) -> object:
                submitted.append(jobspec)
                return object()

        executor = object.__new__(flux_executor_module.FluxJobExecutor)
        executor._flux_executor = RecordingExecutor()
        executor._check_job = lambda job: job.spec
        executor._add_flux_callbacks = lambda job, future: None

        stdin_path = Path(f"/tmp/{token('flux-stdin-')}.txt")
        stdout_path = Path(f"/tmp/{token('flux-stdout-')}.log")
        stderr_path = Path(f"/tmp/{token('flux-stderr-')}.log")
        job = Job(JobSpec(
            executable="/bin/echo",
            arguments=["offline"],
            stdin_path=stdin_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            resources=ResourceSpecV1(
                process_count=1,
                cpu_cores_per_process=1,
                exclusive_node_use=False,
            ),
            attributes=JobAttributes(duration=timedelta(seconds=17)),
        ))
        executor.submit(job)

        self.assertEqual(len(submitted), 1)
        self.assertIs(submitted[0], FakeFluxJobspec.created)
        self.assertEqual(submitted[0].stdin, stdin_path)
        self.assertEqual(submitted[0].stdout, stdout_path)
        self.assertEqual(submitted[0].stderr, stderr_path)
        self.assertEqual(submitted[0].duration, 17.0)

    def test_resource_constraint_validation_is_unchanged(self) -> None:
        with self.assertRaises(InvalidJobException):
            ResourceSpecV1(node_count=2, process_count=5, processes_per_node=2)
        valid = ResourceSpecV1(node_count=3, processes_per_node=4)
        self.assertEqual(valid.computed_process_count, 12)

    def test_status_transition_reconstruction_and_callbacks_are_unchanged(self) -> None:
        observed: list[JobState] = []
        job = Job()
        job.set_job_status_callback(lambda _job, status: observed.append(status.state))
        job.status = JobStatus(JobState.COMPLETED, exit_code=0)
        self.assertEqual(observed, [JobState.QUEUED, JobState.ACTIVE, JobState.COMPLETED])

    def test_local_executor_still_honors_environment_isolation(self) -> None:
        marker = token("isolated-")
        with tempfile.TemporaryDirectory(prefix="psij-env-") as td:
            root = Path(td)
            spec = JobSpec(
                executable="/usr/bin/env",
                directory=root,
                inherit_environment=False,
                environment={"ONLY_TEST_VALUE": marker},
                stdout_path=root / "stdout.log",
                stderr_path=root / "stderr.log",
                launcher="single",
            )
            job = Job(spec)
            JobExecutor.get_instance("local").submit(job)
            status = job.wait(timeout=timedelta(seconds=5))
            self.assertIsNotNone(status)
            assert status is not None
            self.assertEqual(status.state, JobState.COMPLETED)
            output = (root / "stdout.log").read_text(encoding="utf-8")
            self.assertIn(f"ONLY_TEST_VALUE={marker}", output)
            self.assertNotIn("PYTHONPATH=", output)

    def test_multiple_launcher_bounds_and_reaps_worker_descendants(self) -> None:
        launcher = ROOT / "src/psij/launchers/scripts/multi_launch.sh"
        with tempfile.TemporaryDirectory(prefix="psij-multi-timeout-") as td:
            root = Path(td)
            pid_dir = root / "pids"
            pid_dir.mkdir()
            env = os.environ.copy()
            env["PSIJ_MULTI_LAUNCH_TIMEOUT_SECONDS"] = "1"
            env["PSIJ_TEST_PID_DIR"] = str(pid_dir)
            child = (
                "import os, pathlib, subprocess; "
                "process = subprocess.Popen(['sleep', '30']); "
                "pathlib.Path(os.environ['PSIJ_TEST_PID_DIR'], "
                "os.environ['_PSI_J_PROCESS_INDEX_'] + '.pid').write_text(str(process.pid)); "
                "process.wait()"
            )
            command = [
                "/bin/bash",
                str(launcher),
                "timeout-test",
                str(root / "launcher.log"),
                "",
                "",
                "/dev/null",
                str(root / "stdout.log"),
                str(root / "stderr.log"),
                "2",
                sys.executable,
                "-c",
                child,
            ]
            started = time.monotonic()
            result = subprocess.run(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5,
            )
            elapsed = time.monotonic() - started
            worker_pids = [int(path.read_text()) for path in sorted(pid_dir.glob("*.pid"))]
            try:
                self.assertEqual(len(worker_pids), 2)
                self.assertEqual(result.returncode, 124, result.stderr)
                self.assertLess(elapsed, 4.5)
                self.assertEqual(result.stdout, "_PSI_J_LAUNCHER_DONE\n")
                for _ in range(20):
                    if all(not process_is_running(pid) for pid in worker_pids):
                        break
                    time.sleep(0.05)
                for pid in worker_pids:
                    self.assertFalse(process_is_running(pid),
                                     f"timed-out worker descendant survived: {pid}")
            finally:
                for pid in worker_pids:
                    try:
                        os.kill(pid, 9)
                    except ProcessLookupError:
                        pass

    def test_multiple_launcher_interrupt_cleans_worker_descendants(self) -> None:
        launcher = ROOT / "src/psij/launchers/scripts/multi_launch.sh"
        with tempfile.TemporaryDirectory(prefix="psij-multi-signal-") as td:
            root = Path(td)
            pid_dir = root / "pids"
            pid_dir.mkdir()
            env = os.environ.copy()
            env["PSIJ_MULTI_LAUNCH_TIMEOUT_SECONDS"] = "30"
            env["PSIJ_TEST_PID_DIR"] = str(pid_dir)
            child = (
                "import os, pathlib, subprocess; "
                "process = subprocess.Popen(['sleep', '30']); "
                "pathlib.Path(os.environ['PSIJ_TEST_PID_DIR'], "
                "os.environ['_PSI_J_PROCESS_INDEX_'] + '.pid').write_text(str(process.pid)); "
                "process.wait()"
            )
            command = [
                "/bin/bash",
                str(launcher),
                "signal-test",
                str(root / "launcher.log"),
                "",
                "",
                "/dev/null",
                str(root / "stdout.log"),
                str(root / "stderr.log"),
                "2",
                sys.executable,
                "-c",
                child,
            ]
            process = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            worker_pids: list[int] = []
            try:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    paths = sorted(pid_dir.glob("*.pid"))
                    if len(paths) == 2:
                        worker_pids = [int(path.read_text()) for path in paths]
                        break
                    time.sleep(0.05)
                self.assertEqual(len(worker_pids), 2)
                process.terminate()
                process.communicate(timeout=5)
                self.assertEqual(process.returncode, 143)
                for _ in range(20):
                    if all(not process_is_running(pid) for pid in worker_pids):
                        break
                    time.sleep(0.05)
                for pid in worker_pids:
                    self.assertFalse(process_is_running(pid),
                                     f"interrupted worker descendant survived: {pid}")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
                for pid in worker_pids:
                    try:
                        os.kill(pid, 9)
                    except ProcessLookupError:
                        pass


class RecoveryConcurrencyTests(unittest.TestCase):
    def test_fork_children_use_live_process_local_reapers(self) -> None:
        if "fork" not in multiprocessing.get_all_start_methods():
            self.skipTest("POSIX fork start method is unavailable")

        parent_executor = JobExecutor.get_instance("local")
        self.assertTrue(parent_executor._reaper.is_alive())
        context = multiprocessing.get_context("fork")
        children: list[tuple[object, object, multiprocessing.Process]] = []
        for _ in range(2):
            parent_connection, child_connection = context.Pipe(duplex=False)
            process = context.Process(
                target=_run_local_job_after_fork,
                args=(child_connection,),
            )
            process.start()
            child_connection.close()
            children.append((parent_connection, child_connection, process))

        try:
            results = []
            for parent_connection, _child_connection, process in children:
                self.assertTrue(parent_connection.poll(5),
                                f"fork child {process.pid} did not report completion")
                results.append(parent_connection.recv())
            for (_parent_connection, _child_connection, process), result in zip(children, results):
                process.join(5)
                self.assertFalse(process.is_alive(), f"fork child {process.pid} did not exit")
                self.assertEqual(process.exitcode, 0)
                self.assertNotIn("error", result)
                self.assertEqual(result["state"], "COMPLETED")
                self.assertEqual(result["exit_code"], 0)
                self.assertTrue(result["reaper_alive"])
            self.assertEqual(len({result["pid"] for result in results}), 2)
        finally:
            for parent_connection, _child_connection, process in children:
                parent_connection.close()
                if process.is_alive():
                    process.terminate()
                process.join(2)

    def test_idle_batch_executor_and_poller_are_collectable(self) -> None:
        with tempfile.TemporaryDirectory(prefix="psij-poller-gc-") as td:
            executor = _OfflineRecoveryExecutor(Path(td), start_background=True)
            poller = executor._queue_poll_thread
            executor_reference = weakref.ref(executor)
            del executor

            deadline = time.monotonic() + 2
            while executor_reference() is not None and time.monotonic() < deadline:
                gc.collect()
                time.sleep(0.01)
            self.assertIsNone(executor_reference(), "poller retained its executor")
            poller.join(1)
            self.assertFalse(poller.is_alive(), "poller survived executor collection")

    def test_delayed_completion_record_does_not_create_false_success(self) -> None:
        with tempfile.TemporaryDirectory(prefix="psij-delayed-ec-") as td:
            executor = _OfflineRecoveryExecutor(Path(td), completion_grace_period=1.0)
            executor.work_directory.mkdir(parents=True)
            native_id = token("native-")
            job = _register_recovered_job(executor, native_id)
            record = executor.work_directory / f"{native_id}.ec"

            executor._queue_poll_thread._poll()
            self.assertEqual(job.status.state, JobState.ACTIVE)
            for contents in ("", "7"):
                record.write_text(contents, encoding="ascii")
                executor._queue_poll_thread._poll()
                self.assertEqual(job.status.state, JobState.ACTIVE)
                self.assertIsNone(job.status.exit_code)

            record.write_bytes(b"7\r\n")
            executor._queue_poll_thread._poll()
            self.assertEqual(job.status.state, JobState.FAILED)
            self.assertEqual(job.status.exit_code, 7)

    def test_completion_record_uses_exact_byte_grammar(self) -> None:
        with tempfile.TemporaryDirectory(prefix="psij-ec-grammar-") as td:
            cases: tuple[tuple[bytes, int | None], ...] = (
                (b"0\n", 0),
                (b"-19\r\n", -19),
                (b"+1\n", None),
                (b"1", None),
                (b"1\r", None),
                (b" 1\n", None),
                (b"1\n\n", None),
                (b"\xff\n", None),
            )
            for contents, expected_exit_code in cases:
                with self.subTest(contents=contents):
                    executor = _OfflineRecoveryExecutor(
                        Path(td), completion_grace_period=0.03)
                    executor.work_directory.mkdir(parents=True, exist_ok=True)
                    native_id = token("native-")
                    job = _register_recovered_job(executor, native_id)
                    record = executor.work_directory / f"{native_id}.ec"
                    record.write_bytes(contents)
                    executor._queue_poll_thread._poll()

                    if expected_exit_code is not None:
                        expected_state = (JobState.COMPLETED if expected_exit_code == 0
                                          else JobState.FAILED)
                        self.assertEqual(job.status.state, expected_state)
                        self.assertEqual(job.status.exit_code, expected_exit_code)
                    else:
                        self.assertEqual(job.status.state, JobState.ACTIVE)
                        self.assertIsNone(job.status.exit_code)
                        time.sleep(0.05)
                        executor._queue_poll_thread._poll()
                        self.assertEqual(job.status.state, JobState.FAILED)
                        self.assertIsNone(job.status.exit_code)

    def test_scheduler_reappearance_resets_missing_timer(self) -> None:
        with tempfile.TemporaryDirectory(prefix="psij-reappeared-") as td:
            grace_period = 0.12
            executor = _OfflineRecoveryExecutor(
                Path(td), completion_grace_period=grace_period)
            executor.work_directory.mkdir(parents=True)
            native_id = token("native-")
            job = _register_recovered_job(executor, native_id)

            executor._queue_poll_thread._poll()
            self.assertEqual(job.status.state, JobState.ACTIVE)

            time.sleep(grace_period / 2)
            executor.status_map[native_id] = JobStatus(JobState.ACTIVE)
            executor._queue_poll_thread._poll()
            self.assertEqual(job.status.state, JobState.ACTIVE)

            time.sleep(grace_period * 0.75)
            executor.status_map.clear()
            executor._queue_poll_thread._poll()
            self.assertEqual(job.status.state, JobState.ACTIVE)
            self.assertIsNone(job.status.exit_code)

            time.sleep(grace_period * 1.25)
            executor._queue_poll_thread._poll()
            self.assertEqual(job.status.state, JobState.FAILED)
            self.assertIsNone(job.status.exit_code)

    def test_present_scheduler_terminal_state_is_not_delayed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="psij-present-terminal-") as td:
            executor = _OfflineRecoveryExecutor(Path(td))
            executor.work_directory.mkdir(parents=True)
            native_id = token("native-")
            job = _register_recovered_job(executor, native_id)
            executor.status_map[native_id] = JobStatus(JobState.COMPLETED)
            executor._queue_poll_thread._poll()
            self.assertEqual(job.status.state, JobState.COMPLETED)

    def test_invalid_completion_evidence_fails_after_grace_without_exit_code(self) -> None:
        cases: tuple[tuple[str, bytes | None, str], ...] = (
            ("missing", None, "missing"),
            ("partial", b"7", "invalid"),
            ("trailing-junk", b"0\nextra", "invalid"),
        )
        for label, contents, evidence_kind in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory(
                    prefix=f"psij-{label}-") as td:
                executor = _OfflineRecoveryExecutor(
                    Path(td), completion_grace_period=0.03)
                executor.work_directory.mkdir(parents=True)
                native_id = token("native-")
                job = _register_recovered_job(executor, native_id)
                if contents is not None:
                    (executor.work_directory / f"{native_id}.ec").write_bytes(contents)

                executor._queue_poll_thread._poll()
                self.assertEqual(job.status.state, JobState.ACTIVE)
                time.sleep(0.05)
                executor._queue_poll_thread._poll()
                self.assertEqual(job.status.state, JobState.FAILED)
                self.assertIsNone(job.status.exit_code)
                self.assertIsNotNone(job.status.message)
                assert job.status.message is not None
                self.assertIn(native_id, job.status.message)
                self.assertIn(evidence_kind, job.status.message)

    def test_inflight_poll_preserves_same_id_late_attachment(self) -> None:
        with tempfile.TemporaryDirectory(prefix="psij-poll-race-") as td:
            executor = _OfflineRecoveryExecutor(Path(td))
            executor.work_directory.mkdir(parents=True)
            native_id = token("native-")
            (executor.work_directory / f"{native_id}.ec").write_text("0\n", encoding="ascii")
            first = _register_recovered_job(executor, native_id)
            executor.poll_started = threading.Event()
            executor.poll_release = threading.Event()

            poll = threading.Thread(target=executor._queue_poll_thread._poll)
            poll.start()
            self.assertTrue(executor.poll_started.wait(1), "poll did not reach barrier")
            second = _register_recovered_job(executor, native_id)
            executor.poll_release.set()
            poll.join(2)
            self.assertFalse(poll.is_alive(), "barrier-controlled poll did not return")

            self.assertEqual(first.status.state, JobState.COMPLETED)
            self.assertEqual(first.status.exit_code, 0)
            self.assertEqual(second.status.state, JobState.ACTIVE)
            self.assertTrue((executor.work_directory / f"{native_id}.ec").is_file())
            self.assertEqual(executor._queue_poll_thread._jobs[native_id], [second])

            executor.poll_started = None
            executor.poll_release = None
            executor._queue_poll_thread._poll()
            self.assertEqual(second.status.state, JobState.COMPLETED)
            self.assertEqual(second.status.exit_code, 0)
            self.assertNotIn(native_id, executor._queue_poll_thread._jobs)
            self.assertFalse((executor.work_directory / f"{native_id}.ec").exists())

    def test_completion_grace_period_requires_positive_finite_number(self) -> None:
        for value in (0, -0.01, float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                BatchSchedulerExecutorConfig(completion_grace_period=value)
        for value in (True, False, "2", None):
            with self.subTest(value=value), self.assertRaises(TypeError):
                BatchSchedulerExecutorConfig(completion_grace_period=value)  # type: ignore[arg-type]
        config = BatchSchedulerExecutorConfig(completion_grace_period=0.125)
        self.assertEqual(config.completion_grace_period, 0.125)
        self.assertEqual(BatchSchedulerExecutorConfig().completion_grace_period, 2.0)


class SchedulerStatusParsingTests(unittest.TestCase):
    _SLURM_REASONS = {
        "AssociationJobLimit": "The job's association has reached its maximum job count.",
        "AssociationResourceLimit": "The job's association has reached some resource limit.",
        "AssociationTimeLimit": "The job's association has reached its time limit.",
        "BadConstraints": "The job's constraints can not be satisfied.",
        "BeginTime": "The job's earliest start time has not yet been reached.",
        "Cleaning": "The job is being requeued and still cleaning up from its previous execution.",
        "Dependency": "This job is waiting for a dependent job to complete.",
        "FrontEndDown": "No front end node is available to execute this job.",
        "InactiveLimit": "The job reached the system InactiveLimit.",
        "InvalidAccount": "The job's account is invalid.",
        "InvalidQOS": "The job's QOS is invalid.",
        "JobHeldAdmin": "The job is held by a system administrator.",
        "JobHeldUser": "The job is held by the user.",
        "JobLaunchFailure": (
            "The job could not be launched.This may be due to a file system problem, "
            "invalid program name, etc."
        ),
        "Licenses": "The job is waiting for a license.",
        "NodeDown": "A node required by the job is down.",
        "NonZeroExitCode": "The job terminated with a non-zero exit code.",
        "PartitionDown": "The partition required by this job is in a DOWN state.",
        "PartitionInactive": (
            "The partition required by this job is in an Inactive state and not able to "
            "start jobs."
        ),
        "PartitionNodeLimit": (
            "The number of nodes required by this job is outside of its partition's current "
            "limits. Can also indicate that required nodes are DOWN or DRAINED."
        ),
        "PartitionTimeLimit": (
            "The job's time limit exceeds its partition's current time limit."
        ),
        "Priority": (
            "One or more higher priority jobs exist for this partition or advanced reservation."
        ),
        "Prolog": "Its PrologSlurmctld program is still running.",
        "QOSJobLimit": "The job's QOS has reached its maximum job count.",
        "QOSResourceLimit": "The job's QOS has reached some resource limit.",
        "QOSTimeLimit": "The job's QOS has reached its time limit.",
        "ReqNodeNotAvail": (
            "Some node specifically required by the job is not currently available. The node "
            "may currently be in use, reserved for another job, in an advanced reservation, "
            "DOWN, DRAINED, or not responding. Nodes which are DOWN, DRAINED, or not responding "
            "will be identified as part of the job's \"reason\" field as \"UnavailableNodes\". "
            "Such nodes will typically require the intervention of a system administrator to "
            "make available."
        ),
        "Reservation": "The job is waiting its advanced reservation to become available.",
        "Resources": "The job is waiting for resources to become available.",
        "SystemFailure": "Failure of the Slurm system, a file system, the network, etc.",
        "TimeLimit": "The job exhausted its time limit.",
        "QOSUsageThreshold": "Required QOS threshold has been breached.",
        "WaitingForScheduling": (
            "No reason has been set for this job yet. Waiting for the scheduler to determine "
            "the appropriate reason."
        ),
    }

    def test_slurm_retains_every_established_failure_reason_mapping(self) -> None:
        executor = object.__new__(SlurmJobExecutor)
        rows = ["JOBID STATE REASON"]
        native_ids = []
        for index, reason in enumerate(self._SLURM_REASONS):
            native_id = f"slurm-reason-{index}-{secrets.token_hex(3)}"
            native_ids.append(native_id)
            rows.append(f"{native_id} F {reason}")

        statuses = executor.parse_status_output(0, "\n".join(rows) + "\n")
        self.assertEqual(len(statuses), len(self._SLURM_REASONS))
        for native_id, expected in zip(native_ids, self._SLURM_REASONS.values()):
            with self.subTest(native_id=native_id):
                self.assertEqual(statuses[native_id].state, JobState.FAILED)
                self.assertEqual(statuses[native_id].message, expected)

    def test_slurm_preserves_spaced_and_unknown_failure_reason(self) -> None:
        executor = object.__new__(SlurmJobExecutor)
        native_id = token("slurm-")
        reason = "Node telemetry unavailable near rack 17"
        output = f"JOBID STATE REASON\n{native_id} F {reason}\n"
        statuses = executor.parse_status_output(0, output)
        self.assertEqual(statuses[native_id].state, JobState.FAILED)
        self.assertEqual(statuses[native_id].message, reason)

    def test_slurm_rejects_unknown_states_and_malformed_structure(self) -> None:
        executor = object.__new__(SlurmJobExecutor)
        native_id = token("slurm-invalid-")
        cases = (
            ("", "Missing squeue status header"),
            (f"{native_id} R None\n", "Malformed squeue status header"),
            (f"JOBID STATE REASON\n{native_id} ZZ unknown state\n", "Unknown Slurm state"),
            (f"JOBID STATE REASON\n{native_id} R\n", "Malformed squeue status row"),
        )
        for output, message in cases:
            with self.subTest(output=output), self.assertRaisesRegex(ValueError, message):
                executor.parse_status_output(0, output)

    def test_pbs_accepts_missing_optional_comment(self) -> None:
        executor = object.__new__(PBSProJobExecutor)
        native_id = token("pbs-")
        output = json.dumps({"Jobs": {native_id: {"job_state": "R"}}})
        statuses = executor.parse_status_output(0, output)
        self.assertEqual(statuses[native_id].state, JobState.ACTIVE)
        self.assertIsNone(statuses[native_id].message)

    def test_pbs_exit_status_and_cancellation_mapping_is_complete(self) -> None:
        executor = object.__new__(PBSProJobExecutor)
        missing = object()
        for native_state in ("F", "X"):
            for exit_status, expected in (
                (missing, JobState.COMPLETED),
                (0, JobState.COMPLETED),
                (7, JobState.FAILED),
                (265, JobState.CANCELED),
            ):
                native_id = token("pbs-final-")
                report = {"job_state": native_state, "comment": native_id}
                if exit_status is not missing:
                    report["Exit_status"] = exit_status
                output = json.dumps({"Jobs": {native_id: report}})
                statuses = executor.parse_status_output(0, output)
                with self.subTest(native_state=native_state, exit_status=exit_status):
                    self.assertEqual(statuses[native_id].state, expected)
                    self.assertEqual(statuses[native_id].message, native_id)

        native_id = token("pbs-active-")
        output = json.dumps({
            "Jobs": {native_id: {"job_state": "R", "Exit_status": 265}}
        })
        self.assertEqual(executor.parse_status_output(0, output)[native_id].state,
                         JobState.ACTIVE)

    def test_lsf_uses_first_nonempty_real_reason_value(self) -> None:
        executor = object.__new__(LsfJobExecutor)
        native_id = token("lsf-")
        reason = token("preempted-by-")
        output = json.dumps({
            "RECORDS": [{
                "JOBID": native_id,
                "STAT": "EXIT",
                "EXIT_REASON": "",
                "KILL_REASON": reason,
            }]
        })
        statuses = executor.parse_status_output(0, output)
        self.assertEqual(statuses[native_id].state, JobState.FAILED)
        self.assertEqual(statuses[native_id].message, reason)

    def test_lsf_error_records_are_ignored_without_hiding_valid_records(self) -> None:
        executor = object.__new__(LsfJobExecutor)
        native_id = token("lsf-valid-")
        output = json.dumps({
            "RECORDS": [
                {"ERROR": token("lsf-error-")},
                {"ERROR": "not found", "JOBID": 7, "STAT": 7},
                {"JOBID": native_id, "STAT": "DONE"},
            ]
        })
        statuses = executor.parse_status_output(0, output)
        self.assertEqual(set(statuses), {native_id})
        self.assertEqual(statuses[native_id].state, JobState.COMPLETED)

    def test_status_parsers_reject_nonzero_command_exits(self) -> None:
        cases = (
            (object.__new__(SlurmJobExecutor), "squeue"),
            (object.__new__(PBSProJobExecutor), "qstat"),
            (object.__new__(LsfJobExecutor), "bjobs"),
        )
        for executor, command in cases:
            with self.subTest(command=command), self.assertRaisesRegex(
                    RuntimeError, command):
                executor.parse_status_output(23, token("scheduler-error-"))

    def test_json_status_parsers_reject_malformed_payloads(self) -> None:
        pbs = object.__new__(PBSProJobExecutor)
        lsf = object.__new__(LsfJobExecutor)
        pbs_cases = (
            "{truncated",
            json.dumps([]),
            json.dumps({}),
            json.dumps({"Jobs": []}),
            json.dumps({"Jobs": {"42.server": []}}),
            json.dumps({"Jobs": {"42.server": {}}}),
            json.dumps({"Jobs": {"42.server": {"job_state": "UNKNOWN"}}}),
            json.dumps({"Jobs": {"42.server": {"job_state": "F", "Exit_status": "7"}}}),
            json.dumps({"Jobs": {"42.server": {"job_state": "R", "Exit_status": None}}}),
            json.dumps({"Jobs": {"42.server": {"job_state": "R", "comment": 7}}}),
        )
        lsf_cases = (
            "{truncated",
            json.dumps([]),
            json.dumps({}),
            json.dumps({"RECORDS": {}}),
            json.dumps({"RECORDS": ["not-an-object"]}),
            json.dumps({"RECORDS": [{"STAT": "RUN"}]}),
            json.dumps({"RECORDS": [{"JOBID": "42"}]}),
            json.dumps({"RECORDS": [{"JOBID": "42", "STAT": 7}]}),
            json.dumps({"RECORDS": [{"JOBID": "42", "STAT": "UNKNOWN"}]}),
        )
        for scheduler, executor, payloads in (
            ("pbs", pbs, pbs_cases),
            ("lsf", lsf, lsf_cases),
        ):
            for payload in payloads:
                with self.subTest(scheduler=scheduler, payload=payload), \
                        self.assertRaises(ValueError):
                    executor.parse_status_output(0, payload)


def render_script(executor_type: Type[BatchSchedulerExecutor], config: object,
                  name: str, spec: JobSpec) -> str:
    setattr(executor_type, "_NAME_", name)
    executor = executor_type(url=None, config=config)  # type: ignore[call-arg]
    job = Job(spec)
    out = io.StringIO()
    executor.generate_submit_script(job, executor._create_script_context(job), out)
    return out.getvalue()


def render_all(spec: JobSpec) -> Dict[str, str]:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        return {
            "slurm": render_script(
                SlurmJobExecutor, SlurmExecutorConfig(work_directory=root), "slurm", spec),
            "pbs": render_script(
                PBSProJobExecutor, PBSProExecutorConfig(work_directory=root), "pbs", spec),
            "lsf": render_script(
                LsfJobExecutor, LsfExecutorConfig(work_directory=root), "lsf", spec),
        }


def directive_value(script: str, pattern: str) -> str:
    match = re.search(pattern, script, flags=re.MULTILINE)
    if match is None:
        raise AssertionError(f"missing directive matching {pattern!r}\n{script}")
    return match.group(1)


def parse_hms(value: str, allow_days: bool) -> int:
    days = 0
    clock = value
    if "-" in value:
        if not allow_days:
            raise AssertionError(f"days are not valid here: {value}")
        day_text, clock = value.split("-", 1)
        days = int(day_text)
    pieces = clock.split(":")
    if len(pieces) != 3:
        raise AssertionError(f"not an h:m:s duration: {value}")
    hours, minutes, seconds = (int(piece) for piece in pieces)
    if not 0 <= minutes <= 59 or not 0 <= seconds <= 59:
        raise AssertionError(f"invalid minute/second component: {value}")
    if days and not 0 <= hours <= 23:
        raise AssertionError(f"invalid day-relative hour component: {value}")
    return ((days * 24 + hours) * 60 + minutes) * 60 + seconds


class BatchRenderingTests(unittest.TestCase):
    def test_walltimes_preserve_duration_in_each_scheduler_dialect(self) -> None:
        durations = (
            timedelta(seconds=1),
            timedelta(seconds=59, microseconds=1),
            timedelta(hours=1, seconds=1),
            timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=1),
            timedelta(days=4, hours=23, minutes=59, seconds=59, microseconds=999999),
        )
        for duration in durations:
            with self.subTest(duration=duration):
                scripts = render_all(complete_spec(duration=duration, custom={}))
                expected_seconds = math.ceil(duration.total_seconds())

                slurm = directive_value(scripts["slurm"], r"^#SBATCH --time=([^\s]+)$")
                self.assertEqual(parse_hms(slurm, allow_days=True), expected_seconds)

                pbs = directive_value(scripts["pbs"], r"^#PBS -l walltime=([^\s]+)$")
                self.assertEqual(parse_hms(pbs, allow_days=False), expected_seconds)

                lsf = directive_value(scripts["lsf"], r"^#BSUB -W ([^\s]+)$")
                pieces = lsf.split(":")
                self.assertEqual(len(pieces), 2)
                hours, minutes = (int(piece) for piece in pieces)
                self.assertGreaterEqual(hours, 0)
                self.assertTrue(0 <= minutes <= 59)
                self.assertEqual(hours * 60 + minutes, math.ceil(expected_seconds / 60))

    def test_builtins_and_random_custom_attributes_render_without_cross_leakage(self) -> None:
        suffix = secrets.token_hex(5)
        keys = {
            "slurm": f"flag{secrets.token_hex(3)}",
            "pbs": f"flag{secrets.token_hex(3)}",
            "lsf": f"flag{secrets.token_hex(3)}",
        }
        values = {name: f"value-{name}-{suffix}" for name in keys}
        custom = {f"{name}.{keys[name]}": values[name] for name in keys}
        spec = complete_spec(custom=custom)
        scripts = render_all(spec)

        self.assertIn(f'#SBATCH --partition="{spec.attributes.queue_name}"', scripts["slurm"])
        self.assertIn(f'#SBATCH --account="{spec.attributes.project_name}"', scripts["slurm"])
        self.assertIn(f'#SBATCH --reservation="{spec.attributes.reservation_id}"', scripts["slurm"])
        self.assertIn(f'#SBATCH --{keys["slurm"]}="{values["slurm"]}"', scripts["slurm"])

        self.assertIn(f"#PBS -q {spec.attributes.queue_name}", scripts["pbs"])
        self.assertIn(f"#PBS -P {spec.attributes.project_name}", scripts["pbs"])
        self.assertIn(f"#PBS -q {spec.attributes.reservation_id}", scripts["pbs"])
        self.assertIn(f'#PBS -{keys["pbs"]} "{values["pbs"]}"', scripts["pbs"])

        self.assertIn(f'#BSUB -q "{spec.attributes.queue_name}"', scripts["lsf"])
        self.assertIn(f'#BSUB -G "{spec.attributes.project_name}"', scripts["lsf"])
        self.assertIn(f'#BSUB -P "{spec.attributes.project_name}"', scripts["lsf"])
        self.assertIn(f'#BSUB -U "{spec.attributes.reservation_id}"', scripts["lsf"])
        self.assertIn(f'#BSUB -{keys["lsf"]} "{values["lsf"]}"', scripts["lsf"])

        for scheduler, script in scripts.items():
            for other, value in values.items():
                if other != scheduler:
                    self.assertNotIn(value, script)

    def test_invalid_scheduler_custom_attribute_names_and_values_are_rejected(self) -> None:
        cases = (
            ({7: "value"}, TypeError),
            ({".qos": "value"}, ValueError),
            ({"slurm.": "value"}, ValueError),
            ({"slurm.9qos": "value"}, ValueError),
            ({"slurm.bad.key": "value"}, ValueError),
            ({"slurm.bad key": "value"}, ValueError),
            ({"slurm.qos": 7}, TypeError),
            ({"slurm.qos": ""}, ValueError),
            ({"slurm.qos": "line\nbreak"}, ValueError),
            ({"slurm.qos": "tab\tvalue"}, ValueError),
            ({"slurm.qos": 'bad"value'}, ValueError),
        )
        for custom, error_type in cases:
            with self.subTest(custom=custom), self.assertRaises(error_type):
                render_all(complete_spec(custom=custom))  # type: ignore[arg-type]

    def test_recovered_spec_drives_batch_script(self) -> None:
        duration = timedelta(days=1, hours=9, minutes=17, seconds=23)
        marker = secrets.token_hex(7)
        original = complete_spec(
            duration=duration,
            custom={"slurm.constraint": marker, "pbs.place": marker, "lsf.spool": marker},
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "recover.json"
            Export().export(original, str(path))
            restored = Import().load(str(path))
        assert isinstance(restored, JobSpec)
        scripts = render_all(restored)
        self.assertIn(f'#SBATCH --constraint="{marker}"', scripts["slurm"])
        self.assertIn(f'#PBS -place "{marker}"', scripts["pbs"])
        self.assertIn(f'#BSUB -spool "{marker}"', scripts["lsf"])
        self.assertEqual(
            parse_hms(directive_value(scripts["pbs"], r"^#PBS -l walltime=([^\s]+)$"), False),
            int(duration.total_seconds()),
        )


class WaitSemanticsTests(unittest.TestCase):
    def make_job(self, state: JobState, exit_code: int | None = None) -> Job:
        job = Job()
        job.status = JobStatus(state, exit_code=exit_code)
        return job

    def test_single_target_accepts_an_already_later_state(self) -> None:
        job = self.make_job(JobState.COMPLETED, 0)
        status = job.wait(timedelta(milliseconds=100), target_states=JobState.QUEUED)
        self.assertIsNotNone(status)
        assert status is not None
        self.assertEqual(status.state, JobState.COMPLETED)

    def test_sequence_target_accepts_an_already_later_state(self) -> None:
        job = self.make_job(JobState.ACTIVE)
        status = job.wait(timedelta(milliseconds=100), target_states=[JobState.QUEUED])
        self.assertIsNotNone(status)
        assert status is not None
        self.assertEqual(status.state, JobState.ACTIVE)

    def test_each_final_state_is_observable_for_an_earlier_target(self) -> None:
        for state, exit_code in (
            (JobState.COMPLETED, 0),
            (JobState.FAILED, 19),
            (JobState.CANCELED, None),
        ):
            with self.subTest(state=state):
                job = self.make_job(state, exit_code)
                status = job.wait(timedelta(milliseconds=100), target_states=[JobState.ACTIVE])
                self.assertIsNotNone(status)
                assert status is not None
                self.assertEqual(status.state, state)

    def test_real_timeout_still_returns_none(self) -> None:
        job = Job()
        start = time.monotonic()
        status = job.wait(timedelta(milliseconds=40), target_states=JobState.ACTIVE)
        elapsed = time.monotonic() - start
        self.assertIsNone(status)
        self.assertGreaterEqual(elapsed, 0.025)
        self.assertLess(elapsed, 0.5)

    def test_zero_timeout_is_nonblocking(self) -> None:
        job = Job()
        start = time.monotonic()
        status = job.wait(timedelta(0), target_states=JobState.ACTIVE)
        self.assertIsNone(status)
        self.assertLess(time.monotonic() - start, 0.1)


class LauncherClassificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.launcher = SingleLauncher()

    def test_exact_final_marker_with_lf_or_crlf_is_success(self) -> None:
        prefix = token("diagnostic-")
        for output in (
            "_PSI_J_LAUNCHER_DONE\n",
            f"{prefix}\n_PSI_J_LAUNCHER_DONE\n",
            f"{prefix}\r\n_PSI_J_LAUNCHER_DONE\r\n",
        ):
            with self.subTest(output=output):
                self.assertFalse(self.launcher.is_launcher_failure(output))

    def test_missing_partial_or_nonfinal_marker_is_failure(self) -> None:
        invalid = (
            "launcher failed\n",
            "_PSI_J_LAUNCHER_DON\n",
            "_PSI_J_LAUNCHER_DONE",
            "_PSI_J_LAUNCHER_DONE\nextra\n",
            "_PSI_J_LAUNCHER_DONE\n\n",
        )
        for output in invalid:
            with self.subTest(output=output):
                self.assertTrue(self.launcher.is_launcher_failure(output))

    def test_launcher_failure_message_keeps_diagnostics(self) -> None:
        diagnostic = token("wrapper-failed-")
        output = diagnostic + "\nsecond detail\n"
        self.assertTrue(self.launcher.is_launcher_failure(output))
        message = self.launcher.get_launcher_failure_message(output)
        self.assertIn(diagnostic, message)
        self.assertIn("second detail", message)

    def test_local_nonzero_program_is_not_mislabeled_as_launcher_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="psij-local-") as td:
            root = Path(td)
            spec = JobSpec(
                executable="/bin/sh",
                arguments=["-c", "echo application-error >&2; exit 7"],
                directory=root,
                stdout_path=root / "stdout.log",
                stderr_path=root / "stderr.log",
                launcher="single",
            )
            job = Job(spec)
            executor = JobExecutor.get_instance("local")
            executor.submit(job)
            status = job.wait(timeout=timedelta(seconds=5))
            self.assertIsNotNone(status)
            assert status is not None
            self.assertEqual(status.state, JobState.FAILED)
            self.assertEqual(status.exit_code, 7)
            self.assertIsNone(status.message)
            self.assertIn("application-error", (root / "stderr.log").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
