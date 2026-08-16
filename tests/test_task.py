#!/usr/bin/env python3
import io
import hashlib
import json
import math
import os
import random
import re
import secrets
import subprocess
import sys
import tempfile
import time
import unittest
import warnings
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, Tuple, Type


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
from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutor  # noqa: E402
from psij.executors.batch.lsf import LsfExecutorConfig, LsfJobExecutor  # noqa: E402
from psij.executors.batch.pbspro import PBSProExecutorConfig, PBSProJobExecutor  # noqa: E402
from psij.executors.batch.slurm import SlurmExecutorConfig, SlurmJobExecutor  # noqa: E402
from psij.launchers.single import SingleLauncher  # noqa: E402


def token(prefix: str) -> str:
    return prefix + secrets.token_hex(6)


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
        rng = random.SystemRandom()
        for _ in range(4):
            duration = timedelta(
                days=rng.randint(0, 4),
                hours=rng.randint(0, 23),
                minutes=rng.randint(0, 59),
                seconds=rng.randint(0, 59),
                microseconds=rng.randint(0, 999999),
            )
            if duration == timedelta(0):
                duration = timedelta(seconds=1)
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
