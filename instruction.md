# Restore the PSI/J recovery and submission pipeline

## Background

This repository contains ExaWorks PSI/J Python 0.9.0, a portable job-submission
library used by HPC workflow controllers. A controller persists `JobSpec`
objects before a maintenance restart, restores them afterward, renders a
submit script for the destination scheduler, and waits for the recovered job
to reach an observable state. The same controller uses the local executor in
development and distinguishes a user program's non-zero exit from a launcher
failure.

The starter reconstructs defects from the public upstream release and its
subsequent fixes. No Slurm, PBS Pro, or LSF daemon is required: scheduler
scripts are rendered offline, while process execution uses PSI/J's local
executor.

## Initial failure

Run:

```bash
cd /workspace
./scripts/reproduce.sh
```

The five independent probes currently fail. Depending on which probe runs,
you will see a serialization exception or recovered fields with the wrong
types, missing/invalid scheduler directives, an
`UnreachableStateException` for a job that has already advanced past the
requested state, and a launcher completion marker classified as a launcher
failure. The recovery probe also shows a fork child whose local job cannot be
reaped and a recovered batch job that is finalized before its exit evidence
is safely published.

The repository imports successfully and the basic health check already
passes. This is a behavioral repair task, not a dependency-installation task.

## Required final behavior

All behavior below is part of the contract. Repairing only the public probe
values is insufficient; verification uses additional specifications,
durations, paths, attributes, state sequences, and launcher output.

### 1. Restart-safe `JobSpec` persistence

Keep the public compatibility API:

```python
Export().export(spec, path)  # returns True on success
restored = Import().load(path)
```

For newly exported manifests, a round trip must preserve these values and
their runtime types:

- `name`, `executable`, `arguments`, `inherit_environment`, and `environment`;
- `directory`, `stdin_path`, `stdout_path`, `stderr_path`, `pre_launch`, and
  `post_launch` as `pathlib.Path` or `None` after loading;
- `launcher`;
- a `ResourceSpecV1`, including `node_count`, `process_count`,
  `processes_per_node`, `cpu_cores_per_process`,
  `gpu_cores_per_process`, and `exclusive_node_use`;
- `JobAttributes`, including a `datetime.timedelta` duration, `queue_name`,
  `project_name`, `reservation_id`, and JSON-compatible custom-attribute
  values without converting booleans, integers, lists, dictionaries, or
  `None` to strings.

The JSON manifest is self-contained: it must remain loadable by a fresh Python
process after the exporting controller exits, without process memory, a
sidecar file, or regeneration from unrelated inputs.

Values outside JSON's null/boolean/number/string/list/object data model must
cause export to fail with `TypeError` or `ValueError`.

The on-disk document remains a JSON envelope with numeric `version`, string
`type`, and object `data` members. `Import.load()` must continue to read
version `0.1`, type `JobSpec` manifests such as
`examples/legacy-job-v0.1.json`. In a legacy resource object, the historical
key `process_per_node` means `processes_per_node`. A legacy duration is the
canonical text produced by `str(datetime.timedelta(...))`; it must be restored
as a `timedelta`. Fields absent from an old manifest default to the same value
as a newly constructed `JobSpec`.

Reject malformed JSON, unsupported envelope versions or types, non-object
`data`, unknown resource-spec versions, and malformed durations with a clear
`ValueError` or `TypeError`; do not return a partially restored `JobSpec`.
If export-time validation or JSON serialization fails, an already existing
destination file must remain byte-for-byte unchanged.

### 2. Scheduler-native batch directives after recovery

Rendering a recovered spec must produce scheduler-native walltimes. Preserve
the full requested duration, including durations longer than 24 hours:

- Slurm: `[days-]hours:minutes:seconds` (for example `1-02:03:04`);
- PBS Pro: total-hours `hours:minutes:seconds` (for example `26:03:04`);
- LSF: total-hours `hours:minutes`, rounding up when non-zero seconds cannot be
  represented (for example `26:04` for 1 day, 2:03:04).

If a duration contains fractional seconds, Slurm and PBS Pro round up to the
next representable whole second; LSF rounds up to the next whole minute.

Hours, minutes, and seconds must be integers; minute and second components
must be within `00..59`. The resulting directives must be syntactically valid
for the named scheduler.

Built-in attributes use these native mappings:

| Scheduler | `queue_name` | `project_name` | `reservation_id` |
| --- | --- | --- | --- |
| Slurm | `--partition` | `--account` | `--reservation` |
| PBS Pro | `-q` | `-P` | `-q` (PBS reservations are reservation queues) |
| LSF | `-q` | both `-G` and `-P` | `-U` |

Scheduler-qualified custom attributes have the form `<scheduler>.<key>`.
Render only the attributes belonging to the current scheduler, using its
native directive form:

```text
slurm.qos=normal    -> #SBATCH --qos="normal"
pbs.place=scatter   -> #PBS -place "scatter"
lsf.spool=y         -> #BSUB -spool "y"
```

Custom keys and values vary during verification. Do not special-case the
examples, and do not leak one scheduler's attributes into another scheduler's
script. Scheduler-qualified keys match `[A-Za-z][A-Za-z0-9_-]*`; their values
are non-empty printable strings without CR, LF, or double-quote characters.

### 3. Monotonic state waiting

`Job.wait(timeout=..., target_states=...)` must accept either one `JobState`
or a sequence. It returns as soon as the current state is a requested state,
is strictly later than a requested state in PSI/J's existing partial order, or
is any final state. This includes jobs that advance from `NEW` to a final
state before the caller starts waiting. A real timeout still returns `None`.
`timedelta(0)` is a non-blocking poll and returns `None` if no target has been
reached.
Do not change the state transition order or callback behavior.

### 4. Launcher/user-error classification

For script-based launchers, `_PSI_J_LAUNCHER_DONE` is a successful launcher
completion marker only when it is the exact final logical line and is followed
by a normal LF or CRLF line ending. Earlier diagnostic lines are allowed.
Trailing non-empty output, a partial marker, or no marker is a launcher
failure. Launcher failure messages must retain the useful diagnostic text and
must not report the valid completion marker itself as an error.

When a local job executable exits non-zero after a valid launcher completion,
the job remains `FAILED` with its real exit code, but it must not acquire a
false launcher-failure message.

### 5. Fork-safe local execution

A controller may initialize a local executor before creating workers with the
POSIX `fork` start method. Each child that subsequently creates a local
executor and submits a job must use a live process-reaper thread created for
that child, rather than the copied and no-longer-running parent thread. Two
independent fork children must each complete `/bin/true` within five seconds.
Do not globally disable reaping or make the reaper non-daemon.

### 6. Recovered batch-job concurrency and lifetime

An idle batch executor must be collectable after its last strong application
reference is deleted. Its daemon queue-poll thread must not strongly retain
the executor and must terminate promptly when the executor is collected.

Polling takes a snapshot of the jobs registered for each native scheduler ID.
If another `Job` attaches to the same native ID while that poll is in flight,
a terminal result may remove only the `Job` objects in that snapshot. The late
attachment and its completion files must remain available for a later poll;
every attached job must receive its own monotonic terminal transition.

### 7. Delayed and malformed completion evidence

`BatchSchedulerExecutorConfig` accepts a
`completion_grace_period` in seconds, defaulting to `2.0`. The value must be a
positive, finite `int` or `float`; booleans and non-numeric values are invalid.
Existing constructor calls remain compatible.

When a native ID disappears from scheduler status output, do not infer
success. Read `<work_directory>/<executor-name>/<native_id>.ec` without
consuming it. A valid record consists exactly of an ASCII signed decimal
integer followed by LF or CRLF (`^-?[0-9]+\r?\n$`). Exit code zero means
`COMPLETED`; a non-zero value means `FAILED` and the real exit code is
preserved.

Missing, empty, partial, undecodable, or otherwise malformed evidence keeps
the job's previous non-final state during its native-ID-specific grace period.
If valid evidence appears during that period, finalize normally. If the grace
period expires first, finalize as `FAILED`, leave `exit_code` as `None`, and
include the native ID plus whether evidence was missing or invalid in the
diagnostic. A non-final scheduler row that reappears clears the pending timer.
A scheduler row that is still present retains its reported PSI/J status.

### 8. Scheduler status diagnostics

- Slurm status rows may contain reason text with spaces. Known failure reasons
  retain their established mapping, while an unknown reason is preserved
  verbatim as diagnostic text. Unknown states and structurally malformed rows
  still raise a clear exception.
- PBS Pro JSON may omit the optional `comment` member; its message is then
  `None`.
- LSF JSON may omit reason members. Select the value of the first non-empty
  member in `EXIT_REASON`, `KILL_REASON`, `SUSPEND_REASON` order rather than a
  field name or nonexistent key.

## Preserved behavior

- Keep the PSI/J 0.9.0 public class names and `Export`/`Import` call signatures.
- Do not invoke real scheduler commands and do not require scheduler daemons.
- Do not change resource constraint validation, environment inheritance,
  callback transition reconstruction, or the meaning of final job states.
- Keep queue polling daemonized and scheduler-free verification deterministic;
  do not invoke real status commands from the probes or tests.
- Keep the built-in multiple launcher's concurrent worker behavior. Each
  worker must remain bounded by the positive-seconds
  `PSIJ_MULTI_LAUNCH_TIMEOUT_SECONDS` override (default `1800`), and a worker
  timeout or launcher interruption must not leave worker descendants running.
- Do not replace PSI/J with a separate implementation or hard-code the probe
  fixtures.

## Allowed changes and environment limits

You may modify files only under `/workspace/src/psij/`. Do not modify the
reproduction scripts, examples, packaging metadata, dependency locks, or
runtime environment. Do not install or download dependencies. The supplied
environment is complete, and the task does not require an external service or
download even when internet access is available.

The final checks are:

```bash
python3 scripts/healthcheck.py
./scripts/reproduce.sh
```

Both commands must exit zero, and repeated runs must remain successful.
