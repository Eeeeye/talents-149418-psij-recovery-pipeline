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

Keep existing persistence callers compatible: a successful export returns
true, and a successful load returns the reconstructed specification.

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

An unset `name` remains unset across the restart even when `executable` is
present. Preserve the existing fallback relationship: if the executable is
changed after loading, the effective `name` must follow the new executable
rather than remaining frozen to the pre-restart executable.

The JSON manifest is self-contained: it must remain loadable by a fresh Python
process after the exporting controller exits, without process memory, a
sidecar file, or regeneration from unrelated inputs.

Values outside JSON's null/boolean/number/string/list/object data model must
cause export to fail with `TypeError` or `ValueError`. A JSON number is finite:
`NaN`, positive infinity, and negative infinity are invalid anywhere in a
custom-attribute value, including inside a nested list or object. Reject those
values during both export and import rather than accepting Python's
non-standard JSON extensions.

The on-disk document remains a JSON envelope with required `version`, `type`,
and `data` members. Their JSON kinds are exact: `version` is a number (a JSON
boolean is not a number here), `type` is a string, and `data` is an object.
Omitting any of those three members is invalid; `true` and `false` must be
rejected as envelope versions, and `null` must be rejected as envelope data.
The existing loader must continue to read version `0.1`, type `JobSpec`
manifests such as `examples/legacy-job-v0.1.json`.

Within `data`, `arguments` is an array or null; `inherit_environment` is a
boolean; `environment` is an object with string keys and values or null; path
members are strings or null; and `resources` and `attributes` are objects or
null. If present, `resources.version` is a JSON number equal to `1`; a JSON
boolean is not a valid resource version. `attributes.custom_attributes` is an
object or null, and its null value must survive export and import as null. A
legacy resource object may use `process_per_node` to mean
`processes_per_node`; if both names are present with different values, the
manifest is invalid. A legacy duration
is the canonical text produced by `str(datetime.timedelta(...))`, with hour,
minute, and second components in `00..23`, `00..59`, and `00..59`; it must be
restored as a `timedelta`. This includes canonical negative timedeltas, whose
normalized text uses a signed day component such as
`-1 day, 23:59:59.999999`. Fields absent from an old manifest default to the
same value as a newly constructed `JobSpec`.

Reject malformed JSON, unsupported envelope versions or types, non-object
`data`, unknown resource-spec versions, and malformed durations with a clear
`ValueError` or `TypeError`; do not return a partially restored `JobSpec`.
Export validation and serialization complete before publication. If either
fails, a destination that did not exist must remain absent, and an already
existing destination must remain byte-for-byte unchanged; do not leave a
partial manifest at the destination path.

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

Directive verification uses each scheduler's native option/value semantics.
Where the scheduler syntax permits both forms, `--key=value` and
`--key value`, quoted and unquoted equivalent values, and directive ordering
are not distinct behaviors; the sample spelling and ordering are not fixed.

Built-in attributes use these native mappings:

| Scheduler | `queue_name` | `project_name` | `reservation_id` |
| --- | --- | --- | --- |
| Slurm | `--partition` | `--account` | `--reservation` |
| PBS Pro | `-q` | `-P` | `-q` (PBS reservations are reservation queues) |
| LSF | `-q` | both `-G` and `-P` | `-U` |

PBS Pro exposes one `-q` queue selector. The `queue_name` and
`reservation_id` mappings are verified independently: when a specification
sets only one of them, that value must be the single `-q` value. This contract
does not prescribe precedence or duplicate-directive behavior for a
specification that sets both fields at once.

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
script. Any custom attribute containing a dot is scheduler-qualified. Its
complete name must match
`[A-Za-z][A-Za-z0-9_-]*\.[A-Za-z][A-Za-z0-9_-]*`, and its value must be a
non-empty Unicode string for which Python's `str.isprintable()` is true, and
it must not contain a double-quote character. In particular, a tab and every
other control character (including CR and LF) are invalid. Reject an invalid
qualified name or value with `ValueError` or `TypeError` before emitting a
submit script. A valid attribute for another scheduler is ignored, not leaked
into the current scheduler's script.

### 3. Monotonic state waiting

When a caller waits for a target state with an optional timeout, the existing
public API must accept either one `JobState` or a sequence. It returns as soon
as the current state is a requested state, is strictly later than a requested
state in PSI/J's existing partial order, or is any final state. This includes
jobs that advance from `NEW` to a final state before the caller starts waiting.
A real timeout still returns `None`. A zero-duration timeout is a non-blocking
poll and returns `None` if no target has been reached.
Do not change the state transition order or callback behavior.

### 4. Launcher/user-error classification

For script-based launchers, `_PSI_J_LAUNCHER_DONE` is a successful launcher
completion marker only when it is the exact final logical line and is followed
by a normal LF or CRLF line ending. Earlier diagnostic lines are allowed.
Trailing non-empty output, a partial marker, or no marker is a launcher
failure. Launcher failure messages must retain the useful diagnostic text and
must not report the valid completion marker itself as an error.

A same-line prefix before `_PSI_J_LAUNCHER_DONE` means that logical line is not
the exact marker. Likewise, the complete marker text without a terminating LF
or CRLF is not valid completion evidence. Both cases remain launcher failures,
and their malformed, unterminated, or prefixed line must remain in the derived
failure message rather than being silently removed.

When a local job executable exits non-zero after a valid launcher completion,
the job remains `FAILED` with its real exit code, but it must not acquire a
false launcher-failure message.

### 5. Fork-safe local execution

A controller may initialize a local executor before creating workers with the
POSIX `fork` start method. Two independent fork children must each obtain a
local executor, submit `/bin/true`, observe `COMPLETED` with exit code zero,
and exit within five seconds. Copied parent-process runtime state must not make
either child hang or lose completion.

### 6. Recovered batch-job concurrency and lifetime

An idle batch executor must be collectable after its last strong application
reference is deleted. Its daemon queue-poll thread must not strongly retain
the executor and must terminate promptly when the executor is collected.

Polling takes a snapshot of the jobs registered for each native scheduler ID.
If another `Job` attaches to the same native ID while that poll is in flight,
a terminal result may remove only the `Job` objects in that snapshot. The late
attachment and its completion files must remain available for a later poll;
every attached job must receive its own monotonic terminal transition.
When a later poll has finalized the last remaining attachment for that native
ID, remove its completion record and per-ID tracking. Never perform that
cleanup while any late attachment still needs the evidence.

Completion-file cleanup and same-ID registration must have a consistent
retirement boundary. Registration may wait for an already-started cleanup to
finish, but a registration that completes before file deletion must not lose
its evidence to that cleanup. This includes the interval after the old
snapshot's last job has been removed from tracking and before its completion
files are deleted.

### 7. Delayed and malformed completion evidence

The existing batch-executor configuration exposes a completion grace period in
seconds, defaulting to `2.0`. The value must be a positive, finite `int` or
`float`; booleans and non-numeric values are invalid. No coercion is performed:
strings, `None`, containers, and arbitrary objects raise `TypeError`. Existing
constructor calls remain compatible.

When a native ID disappears from scheduler status output, do not infer
success. Read `<work_directory>/<executor-name>/<native_id>.ec` without
consuming it. A valid record consists exactly of an ASCII signed decimal
integer followed by LF or CRLF (`^-?[0-9]+\r?\n$`). Exit code zero means
`COMPLETED`; a non-zero value means `FAILED` and the real exit code is
preserved. The optional sign is minus only: a leading plus sign, surrounding
whitespace, a missing line terminator, additional lines, or non-ASCII bytes
makes the record invalid.

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
  verbatim as diagnostic text. Status output requires the three-column
  `JOBID STATE REASON` header; unknown states, a missing or malformed header,
  and structurally malformed rows raise `ValueError`.
- PBS Pro JSON may omit the optional `comment` member; its message is then
  `None`. The top-level document and `Jobs` member are objects; each job entry
  is an object with a string `job_state`, while optional `Exit_status` is an
  integer and optional `comment` is a string or null. For native states that
  map to `COMPLETED`, an absent or zero `Exit_status` remains `COMPLETED`, value
  `265` maps to `CANCELED`, and every other non-zero value maps to `FAILED`.
  An `Exit_status` on a non-final native state does not make it final.
- LSF JSON may omit reason members. Select the value of the first non-empty
  member in `EXIT_REASON`, `KILL_REASON`, `SUSPEND_REASON` order rather than a
  field name or nonexistent key. The top-level document is an object whose
  `RECORDS` member is a list; non-error records contain string `JOBID` and
  `STAT` members. A record containing an `ERROR` member is ignored and does
  not create a status entry.

Diagnostic wording is not a fixed literal contract. A Slurm validation
`ValueError` must identify the scheduler as either Slurm or `squeue` and make
clear whether the defect concerns the status header, a status row, or an
unknown state, as applicable. Equivalent phrases such as "Missing Slurm
status header" and "Missing squeue status header" are both valid.

All three status parsers reject a non-zero scheduler-command exit before
parsing its payload and raise `RuntimeError` whose message names the failed
command (`squeue`, `qstat`, or `bjobs`). PBS Pro and LSF reject malformed JSON,
wrong container types, missing required members, and unknown native states
with `ValueError`.

## Preserved behavior

- Do not invoke real scheduler commands and do not require scheduler daemons.
- Do not change resource constraint validation, environment inheritance,
  callback transition reconstruction, or the meaning of final job states.
- Keep queue polling daemonized and scheduler-free verification deterministic;
  do not invoke real status commands from the probes or tests.
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
