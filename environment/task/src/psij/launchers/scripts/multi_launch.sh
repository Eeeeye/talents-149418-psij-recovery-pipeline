#!/bin/bash

source $(dirname "$0")/launcher_lib.sh

pre_launch

PIDS=()
_PSI_J_PROCESS_COUNT="$1"
shift
export _PSI_J_PROCESS_COUNT

_PSI_J_FAILED_EC=0
_PSI_J_CHILD_TIMEOUT_SECONDS="${PSIJ_MULTI_LAUNCH_TIMEOUT_SECONDS:-1800}"

if ! [[ "$_PSI_J_CHILD_TIMEOUT_SECONDS" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || [[ "$_PSI_J_CHILD_TIMEOUT_SECONDS" =~ ^0+([.]0+)?$ ]]; then
    log "PSIJ_MULTI_LAUNCH_TIMEOUT_SECONDS must be a positive number"
    exit 2
fi

cleanup_children() {
    local PID ATTEMPT ALIVE

    set +e
    for PID in "${PIDS[@]}"; do
        # GNU timeout creates a process group for the command. Signal both the
        # group and the wrapper PID so cleanup is safe even during startup.
        kill -TERM -- "-$PID" 2>/dev/null
        kill -TERM "$PID" 2>/dev/null
    done

    for ATTEMPT in $(seq 1 1 20); do
        ALIVE=0
        for PID in "${PIDS[@]}"; do
            if kill -0 "$PID" 2>/dev/null || kill -0 -- "-$PID" 2>/dev/null; then
                ALIVE=1
            fi
        done
        if [ "$ALIVE" = "0" ]; then
            break
        fi
        sleep 0.1
    done

    for PID in "${PIDS[@]}"; do
        kill -KILL -- "-$PID" 2>/dev/null
        kill -KILL "$PID" 2>/dev/null
        wait "$PID" 2>/dev/null
    done
    set -e
}

on_signal() {
    local EXIT_CODE="$1"
    trap - HUP INT TERM EXIT
    cleanup_children
    exit "$EXIT_CODE"
}

trap cleanup_children EXIT
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

for INDEX in $(seq 1 1 "$_PSI_J_PROCESS_COUNT"); do
    _PSI_J_PROCESS_INDEX_="$INDEX" \
        timeout --signal=TERM --kill-after=2s \
        "${_PSI_J_CHILD_TIMEOUT_SECONDS}s" \
        "$@" 1>>"$_PSI_J_STDOUT" 2>>"$_PSI_J_STDERR" <"$_PSI_J_STDIN" &
    PIDS+=("$!")
done

for PID in "${PIDS[@]}"; do
    set +e
    wait "$PID"
    _PSI_J_EC=$?
    set -e
    if [ "$_PSI_J_EC" != "0" ]; then
        log "Pid $PID failed with $_PSI_J_EC"
        _PSI_J_FAILED_EC=$_PSI_J_EC
    fi
done

PIDS=()
trap - HUP INT TERM EXIT

log "All completed"

post_launch

echo "_PSI_J_LAUNCHER_DONE"
exit "$_PSI_J_FAILED_EC"
