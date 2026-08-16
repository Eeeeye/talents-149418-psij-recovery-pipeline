import json
import math
import os
import re
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from psij.job_attributes import JobAttributes
from psij.job_spec import JobSpec
from psij.resource_spec import ResourceSpecV1


_DURATION_RE = re.compile(
    r'^(?:(?P<days>-?\d+) day(?:s)?, )?'
    r'(?P<hours>\d+):(?P<minutes>\d{2}):'
    r'(?P<seconds>\d{2}(?:\.\d{1,6})?)$'
)


def _duration_from_text(value: object) -> timedelta:
    if not isinstance(value, str):
        raise TypeError('Job duration must be a timedelta string')
    match = _DURATION_RE.fullmatch(value)
    if match is None:
        raise ValueError('Invalid timedelta value: %r' % value)
    days = int(match.group('days') or 0)
    hours = int(match.group('hours'))
    minutes = int(match.group('minutes'))
    seconds = float(match.group('seconds'))
    if hours > 23 or minutes > 59 or seconds >= 60:
        raise ValueError('Invalid timedelta value: %r' % value)
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def _optional_path(value: object, field: str) -> Optional[Path]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError('%s must be a string or null' % field)
    return Path(value)


def _reject_json_constant(value: str) -> object:
    raise ValueError('Invalid JSON numeric constant: ' + value)


def _validate_json_value(value: object, field: str = 'value') -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError('%s contains a non-finite number' % field)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, '%s[%d]' % (field, index))
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError('%s contains a non-string object key' % field)
            _validate_json_value(item, '%s.%s' % (field, key))
        return
    raise TypeError('%s contains unsupported type %s' % (field, type(value).__name__))


class Export(object):
    """Compatibility exporter for PSI/J objects."""

    def __init__(self) -> None:
        self.version = ''
        self.name = ''

    def envelope(self, type: Optional[str] = None) -> Dict[str, Any]:
        return {'version': 0.1, 'type': type, 'data': None}

    def to_dict(self, obj: object) -> Dict[str, Any]:
        if not isinstance(obj, JobSpec):
            raise TypeError("Can't create dict, type " + type(obj).__name__ + " not supported")
        return obj.to_dict

    def export(self, obj: object, dest: str) -> bool:
        source_type = type(obj).__name__
        envelope = self.envelope(type=source_type)
        envelope['data'] = self.to_dict(obj)
        _validate_json_value(envelope)

        # Encode before opening any destination, so invalid custom attributes
        # cannot truncate a previously committed recovery manifest.
        payload = json.dumps(envelope, ensure_ascii=False, indent=4, allow_nan=False)
        destination = Path(dest)
        temp_name: Optional[str] = None
        fd: Optional[int] = None
        try:
            fd, temp_name = tempfile.mkstemp(
                prefix='.' + destination.name + '.', suffix='.tmp', dir=str(destination.parent)
            )
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                fd = None
                stream.write(payload)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, destination)
            temp_name = None
        finally:
            if fd is not None:
                os.close(fd)
            if temp_name is not None:
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass
        return True


class Import(object):
    """Compatibility importer for PSI/J objects."""

    def _resource_from_dict(self, value: object) -> Optional[ResourceSpecV1]:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise TypeError('resources must be an object or null')
        version = value.get('version', 1)
        if version != 1:
            raise ValueError('Unsupported ResourceSpec version: %r' % version)
        old_ppn = value.get('process_per_node')
        new_ppn = value.get('processes_per_node')
        if old_ppn is not None and new_ppn is not None and old_ppn != new_ppn:
            raise ValueError('Conflicting processes-per-node fields')
        ppn = new_ppn if new_ppn is not None else old_ppn
        try:
            return ResourceSpecV1(
                node_count=value.get('node_count'),
                process_count=value.get('process_count'),
                processes_per_node=ppn,
                cpu_cores_per_process=value.get('cpu_cores_per_process'),
                gpu_cores_per_process=value.get('gpu_cores_per_process'),
                exclusive_node_use=value.get('exclusive_node_use', True),
            )
        except (AssertionError, TypeError) as error:
            raise ValueError('Invalid ResourceSpecV1') from error

    def _attributes_from_dict(self, value: object) -> Optional[JobAttributes]:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise TypeError('attributes must be an object or null')
        duration_value = value.get('duration', str(timedelta(minutes=10)))
        duration = _duration_from_text(duration_value)
        custom = value.get('custom_attributes', {})
        if custom is not None and not isinstance(custom, dict):
            raise TypeError('custom_attributes must be an object or null')
        return JobAttributes(
            duration=duration,
            queue_name=value.get('queue_name'),
            project_name=value.get('project_name'),
            reservation_id=value.get('reservation_id'),
            custom_attributes=custom,
        )

    def _dict2spec(self, value: Dict[str, Any]) -> JobSpec:
        if not isinstance(value, dict):
            raise TypeError('JobSpec data must be an object')
        name = value['name'] if 'name' in value else value.get('_name')
        arguments = value.get('arguments')
        if arguments is not None and not isinstance(arguments, list):
            raise TypeError('arguments must be a list or null')
        environment = value.get('environment')
        if environment is not None:
            if not isinstance(environment, dict):
                raise TypeError('environment must be an object or null')
            if any(not isinstance(k, str) or not isinstance(v, str)
                   for k, v in environment.items()):
                raise TypeError('environment keys and values must be strings')
        inherit_environment = value.get('inherit_environment', True)
        if not isinstance(inherit_environment, bool):
            raise TypeError('inherit_environment must be a boolean')
        return JobSpec(
            name=name,
            executable=value.get('executable'),
            arguments=arguments,
            directory=_optional_path(value.get('directory'), 'directory'),
            inherit_environment=inherit_environment,
            environment=environment,
            stdin_path=_optional_path(value.get('stdin_path'), 'stdin_path'),
            stdout_path=_optional_path(value.get('stdout_path'), 'stdout_path'),
            stderr_path=_optional_path(value.get('stderr_path'), 'stderr_path'),
            resources=self._resource_from_dict(value.get('resources')),
            attributes=self._attributes_from_dict(value.get('attributes')),
            pre_launch=_optional_path(value.get('pre_launch'), 'pre_launch'),
            post_launch=_optional_path(value.get('post_launch'), 'post_launch'),
            launcher=value.get('launcher'),
        )

    def from_dict(self, value: Dict[str, Any], target_type: str) -> object:
        if target_type != 'JobSpec':
            raise TypeError("Can't create object, type " + str(target_type) + " not supported")
        return self._dict2spec(value)

    def load(self, src: str) -> object:
        with open(src, 'r', encoding='utf-8') as stream:
            envelope = json.load(stream, parse_constant=_reject_json_constant)
        if not isinstance(envelope, dict):
            raise TypeError('Serialization envelope must be an object')
        version = envelope.get('version')
        if isinstance(version, bool) or version != 0.1:
            raise ValueError('Unsupported serialization version: %r' % version)
        target_type = envelope.get('type')
        if target_type != 'JobSpec':
            raise TypeError("Can't create object, type " + str(target_type) + " not supported")
        data = envelope.get('data')
        if not isinstance(data, dict):
            raise TypeError('JobSpec data must be an object')
        return self.from_dict(data, target_type=target_type)
