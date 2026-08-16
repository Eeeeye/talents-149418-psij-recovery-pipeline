from pathlib import Path
from typing import Optional, List, Dict, Any

from typeguard import check_argument_types

from psij.job_attributes import JobAttributes
from psij.resource_spec import ResourceSpec

class JobSpec(object):
    """A class to hold information about the characteristics of a:class:`~psij.Job`."""

    def __init__(self, name: Optional[str] = None, executable: Optional[str] = None,
                 arguments: Optional[List[str]] = None, directory: Optional[Path] = None,
                 inherit_environment: bool = True, environment: Optional[Dict[str, str]] = None,
                 stdin_path: Optional[Path] = None, stdout_path: Optional[Path] = None,
                 stderr_path: Optional[Path] = None, resources: Optional[ResourceSpec] = None,
                 attributes: Optional[JobAttributes] = None, pre_launch: Optional[Path] = None,
                 post_launch: Optional[Path] = None, launcher: Optional[str] = None):
        """
        Constructs a `JobSpec` object while allowing its properties to be initialized.

        .. note::
            A note about paths.

            It is strongly recommended that paths to `std*_path`, `directory`, etc. be specified
            as absolute. While paths can be relative, and there are cases when it is desirable to
            specify them as relative, it is important to understand what the implications are.

            Paths in a specification refer to paths *that are accessible to the machine where the
            job is running*. In most cases, that will be different from the machine on which the
            job is launched (i.e., where PSI/J is invoked from). This means that a given path may
            or may not point to the same file in both the location where the job is running and the
            location where the job is launched from.

            For example, if launching jobs from a login node of a cluster, the path `/tmp/foo.txt`
            will likely refer to locally mounted drives on both the login node and the compute
            node(s) where the job is running. However, since they are local mounts, the file
            `/tmp/foo.txt` written by a job running on the compute node will not be visible by
            opening `/tmp/foo.txt` on the login node. If an output file written on a compute node
            needs to be accessed on a login node, that file should be placed on a shared filesystem.
            However, even by doing so, there is no guarantee that the shared filesystem is mounted
            under the same mount point on both login and compute nodes. While this is an unlikely
            scenario, it remains a possibility.

            When relative paths are specified, even when they point to files on a shared filesystem
            as seen from the submission side (i.e., login node), the job working directory may be
            different from the working directory of the application that is launching the job. For
            example, an application that uses PSI/J to launch jobs on a cluster may be invoked from
            (and have its working directory set to) `/home/foo`, where `/home` is a mount point for
            a shared filesystem accessible by compute nodes. The launched job may specify
            `stdout_path=Path('bar.txt')`, which would resolve to `/home/foo/bar.txt`. However, the
            job may start in `/tmp` on the compute node, and its standard output will be redirected
            to `/tmp/bar.txt`.

            Relative paths are useful when there is a need to refer to the job directory that the
            scheduler chooses for the job, which is not generally known until the job is started by
            the scheduler. In such a case, one must leave the `spec.directory` attribute empty and
            refer to files inside the job directory using relative paths.


        :param name: A name for the job. The name plays no functional role except that
            :class:`~psij.JobExecutor` implementations may attempt to use the name to label the
            job as presented by the underlying implementation.
        :param executable: An executable, such as "/bin/date".
        :param arguments: The argument list to be passed to the executable. Unlike with execve(),
            the first element of the list will correspond to `argv[1]` when accessed by the invoked
            executable.
        :param directory: The directory, on the compute side, in which the executable is to be run
        :param inherit_environment: If this flag is set to `False`, the job starts with an empty
            environment. The only environment variables that will be accessible to the job are the
            ones specified by this property. If this flag is set to `True`, which is the default,
            the job will also have access to variables inherited from the environment in which the
            job is run.
        :param environment: A mapping of environment variable names to their respective values.
        :param stdin_path: Path to a file whose contents will be sent to the job's standard input.
        :param stdout_path: A path to a file in which to place the standard output stream of the
            job.
        :param stderr_path: A path to a file in which to place the standard error stream of the job.
        :param resources: The resource requirements specify the details of how the job is to be run
            on a cluster, such as the number and type of compute nodes used, etc.
        :param attributes: Job attributes are details about the job, such as the walltime, that are
            descriptive of how the job behaves. Attributes are, in principle, non-essential in that
            the job could run even though no attributes are specified. In practice, specifying a
            walltime is often necessary to prevent LRMs from prematurely terminating a job.
        :param pre_launch: An optional path to a pre-launch script. The pre-launch script is
            sourced before the launcher is invoked. It, therefore, runs on the service node of the
            job rather than on all of the compute nodes allocated to the job.
        :param post_launch: An optional path to a post-launch script. The post-launch script is
            sourced after all the ranks of the job executable complete and is sourced on the same
            node as the pre-launch script.
        :param launcher: The name of a launcher to use, such as "mpirun", "srun", "single", etc.
            For a list of available launchers, see :ref:`launchers`
        """
        assert check_argument_types()

        self._name = name
        self.executable = executable
        self.arguments = arguments
        self.directory = directory
        self.inherit_environment = inherit_environment
        self.environment = environment
        self.stdin_path = stdin_path
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.resources = resources
        self.attributes = attributes if attributes is not None else JobAttributes()
        self.pre_launch = pre_launch
        self.post_launch = post_launch
        self.launcher = launcher

        # TODO: `resources` is of type `ResourceSpec`, not `ResourceSpecV1`.  An
        #       connector trying to access `job.spec.resources.process_count`
        #       will thus face an `unknown member` warning.

    @property
    def name(self) -> Optional[str]:
        """Returns the name of the job."""
        if self._name is None:
            return self.executable
        else:
            return self._name

    @property
    def _init_job_spec_dict(self) -> Dict[str, Any]:
        """Returns jobspec structure as dict."""
        # convention:
        #  - if expected value is a string then the dict is initialized with an empty string
        # - if the expected value is an object than the key is initialzied with None

        job_spec: Dict[str, Any]
        job_spec = {
            'name': '',
            'executable': '',
            'arguments': [],
            'directory': None,
            'inherit_environment': True,
            'environment': {},
            'stdin_path': None,
            'stdout_path': None,
            'stderr_path': None,
            'resources': None,
            'attributes': None,
            'pre_launch': None,
            'post_launch': None,
            'launcher': None
        }

        return job_spec

    @property
    def to_dict(self) -> Dict[str, Any]:
        """Returns a dictionary representation of this object."""
        d = self._init_job_spec_dict

        # Keep path spelling intact. Paths in a JobSpec describe the compute
        # side and must not be resolved relative to the controller process.
        d['name'] = self._name
        d['executable'] = self.executable
        d['arguments'] = self.arguments
        d['directory'] = str(self.directory) if self.directory is not None else None
        d['inherit_environment'] = self.inherit_environment
        d['environment'] = self.environment
        d['stdin_path'] = str(self.stdin_path) if self.stdin_path is not None else None
        d['stdout_path'] = str(self.stdout_path) if self.stdout_path is not None else None
        d['stderr_path'] = str(self.stderr_path) if self.stderr_path is not None else None
        d['pre_launch'] = str(self.pre_launch) if self.pre_launch is not None else None
        d['post_launch'] = str(self.post_launch) if self.post_launch is not None else None
        d['launcher'] = self.launcher

        if self.attributes is not None:
            d['attributes'] = {
                'duration': str(self.attributes.duration)
                if self.attributes.duration is not None else None,
                'queue_name': self.attributes.queue_name,
                'project_name': self.attributes.project_name,
                'reservation_id': self.attributes.reservation_id,
                'custom_attributes': self.attributes._custom_attributes
                if self.attributes._custom_attributes is not None else {},
            }
        else:
            d['attributes'] = None

        if self.resources is not None:
            if self.resources.version != 1:
                raise ValueError('Unsupported ResourceSpec version: %s' % self.resources.version)
            d['resources'] = {
                'version': 1,
                'node_count': getattr(self.resources, 'node_count'),
                'process_count': getattr(self.resources, 'process_count'),
                'processes_per_node': getattr(self.resources, 'processes_per_node'),
                'cpu_cores_per_process': getattr(self.resources, 'cpu_cores_per_process'),
                'gpu_cores_per_process': getattr(self.resources, 'gpu_cores_per_process'),
                'exclusive_node_use': getattr(self.resources, 'exclusive_node_use'),
            }

        return d
