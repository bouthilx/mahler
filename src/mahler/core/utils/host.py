import logging
import os
import platform
import subprocess
import xml

import bson

import mahler.core


logger = logging.getLogger(__name__)


def fetch_host_name():
    return os.environ['CLUSTERNAME']


def fetch_host_info():
    host_info = dict()
    host_info['cpus'] = fetch_cpus_info()
    host_info['gpus'] = fetch_gpus_info()
    host_info['platform'] = fetch_platform_info()
    host_info['env'] = fetch_host_env_vars()

    return host_info


def fetch_cpus_info():
    lscpu_output = subprocess.check_output("lscpu", shell=True).strip().decode()

    cpus_info = dict()
    for line in lscpu_output.split("\n"):
        items = line.split(":")
        cpus_info[items[0]] = ":".join(item.strip(' \t') for item in items[1:])

    cpus_info.pop('CPU MHz', None)

    return cpus_info


def fetch_platform_info():

    platform_info = dict()
    platform_info['mahler'] = mahler.core.__version__

    for key in list(sorted(platform.__dict__.keys())):
        if key.startswith('_'):
            continue

        item = getattr(platform, key)

        if callable(item):
            try:
                item = item()
            except BaseException as e:
                logger.debug("Cannot call platform.{}: {}".format(key, str(e)))

        if not callable(item):
            try:
                bson.BSON.encode(dict(a=item))
            except bson.errors.InvalidDocument as e:
                if "Cannot encode" not in str(e):
                    raise
            else:
                platform_info[key] = item

    return platform_info


ENV_VARS = ([
    'CLUSTERNAME', 'CUDA_VISIBLE_DEVICES']
    + [
    'SLURM_ARRAY_TASK_COUNT',  # Total number of tasks in a job array.
    'SLURM_ARRAY_TASK_ID',     # Job array ID (index) number.
    'SLURM_ARRAY_TASK_MAX',    # Job array's maximum ID (index) number.
    'SLURM_ARRAY_TASK_MIN',    # Job array's minimum ID (index) number.
    'SLURM_ARRAY_TASK_STEP',   # Job array's index step size.
    'SLURM_ARRAY_JOB_ID',      # Job array's master job ID number.
    'SLURM_CHECKPOINT_IMAGE_DIR', # Directory into which checkpoint images should be written if specified on the execute line.
    'SLURM_CLUSTER_NAME', # Name of the cluster on which the job is executing.
    'SLURM_CPUS_ON_NODE', # Number of CPUS on the allocated node.
    'SLURM_CPUS_PER_TASK', # Number of cpus requested per task. Only set if the --cpus-per-task option is specified.
    'SLURM_DISTRIBUTION', # Same as -m, --distribution
    'SLURM_GTIDS', # Global task IDs running on this node. Zero origin and comma separated.
    'SLURM_JOB_ACCOUNT', # Account name associated of the job allocation.
    'SLURM_JOB_ID', # The ID of the job allocation.
    'SLURM_JOB_CPUS_PER_NODE', # Count of processors available to the job on this node. Note the select/linear plugin allocates entire nodes to jobs, so the value indicates the total count of CPUs on the node. The select/cons_res plugin allocates individual processors to jobs, so this number indicates the number of processors on this node allocated to the job.
    'SLURM_JOB_DEPENDENCY', # Set to value of the --dependency option.
    'SLURM_JOB_NAME', # Name of the job.
    'SLURM_JOB_NODELIST', # List of nodes allocated to the job.
    'SLURM_JOB_NUM_NODES', # Total number of nodes in the job's resource allocation.
    'SLURM_JOB_PARTITION', # Name of the partition in which the job is running.
    'SLURM_JOB_QOS', # Quality Of Service (QOS) of the job allocation.
    'SLURM_JOB_RESERVATION', # Advanced reservation containing the job allocation, if any.
    'SLURM_LOCALID', # Node local task ID for the process within a job.
    'SLURM_MEM_PER_CPU', # Same as --mem-per-cpu
    'SLURM_MEM_PER_NODE', # Same as --mem
    'SLURM_NODE_ALIASES', # Sets of node name, communication address and hostname for nodes allocated to the job from the cloud. Each element in the set if colon separated and each set is comma separated. For example: SLURM_NODE_ALIASES=ec0:1.2.3.4:foo,ec1:1.2.3.5:bar
    'SLURM_NODEID', # ID of the nodes allocated.
    'SLURM_NTASKS', # Same as -n, --ntasks
    'SLURM_NTASKS_PER_CORE', # Number of tasks requested per core. Only set if the --ntasks-per-core option is specified.
    'SLURM_NTASKS_PER_NODE', # Number of tasks requested per node. Only set if the --ntasks-per-node option is specified.
    'SLURM_NTASKS_PER_SOCKET', # Number of tasks requested per socket. Only set if the --ntasks-per-socket option is specified.
    'SLURM_PACK_SIZE', # Set to count of components in heterogeneous job.
    'SLURM_PRIO_PROCESS', # The scheduling priority (nice value) at the time of job submission. This value is propagated to the spawned processes.
    'SLURM_PROCID', # The MPI rank (or relative process ID) of the current process
    'SLURM_PROFILE', # Same as --profile
    'SLURM_RESTART_COUNT', # If the job has been restarted due to system failure or has been explicitly requeued, this will be sent to the number of times the job has been restarted.
    'SLURM_SUBMIT_DIR', # The directory from which sbatch was invoked.
    'SLURM_SUBMIT_HOST', # The hostname of the computer from which sbatch was invoked.
    'SLURM_TASKS_PER_NODE', # Number of tasks to be initiated on each node. Values are comma separated and in the same order as SLURM_JOB_NODELIST. If two or more consecutive nodes are to have the same task count, that count is followed by "(x#)" where "#" is the repetition count. For example, "SLURM_TASKS_PER_NODE=2(x3),1" indicates that the first three nodes will each execute three tasks and the fourth node will execute one task.
    'SLURM_TASK_PID', # The process ID of the task being started.
    'SLURM_TOPOLOGY_ADDR', # This is set only if the system has the topology/tree plugin configured. The value will be set to the names network switches which may be involved in the job's communications from the system's top level switch down to the leaf switch and ending with node name. A period is used to separate each hardware component name.
    'SLURM_TOPOLOGY_ADDR_PATTERN', # This is set only if the system has the topology/tree plugin configured. The value will be set component types listed in SLURM_TOPOLOGY_ADDR. Each component will be identified as either "switch" or "node". A period is used to separate each hardware component type.
    'SLURMD_NODENAME'])


def fetch_host_env_vars():
    host_env_vars = dict()
    for env_var, value in os.environ.items():
        if env_var.startswith('_'):
            continue
        host_env_vars[env_var.lower()] = value

    return host_env_vars


def fetch_gpus_info():
    gpus_info = dict()

    try:
        nvidia_xml = subprocess.check_output(['nvidia-smi', '-q', '-x']).decode()
    except (FileNotFoundError, OSError, subprocess.CalledProcessError):
        return {}

    for child in xml.etree.ElementTree.fromstring(nvidia_xml):
        if child.tag == 'driver_version':
            gpus_info['driver_version'] = child.text
        if child.tag != 'gpu':
            continue
        gpu = dict((
            ('model', child.find('product_name').text),
            ('total_memory', (child.find('fb_memory_usage').find('total').text)),
            ('persistence_mode', (child.find('persistence_mode').text == 'Enabled'))
        ))
        gpus_info[child.attrib['id'].replace(".", ",")] = gpu

    return gpus_info
