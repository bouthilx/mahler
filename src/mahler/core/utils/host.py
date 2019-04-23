import copy
import logging
import os
import platform
import psutil
import subprocess
import xml.etree.ElementTree

import bson
import numpy

import mahler.core
from mahler.core.utils.flatten import flatten, unflatten


logger = logging.getLogger(__name__)


def fetch_host_name():
    return os.environ.get('CLUSTERNAME', 'local')


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
                if "cannot encode" not in str(e).lower():
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


def get_cpu_usage(pid):
    try:
        process = psutil.Process(pid)

        with process.oneshot():
            mem = process.memory_full_info().uss
            cpu_percent = process.cpu_percent()
            cpu_num = process.cpu_num()
    except psutil._exceptions.NoSuchProcess:
        return {str(pid): {}}

    children = {}
    total_cpu_percent = cpu_percent
    total_mem = mem
    cpu_nums = set([cpu_num])
    for child in process.children(recursive=False):
        child_stats = get_cpu_usage(child.pid)
        child_pid = str(child.pid)
        if not child_stats[child_pid]:
            continue
        children.update({child_pid: child_stats[child_pid]})
        child_total_stats = child_stats['total']
        total_mem += child_total_stats['mem']
        total_cpu_percent += child_total_stats['cpu_percent']
        cpu_nums |= set(child_total_stats['cpu_nums'])

    process_stats = {
        str(pid): {
            'mem': mem,
            'cpu_percent': cpu_percent,
            'cpu_num': cpu_num,
            'children': children},
        'total': {
            'cpu_percent': total_cpu_percent,
            'mem': total_mem,
            'cpu_nums': list(cpu_nums)}}

    return process_stats


def get_max_usage(metrics):
    cores_avail = os.environ.get('SLURM_CPUS_PER_NODE', psutil.cpu_count())

    stats = {}
    for usage in metrics:
        if usage['gpu'] and usage['gpu']['memory']['process']['max'] > stats.get('gpu.memory', -1):
            memory = stats['gpu.memory'] = usage['gpu']['memory']['process']['max']
            total_mem_used = usage['gpu']['memory']['used']['max']
            # We use mean for util because reaching 100 is not critical.
            # Well... it is simply impossible.
            if total_mem_used:
                stats['gpu.util'] = int(memory / total_mem_used *
                                        usage['gpu']['util']['mean'] + 0.5)
            else:
                stats['gpu.util'] = 0

        stats['cpu.memory'] = max(stats.get('cpu.memory', 0), usage['cpu']['total']['mem']['max'])
        cpu_util = int(usage['cpu']['total']['cpu_percent']['max'] / cores_avail * 100 + 0.5)
        stats['cpu.util'] = max(stats.get('cpu.util', 0), cpu_util)

    return stats


def get_gpu_usage(pid=None):
    gpu = {}

    try:
        nvidia_xml = subprocess.check_output(['nvidia-smi', '-q', '-x']).decode()
    except (FileNotFoundError, OSError, subprocess.CalledProcessError):
        return {}

    for child in xml.etree.ElementTree.fromstring(nvidia_xml):
        if child.tag != 'gpu':
            continue

        memory = child.find('fb_memory_usage')
        process_memory = ''
        for process_child in child.find('processes'):
            if int(process_child.find('pid').text) == pid:
                process_memory = process_child.find('used_memory').text
                break

        # NOTE: For now, we assume that there is a single GPU.
        # Continue until we hit the GPU where the process resides.
        # if not process_memory:
        #     continue

        processes = child.find('processes')
        gpu = {
            'id': child.attrib['id'],
            'model': child.find('product_name').text,
            'util': int(child.find('utilization').find('gpu_util').text.replace(' %', '')),
            'memory': {
                'total': convert_format(memory.find('total').text),
                'used': convert_format(memory.find('used').text),
                'free': convert_format(memory.find('free').text),
                'process': convert_format(process_memory)
            }}
        break

    return gpu


class ResourceUsageMonitor:
    def __init__(self, pid):
        self.pid = pid
        self.reset()

    def update(self):

        cpu_usage = get_cpu_usage(self.pid)
        gpu_usage = get_gpu_usage(self.pid)

        self.last_point = {'cpu': cpu_usage, 'gpu': gpu_usage}
        flattened_last_point = flatten(self.last_point)
        for key, values in self.stats.items():
            values.append(int(flattened_last_point[key]))

    def reset(self):
        self.last_point = None
        self.stats = flatten({
            'gpu': {
                'memory': {
                    'process': [],
                    'used': []},
                'util': []},
            'cpu.total': {
                    'cpu_percent': [],
                    'mem': []}})

    def get(self):
        usage = flatten(self.last_point)
        for name, values in self.stats.items():
            array = numpy.array(values)
            stats = {}
            for op_name in 'mean min max std'.split():
                stats[op_name] = float(getattr(numpy, op_name)(array))
            usage[name] = stats
            
        return unflatten(usage)


def convert_format(v):

    if v.endswith(' MiB'):
        return int(v[:-4]) * 2**20

    return 0
