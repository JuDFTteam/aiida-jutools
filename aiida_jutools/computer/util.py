# -*- coding: utf-8 -*-
###############################################################################
# Copyright (c), Forschungszentrum Jülich GmbH, IAS-1/PGI-1, Germany.         #
#                All rights reserved.                                         #
# This file is part of the aiida-jutools package.                             #
# (AiiDA JuDFT tools)                                                         #
#                                                                             #
# The code is hosted on GitHub at https://github.com/judftteam/aiida-jutools. #
# For further information on the license, see the LICENSE.txt file.           #
# For further information please visit http://judft.de/.                      #
#                                                                             #
###############################################################################
"""Tools for working with aiida Computer nodes: utils."""

import typing as _typing

import aiida as _aiida
from aiida import orm as _orm


def get_computers(computer_name_pattern: str = "") -> _typing.List[_orm.Computer]:
    """Query computer.

    :param computer_name_pattern: (sub)string of computer label, case-insensitive, no regex. default = "":
           get all computers.
    :return: aiida Computer if unique, list of Computers if not, empty list if no match
    """
    # version compatibility check: aiida v1: computer.name, v2: computer.label. else error.
    qb = _orm.QueryBuilder()
    computer = None
    return qb.append(
        _orm.Computer,
        filters={'label': {'ilike': f"%{computer_name_pattern}%"}},
    ).all(flat=True)


def is_slurm_computer(computer: _orm.Computer) -> bool:
    """Check if the computer uses SLURM scheduler.

    :param computer: aiida computer.
    :return: True if computer uses SLURM scheduler, False otherwise.
    """
    return 'slurm' in computer.scheduler_type.lower()


def shell_command(computer: _orm.Computer,
                  command: str) -> _typing.Tuple[str, str, str]:
    """Get output of shell command on aiida computer.

    Assume aiida computer is remote. so execute remote command via get_transport().

    Note: if you get a port error, you probably forgot to open an ssh tunnel to the remote computer on the
    specified ports first.

    Note: if stderr is NotExistent, the computer is probably not configured, eg imported.

    :param computer: aiida computer
    :param command: shell command to execute
    :return: tuple of strings exit_code, stdout, stderr. Use only stdout like: _,stdout,_ = shell_command(...).
    """

    assert isinstance(computer, _orm.Computer), "computer is not a Computer, but a %r" % type(computer)
    # import signal
    # signal.alarm(maxwait)
    try:
        with computer.get_transport() as connection:
            exit_code, stdout, stderr = connection.exec_command_wait(command)
    except _aiida.common.exceptions.AiidaException as err:
        # common error: NotExistent. often cause computer not configured, eg imported.
        exit_code = type(err)
        stdout = ''
        stderr = err.args[0]
    # signal.alarm(0)

    return exit_code, stdout, stderr


def get_queues(computer: _orm.Computer,
               gpu: bool = None,
               with_node_count: bool = True,
               with_arch: bool = False,
               silent: bool = False) -> _typing.List[_typing.Union[str, _typing.List[_typing.Union[str, int]]]]:
    """Get list of the remote computer (cluster's) queues (slurm: partitions) sorted by highest number of idle nodes
    descending.

    Works with any computer that uses SLURM scheduler.

    :param computer: aiida computer.
    :param gpu: False: exclude gpu queues. True exclude non-gpu partitions. None: ignore this option.
    :param with_node_count: True: return queue info with node counts (total and idle), False: just queue names.
    :param with_arch: True: include architecture (AMD/Intel) for each queue. False: omit architecture.
    :param silent: True: do not print out any info.
    :return: list of queue information. Format depends on parameters:
        - with_node_count=False, with_arch=False: ['queue1', 'queue2', ...]
        - with_node_count=True, with_arch=False: [['queue1', total_nodes, idle_nodes], ...]
        - with_node_count=False, with_arch=True: [['queue1', 'AMD'], ...]
        - with_node_count=True, with_arch=True: [['queue1', total_nodes, idle_nodes, 'AMD'], ...]
    :raise: NotImplementedError if computer does not use SLURM scheduler.

    DEVNOTES: TODO: replace filter by shell command with sinfo -> pandas.Dataframe -> apply filters.
    """
    if not is_slurm_computer(computer):
        raise NotImplementedError(f"{get_queues.__name__} only works with SLURM scheduler. "
                                 f"Computer '{computer.label}' uses scheduler: {computer.scheduler_type}")

    # Command to get partition name, total nodes, and idle nodes
    # Output format: partition_name total_nodes idle_nodes
    slurm_cmd_queue_info = """{ for p in $(sinfo --noheader --format="%R"); do \
        total=$(sinfo -p "${p}" --noheader --format="%D"); \
        idle=$(sinfo -p "${p}" --noheader --format="%t %n" | awk '$1 == "idle"' | wc -l); \
        echo "$p $total $idle"; \
    done } | sort -k 3 -n -r"""

    exit_code, stdout, stderr = shell_command(computer=computer, command=slurm_cmd_queue_info)

    # Parse output into list of [partition_name, total_nodes, idle_nodes]
    queue_info = []
    for line in stdout.split('\n'):
        if line.strip():
            parts = line.split()
            if len(parts) >= 3:
                queue_name = parts[0]
                total_nodes = int(parts[1])
                idle_nodes = int(parts[2])
                queue_info.append([queue_name, total_nodes, idle_nodes])

    # Filter by gpu option
    if gpu is not None:
        if gpu:
            # Keep only gpu queues
            queue_info = [qi for qi in queue_info if 'gpu' in qi[0].lower()]
        else:
            # Exclude gpu queues
            queue_info = [qi for qi in queue_info if 'gpu' not in qi[0].lower()]

    # Add architecture information if requested
    if with_arch:
        for qi in queue_info:
            queue_name = qi[0]
            try:
                arch = get_queue_architecture(computer=computer, queue_name=queue_name)
                qi.append(arch)
            except (ValueError, NotImplementedError) as e:
                # If architecture detection fails, use 'unknown'
                qi.append('unknown')

    # Print summary if not silent
    if not silent:
        sum_total_nodes = sum(qi[1] for qi in queue_info)
        sum_idle_nodes = sum(qi[2] for qi in queue_info)
        print(f"Idle nodes left on computer '{computer.label}': {sum_idle_nodes}/{sum_total_nodes}")

    # Format output based on parameters
    if not with_node_count and not with_arch:
        # Return just queue names
        return [qi[0] for qi in queue_info]
    elif not with_node_count and with_arch:
        # Return [queue_name, architecture]
        return [[qi[0], qi[3]] for qi in queue_info]
    elif with_node_count and not with_arch:
        # Return [queue_name, total_nodes, idle_nodes]
        return [[qi[0], qi[1], qi[2]] for qi in queue_info]
    else:
        # Return [queue_name, total_nodes, idle_nodes, architecture]
        return queue_info


def get_queue_architecture(computer: _orm.Computer,
                           queue_name: str) -> str:
    """Get the CPU architecture (AMD or Intel) for a specific queue/partition on a SLURM cluster.

    Works with any computer that uses SLURM scheduler.

    :param computer: aiida computer.
    :param queue_name: exact name of the queue/partition.
    :return: 'AMD' or 'intel' depending on the CPU vendor.
    :raise: NotImplementedError if computer does not use SLURM scheduler.
    :raise: ValueError if architecture cannot be determined from output.
    """
    if not is_slurm_computer(computer):
        raise NotImplementedError(f"{get_queue_architecture.__name__} only works with SLURM scheduler. "
                                 f"Computer '{computer.label}' uses scheduler: {computer.scheduler_type}")

    # Run lscpu on a node in the specified partition to get CPU vendor info
    command = f"srun -p {queue_name} --time=00:01:00 lscpu 2>/dev/null | grep 'Vendor ID'"
    exit_code, stdout, stderr = shell_command(computer=computer, command=command)

    if not stdout:
        raise ValueError(f"Could not determine architecture for queue '{queue_name}' on computer "
                        f"'{computer.label}'. Command output was empty. stderr: {stderr}")

    # Parse the vendor ID from output
    stdout_lower = stdout.lower()
    if 'authenticamd' in stdout_lower:
        return 'AMD'
    elif 'genuineintel' in stdout_lower:
        return 'intel'
    else:
        raise ValueError(f"Could not determine architecture for queue '{queue_name}' on computer "
                        f"'{computer.label}'. Unexpected Vendor ID in output: {stdout}")


def get_least_occupied_queue(computer: _orm.Computer,
                             gpu: bool = None,
                             with_node_count: bool = True,
                             silent: bool = False) -> _typing.Union[_typing.Tuple[str, int, int], str]:
    """Get name of the remote computer (cluster's) queue (slurm: partition) with the highest number of idle nodes.

    Works with any computer that uses SLURM scheduler.

    :param computer: aiida computer.
    :param gpu: False: exclude gpu queues. True exclude non-gpu queues. None: ignore this option.
    :param with_node_count: True: return tuple of (queue_name, total_nodes, idle_nodes), False: just queue name.
    :param silent: True: do not print out any info.
    :return: tuple of (queue_name, total_nodes, idle_nodes) or just queue name
    :raise: NotImplementedError if computer does not use SLURM scheduler.
    """
    queues = get_queues(computer=computer, gpu=gpu, with_node_count=True, with_arch=False, silent=silent)
    # if anything is left, get the first queue (ie the one with most idle nodes)
    if queues:
        queue_info = queues[0]  # [queue_name, total_nodes, idle_nodes]
        queue_name = queue_info[0]
        total_nodes = queue_info[1]
        idle_nodes_count = queue_info[2]
        return (queue_name, total_nodes, idle_nodes_count) if with_node_count else queue_name
    else:
        return (None, None, None) if with_node_count else None
