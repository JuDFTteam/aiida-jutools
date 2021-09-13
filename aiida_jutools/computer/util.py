import typing as _typing

import aiida as _aiida
from aiida import orm as _orm


def get_computers(computer_name_pattern: str = "") -> _typing.List[_orm.Computer]:
    """Query computer.

    :param computer_name_pattern: (sub)string of computer name, case-insensitive, no regex. default = "":
           get all computers.
    :return: aiida Computer if unique, list of Computers if not, empty list if no match
    """
    # version compatibility check: old: computer.name, new: computer.label. else error.
    qb = _orm.QueryBuilder()
    computer = None
    return qb.append(
        _orm.Computer,
        filters={'name': {'ilike': f"%{computer_name_pattern}%"}},
    ).all(flat=True)


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
               silent: bool = False) -> _typing.List[_typing.List[_typing.Union[str, int]]]:
    """Get list of the remote computer (cluster's) queues (slurm: partitions) sorted by highest number of idle nodes
    descending.

    Implementations available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.

    :param computer: aiida computer.
    :param gpu: False: exclude gpu queues. True exclude non-gpu partitions. None: ignore this option.
    :param with_node_count: True: return queue names with resp. idle nodes count, False: just queue names.
    :param silent: True: do not print out any info.
    :return: list of [queue/partition name, idle nodes count] or just list of queue names
    :raise: NotImplementedError if get queues not implemented for that type (by label substring) of computer.

    DEVNOTES: TODO: replace filter by shell command with sinfo -> pandas.Dataframe -> apply filters.
    """
    if 'iffslurm' not in computer.label:
        raise NotImplementedError(f"{get_queues.__name__} not implemented for computer {computer.label}")
    iffslurm_cmd_sorted_partitions_list = """{ for p in $(sinfo --noheader --format="%R"); do echo "$p $(sinfo -p "${p}" --noheader --format="%t %n" | awk '$1 == "idle"' | wc -l)"; done } | sort -k 2 -n -r"""
    iffslurm_partition_max_idle_nodes = """{ for p in $(sinfo --noheader --format="%R"); do echo "$p $(sinfo -p "${p}" --noheader --format="%t %n" | awk '$1 == "idle"' | wc -l)"; done } | sort -k 2 -n -r | awk 'NR == 1 { print $1 }'"""

    exit_code, stdout, stderr = shell_command(computer=computer, command=iffslurm_cmd_sorted_partitions_list)
    # turn into list of strings
    idle_nodes = stdout.split('\n')
    # split into partition and idle_nodes_count, filter out empty entries
    idle_nodes = [partition_nodes.split(' ') for partition_nodes in idle_nodes if partition_nodes]
    # filter out 'gpu' partitions
    if gpu is None:
        idle_nodes = [[pn[0], int(pn[1])] for pn in idle_nodes]
    elif not gpu:
        idle_nodes = [[pn[0], int(pn[1])] for pn in idle_nodes if 'gpu' not in pn[0]]
    else:
        idle_nodes = [[pn[0], int(pn[1])] for pn in idle_nodes if 'gpu' in pn[0]]
        # print out info about total remaining idle nodes
    if not silent:
        sum_idle_nodes = sum(pn[1] for pn in idle_nodes)
        print(f"Idle nodes left on computer '{computer.label}': {sum_idle_nodes}")

    if not with_node_count:
        idle_nodes = [pn[0] for pn in idle_nodes]

    return idle_nodes


def get_least_occupied_queue(computer: _orm.Computer,
                             gpu: bool = None,
                             with_node_count: bool = True,
                             silent: bool = False) -> _typing.Union[_typing.Tuple[str, int], str]:
    """Get name of the remote computer (cluster's) queue (slurm: partition) with the highest number of idle nodes.

    Implementations available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.

    :param computer: aiida computer.
    :param gpu: False: exclude gpu queues. True exclude non-gpu queues. None: ignore this option.
    :param with_node_count: True: queue name with idle nodes count, False: just queue name.
    :param silent: True: do not print out any info.
    :return: tuple of queue name, idle nodes count, or just queue name
    :raise: NotImplementedError if get queues not implemented for that type (by label substring) of computer.
    """
    idle_nodes = get_queues(computer=computer, gpu=gpu, with_node_count=True, silent=silent)
    # if anything is left, get the first queue (ie the one with most idle nodes)
    queue_name, idle_nodes_count = idle_nodes[0] if idle_nodes else (None, None)
    return (queue_name, idle_nodes_count) if with_node_count else queue_name