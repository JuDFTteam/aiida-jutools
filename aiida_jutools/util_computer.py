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
"""Tools for working with aiida Computer nodes."""

# python imports
import dataclasses as dc
from typing import AnyStr, List, Tuple

# aiida imports
from aiida.orm import Computer, Dict


def get_computer(computer_name_pattern: str = ""):
    """Query computer.

    :param computer_name_pattern: (sub)string of computer name, case-insensitive, no regex. default = "": get all computers.
    :return: aiida Computer if unique, computer list if not, None if no match

    DEVNOTE: computer db schema uses 'name', while computer python class uses .label for the *same thing* (valid at least for aiida >=1.0<=1.5.2).
    """
    from aiida.orm import QueryBuilder
    # version compatibility check: old: computer.name, new: computer.label. else error.
    qb = QueryBuilder()
    computer = None
    computers = qb.append(Computer, filters={'name': {'ilike': f"%{computer_name_pattern}%"}}).all()
    if len(computers):
        if len(computers) == 1:
            return computers[0][0]
        # convert result list of single-item lists to list
        return [li[0] for li in computers]
    return computers


def get_computer_options(computer_name: str, as_Dict: bool = True, account_name: str = "jara0191", gpu: bool = None):
    """Return builder metadata options for given aiida computer.

    Implementations available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.
    - 'claix18': RWTH claix18 cluster (should work for claix16 but untested).
    - 'localhost': FZJ PGI-1 desktop workstation.

    Note: claix18: has 48 procs/node. But default here is set to 24 (std for kkrhost). change manually if desired.

    :param computer_name: aiida computer
    :param as_Dict: if True, return as aiida Dict, else normal dict.
    :param account_name: account name. currently only for 'claix18'.
    :param gpu: False: exclude gpu partitions. True exclude non-gpu partitions. None: ignore this option. Currently only for 'iffslurm'.
    :return: builder metadata options
    """
    if computer_name == "localhost":
        # run a job on localhost: 1 pc, 4 cpus
        options = {'resources': {'num_machines': 1, 'tot_num_mpiprocs': 4},
                   'withmpi': True}

    elif computer_name == "claix18":
        # run a job on cluster: 1 node, xx processors on account jara0191
        options = {'max_wallclock_seconds': 60 * 60,
                   'resources': {'num_machines': 1, 'tot_num_mpiprocs': 24},
                   'custom_scheduler_commands': f"#SBATCH --account={account_name}",
                   'withmpi': True}

    elif computer_name == "iffslurm":
        # for iffslurm cluster, got to select partition myself
        computer = get_computer(computer_name_pattern=computer_name)
        partition_name, idle_nodes_count = get_least_occupied_partition(computer=computer, gpu=gpu)
        if not partition_name:
            raise ValueError(f"No partitions found for computer {computer_name}")
        options = {'max_wallclock_seconds': 36000,
                   'resources': {'num_machines': 1, 'tot_num_mpiprocs': 24},
                   # 'custom_scheduler_commands': f"#SBATCH --partition {partition_name}",
                   'queue_name': partition_name,
                   'withmpi': True}
        print(f"WARNING: for computer {computer_name}, check which partitions are idle with 'sinfo'. "
              f"Randomly default selected partition: {partition_name}.")
    else:
        raise NotImplementedError("Not implemented for this computer.")

    if as_Dict:
        return Dict(dict=options)
    else:
        import copy
        return copy.deepcopy(options)


def shell_command(computer: Computer, command: str) -> Tuple[AnyStr, AnyStr, AnyStr]:
    """Get output of shell command on aiida computer.

    Assume aiida computer is remote. so execute remote command via get_transport().

    Note: if you get a port error, you probably forgot to open an ssh tunnel to the remote computer on the specified ports first.

    Note: if stderr is NotExistent, the computer is probably not configured, eg imported.

    :param computer: aiida computer
    :param command: shell command to execute
    :return: tuple of strings exit_code, stdout, stderr. Use only stdout like: _,stdout,_ = shell_command(...).
    """
    from aiida.common.exceptions import AiidaException

    assert isinstance(computer, Computer), "computer is not a Computer, but a %r" % type(computer)
    # import signal
    # signal.alarm(maxwait)
    try:
        with computer.get_transport() as connection:
            exit_code, stdout, stderr = connection.exec_command_wait(command)
    except AiidaException as err:
        # common error: NotExistent. often cause computer not configured, eg imported.
        exit_code = type(err)
        stdout = ''
        stderr = err.args[0]
    # signal.alarm(0)

    return exit_code, stdout, stderr


def get_partitions(computer: Computer, gpu: bool = None, silent: bool = False) -> tuple:
    """Get list of the remote computer (cluster's) partition sorted by highest number of idle nodes descending.

    Implementations available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.

    :param computer: aiida computer.
    :type computer: Computer
    :param gpu: False: exclude gpu partitions. True exclude non-gpu partitions. None: ignore this option.
    :param silent: True: do not print out any info.
    :return: list of lists of: name of the partition with the highest idle nodes count, idle nodes count
    """
    if 'iffslurm' in computer.label:

        iffslurm_cmd_sorted_partition_list = """{ for p in $(sinfo --noheader --format="%R"); do echo "$p $(sinfo -p "${p}" --noheader --format="%t %n" | awk '$1 == "idle"' | wc -l)"; done } | sort -k 2 -n -r"""
        iffslurm_partition_max_idle_nodes = """{ for p in $(sinfo --noheader --format="%R"); do echo "$p $(sinfo -p "${p}" --noheader --format="%t %n" | awk '$1 == "idle"' | wc -l)"; done } | sort -k 2 -n -r | awk 'NR == 1 { print $1 }'"""

        exit_code, stdout, stderr = shell_command(computer=computer, command=iffslurm_cmd_sorted_partition_list)
        # turn into list of strings
        idle_nodes = stdout.split('\n')
        # split into partition and idle_nodes_count, filter out empty entries
        idle_nodes = [partition_nodes.split(' ') for partition_nodes in idle_nodes if partition_nodes]
        # filter out 'gpu' partitions
        if gpu is None:
            idle_nodes = [[pn[0], int(pn[1])] for pn in idle_nodes]
        elif not gpu:
            idle_nodes = [[pn[0], int(pn[1])] for pn in idle_nodes if not 'gpu' in pn[0]]
        elif gpu:
            idle_nodes = [[pn[0], int(pn[1])] for pn in idle_nodes if 'gpu' in pn[0]]
        # print out info about total remaining idle nodes
        if not silent:
            sum_idle_nodes = sum([pn[1] for pn in idle_nodes])
            print(f"Idle nodes left on computer '{computer.label}': {sum_idle_nodes}")

        return idle_nodes
    else:

        raise NotImplementedError(f"{get_partitions.__name__} not implemented for computer {computer.label}")


def get_least_occupied_partition(computer: Computer, gpu: bool = None, silent: bool = False) -> tuple:
    """Get name of the remote computer (cluster's) partition with the highest number of idle nodes.

    Implementations available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.

    :param computer: aiida computer.
    :type computer: Computer
    :param gpu: False: exclude gpu partitions. True exclude non-gpu partitions. None: ignore this option.
    :param silent: True: do not print out any info.
    :return: tuple of: name of the partition with the highest idle nodes count, idle nodes count
    """
    if 'iffslurm' in computer.label:
        idle_nodes = get_partitions(computer=computer, gpu=gpu, silent=silent)
        # if anything is left, get the first partition (ie the one with most idle nodes)
        partition, idle_nodes_count = idle_nodes[0] if idle_nodes else (None, None)
        return partition, idle_nodes_count
    else:
        raise NotImplementedError(
            f"{get_least_occupied_partition.__name__} not implemented for computer {computer.label}")


@dc.dataclass
class QuotaQuerierSettings:
    """Settings for QuotaQuerier.

    :param command: computer-specific, full 'quota' command, including optons etc. username or groupname
    :param header_line_count: no. of lines of header before quota table in quota output string.
    :param column_space_used: column label for space used in table in quota output string.
    :param column_space_hard: column label for space hard limit in table in quota output string.
    :param dirname_pattern: uniquely identifying (sub)string of path to return quota for. Else first table entry.
    :param min_free_space: memory space too stay away from hard limit. string like "10G" = 10 gigabyte = default.
    :param comment: optional comment on usage, e.g. for different template instances.
    """
    from masci_tools.util import python_util

    command: str = "quota"
    header_line_count: int = 1
    column_space_used: str = "used"
    column_space_hard: str = "hard"
    dirname_pattern: str = ""
    min_free_space: str = "10G"
    comment: List[AnyStr] = python_util.dataclass_default_field([""])


class QuotaQuerier:
    def __init__(self, computer: Computer, settings: QuotaQuerierSettings):
        """Convenience methods for getting the quota of aiida configured computer nodes.

        Use this class' builder class to create an intance.

        :param computer:  an aiida configured computer
        :type computer: Computer
        :param settings: settings
        :type settings: QuotaQuerierSettings
        """
        self.computer = computer
        self.settings = settings

    def get_quota(self):
        """Get quota as a pandas dataframe.
        """
        import pandas as pd
        import io
        s = self.settings

        exit_code, stdout, stderr = shell_command(self.computer, s.command)

        # strip away the header lines:
        # # stdout is a single string, lines sep. by '\n'
        for i in range(s.header_line_count):
            stdout = stdout[stdout.find("\n") + 1:].lstrip()

        # convert remaining string to dataframe
        df = pd.read_table(io.StringIO(stdout), sep=r"\s+")
        return df

    def is_min_free_space_left(self) -> bool:
        """Check output of quota command on computer to see if more than 'buffer' space is available.

        :return: True if used space > (hard limit - buffer) , False if not, None if could not find out.
        """
        import humanfriendly
        s = self.settings

        if not s.dirname_pattern or not s.min_free_space:
            raise ValueError("settings dirname_pattern and/or min_free_space not set!")

        df = self.get_quota()

        # extract row for user dirname we want to get quota of
        row_mask = df.iloc[:, 0].str.contains(s.dirname_pattern)
        row = df[row_mask]
        row_idx = row.index[0]

        # extract bytesize used, hard limit
        used = humanfriendly.parse_size(row[s.column_space_used][row_idx])
        hard = humanfriendly.parse_size(row[s.column_space_hard][row_idx])

        # check if free space is big enough
        min_free_space = humanfriendly.parse_size(s.min_free_space)
        is_min_free_space_left = (hard - used) > min_free_space
        if not is_min_free_space_left:
            print(
                f"not enough space available on computer (used: {used}, hard limit: {hard}, requested free space:"
                f" {s.min_free_space}). Try cleaning up the aiida computer's workdir (see verdi calcjob cleanworkdir -h).")
        return is_min_free_space_left


class QuotaQuerierBuilder:
    templates = ["rwth_cluster", "iff_workstation"]

    def __init__(self):
        # check templates
        assert self.templates[0] == "rwth_cluster"
        assert self.templates[1] == "iff_workstation"

    def print_available_templates(self):
        print(self.templates)

    def build(self, template: str, computer: Computer):
        """Build a QuotaQuerier for a given template and computer.

        The template configures the settings such that the quota will return the desired
        output for that computer.

        Implementations available for these computers:
        - 'rwth_cluster': RWTH claix18 cluster (should work for claix16 but untested).
        - 'iff_workstation': FZJ PGI-1 desktop workstation.
        - TODO: 'iffslurm'.

        :param template: a valid template from this class
        :param computer: configured aiida computer
        :return: an instance of a quota querier for that computer.
        :rtype: QuotaQuerier
        """
        print(f"Configuring {QuotaQuerierSettings.__name__} for template '{template}'.")

        def _print_comment(qq):
            for line in qq.settings.comment:
                print(line)

        qq = None
        if template == self.templates[0]:
            qqs = QuotaQuerierSettings()
            qqs.command = "quota -u <USER_OR_GROUP>"
            qqs.comment = ["Before usage, adjust 'settings' manually:",
                           "1. (required) In 'command', replace user-or-group-name wildcard (x = x.replace(...)).",
                           "2. (optional) Replace 'dirname_pattern'. Affects some not all methods behavior.", ]
            qq = QuotaQuerier(computer, qqs)

        elif template == self.templates[1]:
            qqs = QuotaQuerierSettings()
            qqs.command = "/usr/local/bin/quota.py"
            qqs.column_space_used = "blocks"
            qqs.column_space_hard = "limit"
            qqs.comment = [
                "Note: about command: 'quota' on iff workstations is an alias for the ",
                "python script /usr/local/bin/quota.py. But calling 'quota -u <USER>'",
                "*over* aiida instead will call the actual quota with different output,",
                "which is not supported here.", ]
            qq = QuotaQuerier(computer, qqs)
        else:
            raise NotImplementedError(f"No {QuotaQuerier.__name__} template '{template}' exists.")

        _print_comment(qq)
        return qq
