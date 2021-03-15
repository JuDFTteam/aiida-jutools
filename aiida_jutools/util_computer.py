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
    :return: aiida Computer if unique, list of Computers if not, empty list if no match
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


def get_computer_options(computer_name_pattern: str, partition_name: str = None, account_name: str = None,
                         with_mpi: bool = True, gpu: bool = None, as_Dict: bool = True, silent: bool = False):
    """Return builder metadata options for given aiida computer.

    Options available for these computers categories:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.
    - 'claix': RWTH claix18 cluster (should work for claix16 but untested).
    - 'localhost': FZJ PGI-1 desktop workstation.

    Note: The argument computer_name_pattern serves a double purpose. Firstly, it selects the computer category.
    For example, 'abc_iffslurm_def' will select category 'iffslurm'. Secondly, if no partition_name or account_name
    is supplied to specify the computer partition, it will query a configured aiida computer with that name pattern,
    connect to it and find the currently least occupied matching partition. So, if you have several iffslurm aiida
    computers in your profile, you can specify exactly which one to use for that second part.

    Note: claix18: has 48 procs/node. But default here is set to 24 (std for kkrhost). change manually if desired.

    :param computer_name_pattern: must contain substring of one of the computer categories above.
    :param partition_name: If no account_name given or supported, will select this partition.
    :param account_name: If no partition_name given or supported, will set this account name for job submission.
    :param as_Dict: if True, return as aiida Dict, else normal dict.
    :param account_name: account name. Optional, default None. Currently only applied for computer type 'claix18'.
    :param gpu: False: exclude gpu partitions. True exclude non-gpu partitions. None: ignore this option. Currently only for 'iffslurm'.
    :param with_mpi: True: tot_num_mpiprocs to reasonable max. of respective computer/partition. False: set it to 1.
    :param silent: True: do not print out any info.
    :return: builder metadata options
    """
    available_implementations = {"localhost", "iffslurm", "claix"}

    if "localhost" in computer_name_pattern:
        # run a job on localhost: 1 pc, 4 cpus
        options = Dict(dict={'resources': {'num_machines': 1, 'tot_num_mpiprocs': 4 if with_mpi else 1},
                             'withmpi': with_mpi})

    elif "claix" in computer_name_pattern:
        # run a job on cluster: 1 node, xx processors on account
        scheduler_commands = "#SBATCH"
        if account_name:
            scheduler_commands += f" --account={account_name}"
        options = Dict(dict={'max_wallclock_seconds': 60 ** 2,
                             'resources': {'num_machines': 1, 'tot_num_mpiprocs': 24 if with_mpi else 1},
                             'custom_scheduler_commands': scheduler_commands,
                             'withmpi': with_mpi})

    elif "iffslurm" in computer_name_pattern:
        reference = "https://iffgit.fz-juelich.de/aiida/aiida_nodes/-/tree/master/data/groups"
        iffslurm_options_should = [Dict(dict=a_dict) for a_dict in [
            {"withmpi": True, "resources": {"num_machines": 1, "tot_num_mpiprocs": 12},
             "queue_name": "th1", "max_wallclock_seconds": 60 ** 3},
            {"withmpi": True, "resources": {"num_machines": 1, "tot_num_mpiprocs": 64},
             "queue_name": "th1-2020-64", "max_wallclock_seconds": 60 ** 3},
            {"withmpi": True, "resources": {"num_machines": 1, "tot_num_mpiprocs": 20},
             "queue_name": "viti", "max_wallclock_seconds": 60 ** 3},
            {"withmpi": False, "resources": {"num_machines": 1, "tot_num_mpiprocs": 1},
             "queue_name": "oscar", "max_wallclock_seconds": 60 ** 2},
            {"withmpi": False, "resources": {"num_machines": 1, "tot_num_mpiprocs": 1},
             "queue_name": "th1", "max_wallclock_seconds": 60 ** 2},
            {"withmpi": True, "resources": {"num_machines": 1, "tot_num_mpiprocs": 32},
             "queue_name": "th1-2020-32", "max_wallclock_seconds": 60 ** 3},
            {"withmpi": False, "resources": {"num_machines": 1, "tot_num_mpiprocs": 1},
             "queue_name": "th1-2020-32", "max_wallclock_seconds": 60 ** 2},
            {"withmpi": True, "resources": {"num_machines": 1, "tot_num_mpiprocs": 12},
             "queue_name": "oscar", "max_wallclock_seconds": 60 ** 3},
        ]]

        # for iffslurm cluster, got to select partition myself
        computer = get_computer(computer_name_pattern=computer_name_pattern)

        if isinstance(computer, list):
            if not computer:
                from aiida.common.exceptions import NotExistent
                raise NotExistent(f"Found no configured aiida computer matching the pattern '{computer_name_pattern}'.")
            else:
                print(f"WARNING: {get_computer_options.__name__}(): Found more than one matching computers: "
                      f"{computer}. I will select the first one.")
                computer = computer[0]

        if not partition_name:
            partition_name, idle_nodes_count = get_least_occupied_partition(computer=computer, gpu=gpu, silent=silent)
        if not partition_name:
            raise ValueError(f"No partitions found for computer name pattern '{computer_name_pattern}'.")

        # Now, try get aiida_nodes group iffslurm_options
        from aiida.orm import Group
        from aiida.tools.groups import GroupPath

        iffslurm_options_grouppath = GroupPath(path='iffslurm_options')
        iffslurm_options_group, created = iffslurm_options_grouppath.get_or_create_group()
        if created or (len(iffslurm_options_group.nodes) < len(iffslurm_options_should)):
            # here we populate the newly created group with options as defined in aiida_nodes
            # group iffslurm_options.
            iffslurm_options_group.description = "Collection of Dict nodes that contain default options " \
                                                 "(queue_names etc.) for the different nodes of iffslurm."
            iffslurm_options_group.store()
            for option in iffslurm_options_should:
                option.store()
            iffslurm_options_group.add_nodes(iffslurm_options_should)

            if created and not silent:
                print(f"Info: {get_computer_options.__name__}():I created group '{iffslurm_options_group}' and "
                      f"added iffslurm option nodes to it as defined in aiida_nodes, reference: {reference}.")

        # now get the respective option
        from aiida.orm import QueryBuilder
        qb = QueryBuilder()
        qb.append(Group, filters={'label': iffslurm_options_group.label}, tag='group')
        opt_dicts = qb.append(Dict, with_group='group', filters={'and': [{"attributes.queue_name": partition_name},
                                                                         {"attributes.withmpi": with_mpi}]}).first()
        if len(opt_dicts) > 1:
            options = opt_dicts
            print(f"WARNING: {get_computer_options.__name__}(): found several matching computer options for given "
                  f"input computer '{computer_name_pattern}', partition '{partition_name}', with_mpi {with_mpi}. Will "
                  f"return all of them instead of one.")
        elif len(opt_dicts) == 1:
            options = opt_dicts[0]
        else:
            if not silent:
                print(
                    f"Info: {get_computer_options.__name__}(): For computer '{computer_name_pattern}' selected partition "
                    f"'{partition_name}', but could not find an preexisting options Dict. So creating a new one with "
                    f"default conservative values.")
            options = Dict(dict={'max_wallclock_seconds': 60 ** 2,
                                 'resources': {'num_machines': 1, 'tot_num_mpiprocs': 12 if with_mpi else 1},
                                 # 'custom_scheduler_commands': f"#SBATCH --partition {partition_name}",
                                 'queue_name': partition_name,
                                 'withmpi': with_mpi})
    else:
        raise NotImplementedError(f"No options available for computers of type '{computer_name_pattern}'. Available "
                                  f"implementations: {available_implementations}.")

    # DEVNOTE: we're not using deepcopy() here to return dicts only because this is a *stateless* function.
    if isinstance(options, Dict):
        return options if as_Dict else options.attributes
    elif isinstance(options, list):
        return options if as_Dict else [opt.attributes for opt in options]
    else:
        raise ValueError(f"{get_computer_options.__name__}(): something went wrong. Unknown return value.")


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
