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
"""Tools for dealing with computer file system disk quotas."""

import dataclasses as _dc
import io as _io
import typing as _typing

import humanfriendly as _humanfriendly
import pandas as _pd
from aiida import orm as _orm
from masci_tools.util import python_util as _masci_python_util

import aiida_jutools as _jutools


@_dc.dataclass
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
    command: str = "quota"
    header_line_count: int = 1
    column_space_used: str = "used"
    column_space_hard: str = "hard"
    dirname_pattern: str = ""
    min_free_space: str = "10G"
    comment: _typing.List[str] = _masci_python_util.dataclass_default_field([""])


class QuotaQuerier:
    def __init__(self,
                 computer: _orm.Computer,
                 settings: QuotaQuerierSettings):
        """Convenience methods for getting the quota of aiida configured computer nodes.

        Use this class' builder :py:class:`~aiida_jutools.util_computer.QuotaQuerierBuilder` to create an instance.

        :param computer:  an aiida configured computer
        :param settings: settings
        """
        self.computer = computer
        self.settings = settings

    def get_quota(self) -> _pd.DataFrame:
        """Get quota as a pandas dataframe.
        """
        s = self.settings

        exit_code, stdout, stderr = _jutools.computer.shell_command(computer=self.computer,
                                                                    command=s.command)

        # strip away the header lines:
        # # stdout is a single string, lines sep. by '\n'
        for _ in range(s.header_line_count):
            stdout = stdout[stdout.find("\n") + 1:].lstrip()

        return _pd.read_table(_io.StringIO(stdout), sep=r"\s+")

    def is_min_free_space_left(self) -> bool:
        """Check output of quota command on computer to see if more than 'buffer' space is available.

        :return: True if used space > (hard limit - buffer) , False if not.
        """
        s = self.settings

        if not s.dirname_pattern or not s.min_free_space:
            raise ValueError("settings dirname_pattern and/or min_free_space not set!")

        df = self.get_quota()

        # extract row for user dirname we want to get quota of
        row_mask = df.iloc[:, 0].str.contains(s.dirname_pattern)
        row = df[row_mask]
        row_idx = row.index[0]

        # extract bytesize used, hard limit
        used = _humanfriendly.parse_size(row[s.column_space_used][row_idx])
        hard = _humanfriendly.parse_size(row[s.column_space_hard][row_idx])

        # check if free space is big enough
        min_free_space = _humanfriendly.parse_size(s.min_free_space)
        is_min_free_space_left = (hard - used) > min_free_space
        if not is_min_free_space_left:
            print(
                f"not enough space available on computer (used: {used}, hard limit: {hard}, requested free space:"
                f" {s.min_free_space}). Try cleaning up the aiida computer's workdir "
                f"(see verdi calcjob cleanworkdir -h).")
        return is_min_free_space_left


class QuotaQuerierBuilder:
    templates = ["rwth_cluster", "iff_workstation"]

    def __init__(self):
        # check templates
        assert self.templates[0] == "rwth_cluster"
        assert self.templates[1] == "iff_workstation"

    def print_available_templates(self):
        print(self.templates)

    def build(self,
              template: str,
              computer: _orm.Computer) -> QuotaQuerier:
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
