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
"""Tools for working with aiida Code nodes."""


def get_code_for_computer_partition(computer_name: str, partition_name: str, code_name_pattern: str):
    """Get the appropriate code for computer based on given computer partition and code name.

    This will choose the appropriate code under the assumption that different partitions of the respective
    computer require the code to be compiled with different architecture. It is assumed that the code labels
    either have a substring which specifies the which specifies the computer partition name, or a substring
    which specifies the architecture.

    All performed substring matches are case-insensitive.

    Implementations available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.

    Implementations available for these architectures:
    - 'intel'
    - 'AMD'

    :param computer_name: exact computer label
    :param partition_name: name of the computer partition
    :param code_name_pattern: code label substring identifying a subset of codes (eg a 'kkrimp', 'fleur', ...)
    :return: the appropriate code for the specified partition
    :rtype: Code
    """
    from aiida_jutools import util_computer
    from aiida.orm import Code

    computer = util_computer.get_computer(computer_name)
    # convert to codestrings needed for Code.get_from_string(), and
    # filter to desired codes on desired computers
    _codestrings = [f"{code.label}@{code.computer.label}" for code in Code.objects.all() if
                    computer_name.lower() in code.computer.label.lower()
                    and code_name_pattern.lower() in code.label.lower()]

    def get_computer_partition_architectures():
        """hardcoded knowledge about architecture of computer partitions"""
        # for computer 'ifflsurm':
        iffslurm_architecture_partitions = {
            'AMD': ['th1-2020-32', 'th1-2020-64', 'th1-2020-gpu'],
            'intel': ['oscar', 'th1', 'th2-gpu', 'viti']
        }
        # invert dict
        iffslurm_partition_architecture = {}
        for a, ps in iffslurm_architecture_partitions.items():
            for p in ps:
                iffslurm_partition_architecture[p] = a

        # gather knowledge for all computers
        partition_architectures = {
            'iffslurm': iffslurm_partition_architecture
        }
        return partition_architectures

    computer_partition_architectures = get_computer_partition_architectures()

    def _select_codestring_from_filtered(codestrings: list, msg_suffix: str = ""):
        msg_middle = f"for specified computer '{computer_name}', computer partition '{partition_name}', code " \
                     f"name pattern '{code_name_pattern}'{msg_suffix}."
        warning_msg = f"WARNING: Ambiguous codestrings result {codestrings} while determining appropriate code " \
                      f"{msg_middle} Will choose first one."

        error_msg = f"Could not determine appropriate code " \
                    f"{msg_middle} No match found among all codes with matching codename pattern: {_codestrings}. " \
                    f"Reason: codes do not have a substring specifying either matching partition or architecture."
        codestring = None

        if len(codestrings) >= 1:
            if len(codestrings) > 1:
                print(warning_msg)

            codestring = codestrings[0]
            error_msg = None

        return codestring, error_msg

    def _codestring_for_computer_iffslurm(computer_name: str, partition_name: str, code_name_pattern: str):
        # first assume A) that code labels contain partition name for which they were compiled.
        # if that fails, assume B) that code labels contain architecture for which they were compiled,
        # and determine the code from hardcoded knowledge about the computer partitions' architecture.

        # ------------------------------------
        # assume A): code labeled by partition

        codestrings_by_partition = [cs for cs in _codestrings if partition_name.lower() in cs.lower()]
        codestring, error_msg = _select_codestring_from_filtered(codestrings=codestrings_by_partition)
        if error_msg:
            pass  # assume B) instead
        else:
            return codestring

        # ---------------------------------------
        # assume B): code labeled by architecture

        # check that hardcoded-partitions list still corresponds to dynamical one
        partitions_hardcoded = list(computer_partition_architectures[computer_name].keys())
        idle_nodes = util_computer.get_partitions(computer, gpu=None)
        partitions_queried = [pn[0] for pn in idle_nodes]
        if not set(partitions_hardcoded) == set(partitions_queried):
            raise ValueError(
                f"computer '{computer.label}' hardcoded partitions {partitions_hardcoded} do not "
                f"correspond anymore to queried partitions {partitions_queried}. Update code.")

        # now find the appropriate code for the given partition
        # assume that the codestring (code.label) has info about the architecture
        # (ie, architecture as a substring)
        architecture = computer_partition_architectures[computer_name].get(partition_name, None)
        if not architecture:
            # since have just that hardcoded partitions match actual partitions, can conclude
            # that user has specified non-existant partition
            import inspect
            module_name = inspect.getmodulename(inspect.getfile(util_computer.get_partitions))
            raise KeyError(f"Computer '{computer_name}' has no partition '{partition_name}'. Use "
                           f"'{module_name}.{util_computer.get_partitions.__name__}()' to get list of partitions.")

        codestrings_by_architecture = [cs for cs in _codestrings if architecture in cs]

        # now codestring should be unique
        msg_suffix = f"and determined partition architecture '{architecture}'"
        codestring, error_msg = _select_codestring_from_filtered(codestrings=codestrings_by_architecture,
                                                                 msg_suffix=msg_suffix)
        if error_msg:
            raise ValueError(error_msg)
        else:
            return codestring

    # switch-case for different computers
    codestring = None
    if 'iffslurm' in computer.label:
        codestring = _codestring_for_computer_iffslurm(computer_name=computer_name,
                                                       partition_name=partition_name,
                                                       code_name_pattern=code_name_pattern)
    else:

        error_msg = f"{get_code_for_computer_partition.__name__} not implemented for computer '{computer.label}'."
        raise NotImplementedError(error_msg)

    return Code.get_from_string(code_string=codestring)
