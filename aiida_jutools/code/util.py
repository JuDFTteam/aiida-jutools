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
"""Tools for working with aiida Code nodes: utils."""

import copy as _copy
import inspect as _inspect
import typing as _typing

import aiida as _aiida
from aiida import orm as _orm

import aiida_jutools as _jutools



def get_code(computer_name_pattern: str = "",
             code_name_pattern: str = "",
             queue_name: str = "") -> _orm.Code:
    """Find a matching code. If queue_name given, choose code with appropriate architecture.

    All arguments are optional. defaults (empty strings), function will query all codes and choose first found.
    Just try it out with different argument combinations to get a feel for the behavior.

    If queue_name given, and applicable for this computer, this will choose the appropriate code under the assumption
    that different queues (partitions) of the respective computer require the code to be compiled with different
    architecture. For this to work, it is assumed that the code labels either have a substring which specifies the
    computer queue name, or a substring which specifies the architecture.

    All performed substring matches are case-insensitive.

    Architecture detection (AMD/Intel) is performed dynamically by querying the compute cluster.

    :param computer_name_pattern: substring matching some computer label(s)
    :param queue_name: exact name of the computer queue (slurm: partition)
    :param code_name_pattern: substring matching some code label(s)
    :return: closest matching code. if found several, return first, but print all matches
    """


    def _select_codestring_from_filtered(codestrings_by_computer_code: _typing.List[str],
                                         filtered_codestrings: _typing.List[str],
                                         msg_suffix: str = "") -> _typing.Tuple[str, str]:
        """Selects first codestring from filtered if more than one, prints warning/error messages.
        :return: tuple (selected codestring, error_msg). error_msg None if success, else None.
        """
        msg_middle_queue = "" if not queue_name else f", computer queue '{queue_name}'"
        msg_middle = f"for specified computer '{computer_name_pattern}'{msg_middle_queue}, code " \
                     f"name pattern '{code_name_pattern}'{msg_suffix}."

        warning_msg = f"WARNING: '{get_code.__name__}()': Ambiguous codestrings result " \
                      f"{filtered_codestrings} while determining appropriate code {msg_middle} Will choose first " \
                      f"one. Resolve ambiguity by more precise code name pattern."
        all_codestrings = [f"{code.label}@{code.computer.label}" for code in _orm.Code.objects.all()]
        error_msg = f"Could not determine appropriate code " \
                    f"{msg_middle} No match found among all codes with matching computer name / code name pattern: " \
                    f"{codestrings_by_computer_code}. Possible causes: a) Wrong computer-code combination; b) codes " \
                    f"do not have a substring specifying either matching queue (partition) or architecture. " \
                    f"All available codes: {all_codestrings}"

        codestring = None
        cs_filtered = _copy.copy(filtered_codestrings)
        if cs_filtered:  # found at least one match
            if len(cs_filtered) > 1:  # found more than one match

                # # try to narrow the matches list down by stricter matching
                # try again, but this time not with substring match ('in') but full string equality ('==')
                cs_filtered2 = [f"{code.label}@{code.computer.label}" for code in _orm.Code.objects.all() if
                                computer_name_pattern.lower() == code.computer.label.lower()
                                and code_name_pattern.lower() == code.label.lower()]
                if cs_filtered2:
                    if len(cs_filtered2) < len(cs_filtered):
                        cs_filtered = cs_filtered2
                    else:
                        # try again, but this time with case sensitive
                        cs_filtered3 = [f"{code.label}@{code.computer.label}" for code in _orm.Code.objects.all() if
                                        computer_name_pattern == code.computer.label
                                        and code_name_pattern == code.label]
                        if cs_filtered3 and len(cs_filtered3) < len(cs_filtered):
                            cs_filtered = cs_filtered3

            # okay, now take what we have and run with it
            if len(cs_filtered) > 1:
                print(warning_msg)
            codestring = cs_filtered[0]
            error_msg = None

        return codestring, error_msg

    computers = _jutools.computer.get_computers(computer_name_pattern)
    if not computers:
        raise _aiida.common.exceptions.NotExistent(f"No computer '{computer_name_pattern}' found.")
    else:
        computer = computers[0]
        if len(computers) > 1:
            print(
                f"WARNING: For computer name {computer_name_pattern}, found several computers "
                f"{[c.label for c in computers]}. Will choose first one.")

    # get cs = codestrings needed for Code.get_from_string(), filter to desired codes on desired computers
    cs_by_computer_code = [f"{code.label}@{code.computer.label}" for code in _orm.Code.objects.all() if
                           computer_name_pattern.lower() in code.computer.label.lower()
                           and code_name_pattern.lower() in code.label.lower()]

    if not queue_name:
        # Case A): if no queue_name is supplied,

        # first try to get queue_name from computer
        try:
            queue_name = _jutools.computer.get_least_occupied_queue(computer=computer,
                                                                    gpu=None,
                                                                    with_node_count=False,
                                                                    silent=True)
        except NotImplementedError as err:
            # if that failed, only determine by computer_name and code_name_pattern, select first found
            codestring, error_msg = _select_codestring_from_filtered(codestrings_by_computer_code=cs_by_computer_code,
                                                                     filtered_codestrings=cs_by_computer_code)
            if error_msg:
                raise ValueError(error_msg)

    if queue_name:
        # first assume B) that code labels contain queue name for which they were compiled.
        # if that fails, assume C) that code labels contain architecture for which they were compiled,
        # and dynamically determine the architecture by querying the computer cluster.

        # ------------------------------------
        # assume B): code labeled by queue

        cs_by_computer_code_queue = [cs for cs in cs_by_computer_code if queue_name.lower() in cs.lower()]
        codestring, error_msg = _select_codestring_from_filtered(codestrings_by_computer_code=cs_by_computer_code,
                                                                 filtered_codestrings=cs_by_computer_code_queue)
        if error_msg:
            # ---------------------------------------
            # assume C): code labeled by architecture

            # Dynamically determine the architecture for the given queue
            try:
                architecture = _jutools.computer.get_queue_architecture(computer=computer, queue_name=queue_name)
            except NotImplementedError:
                # Architecture detection not implemented for this computer
                raise ValueError(f"Could not determine appropriate code for computer '{computer_name_pattern}', "
                                f"queue '{queue_name}', code name pattern '{code_name_pattern}'. "
                                f"Neither queue name nor architecture detection is available.")
            except ValueError as err:
                # Could not determine architecture (e.g., queue doesn't exist or command failed)
                module_name = _inspect.getmodulename(_inspect.getfile(_jutools.computer.get_queues))
                raise ValueError(f"Could not determine architecture for queue '{queue_name}' on computer "
                                f"'{computer_name_pattern}': {err}. Use '{module_name}.{_jutools.computer.get_queues.__name__}()' "
                                f"to get list of available queues.") from err

            # now find the appropriate code for the given queue based on architecture
            # assume that the codestring (code.label) has info about the architecture
            # (ie, architecture as a substring)
            cs_by_computer_code_arch = [cs for cs in cs_by_computer_code if architecture in cs]

            # now codestring should be unique
            msg_suffix = f"and determined queue architecture '{architecture}'"
            codestring, error_msg = _select_codestring_from_filtered(codestrings_by_computer_code=cs_by_computer_code,
                                                                     filtered_codestrings=cs_by_computer_code_arch,
                                                                     msg_suffix=msg_suffix)
            if error_msg:
                raise ValueError(error_msg)

    return _orm.Code.get_from_string(code_string=codestring)
