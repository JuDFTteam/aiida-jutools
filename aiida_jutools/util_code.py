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


def get_code(computer_name_pattern: str = "", code_name_pattern: str = "", queue_name: str = ""):
    """Find a matching code. If queue_name given, choose code with appropriate architecture.

    All arguments are optional. defaults (empty strings), function will query all codes and choose first found.
    Just try it out with different argument combinations to get a feel for the behavior.

    If queue_name given, and applicable for this computer, this will choose the appropriate code under the assumption
    that different queues (partitions) of the respective computer require the code to be compiled with different
    architecture. For this to work, it is assumed that the code labels either have a substring which specifies the
    which specifies the computer queue name, or a substring which specifies the architecture.

    All performed substring matches are case-insensitive.

    Queue_name <-> architecture code matching available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.

    Queue_name <-> architecture code matching available for these architectures:
    - 'intel'
    - 'AMD'

    :param computer_name_pattern: substring matching some computer label(s)
    :param queue_name: exact name of the computer queue (slurm: partition)
    :param code_name_pattern: substring matching some code label(s)
    :return: closest matching code. if found several, return first warn, but print all matches
    :rtype: Code
    """
    from aiida_jutools import util_computer
    from aiida.orm import Code

    def _hardcoded_queue_architectures() -> dict:
        """hardcoded knowledge about architecture of computer queues (slurm: partitions).
        :return: dict of {computer_name : {queue name : architecture name} }
        """
        iffslurm_architecture_queues = {
            'AMD': ['th1-2020-32', 'th1-2020-64', 'th1-2020-gpu'],
            'intel': ['oscar', 'th1', 'th2-gpu', 'viti']
        }

        def _invert(dict_architecture_queues):
            """{architecture : [queue names]} --> {queue_name : architecture}"""
            dict_queue_architecture = {}
            for arch, queues in dict_architecture_queues.items():
                for queue in queues:
                    dict_queue_architecture[queue] = arch
            return dict_queue_architecture

        # gather knowledge for all computers
        queue_architectures = {
            'iffslurm': _invert(iffslurm_architecture_queues)
        }
        return queue_architectures

    def _select_codestring_from_filtered(codestrings_by_computer_code: list, filtered_codestrings: list,
                                         msg_suffix: str = "") -> tuple:
        """Selects first codestring from filtered if more than one, prints warning/error messages.
        :return: tuple (selected codestring, error_msg). error_msg None if success, else None.
        """
        msg_middle_queue = "" if not queue_name else f", computer queue '{queue_name}'"
        msg_middle = f"for specified computer '{computer_name_pattern}'{msg_middle_queue}, code " \
                     f"name pattern '{code_name_pattern}'{msg_suffix}."

        warning_msg = f"WARNING: '{get_code.__name__}()': Ambiguous codestrings result " \
                      f"{filtered_codestrings} while determining appropriate code {msg_middle} Will choose first " \
                      f"one. Resolve ambiguity by more precise code name pattern."
        all_codestrings = [f"{code.label}@{code.computer.label}" for code in Code.objects.all()]
        error_msg = f"Could not determine appropriate code " \
                    f"{msg_middle} No match found among all codes with matching computer name / code name pattern: " \
                    f"{codestrings_by_computer_code}. Possible causes: a) Wrong computer-code combination; b) codes do " \
                    f"not have a substring specifying either matching queue (partition) or architecture. " \
                    f"All available codes: {all_codestrings}"

        codestring = None
        if len(filtered_codestrings) >= 1:
            if len(filtered_codestrings) > 1:
                print(warning_msg)
            codestring = filtered_codestrings[0]
            error_msg = None

        return codestring, error_msg

    computers = util_computer.get_computers(computer_name_pattern)
    if not computers:
        from aiida.common.exceptions import NotExistent
        raise NotExistent(f"No computer '{computer_name_pattern}' found.")
    else:
        computer = computers[0]
        if len(computers) > 1:
            print(
                f"WARNING: For computer name {computer_name_pattern}, found several computers {[c.label for c in computers]}. "
                f"Will choose first one.")

    # get cs = codestrings needed for Code.get_from_string(), filter to desired codes on desired computers
    cs_by_computer_code = [f"{code.label}@{code.computer.label}" for code in Code.objects.all() if
                           computer_name_pattern.lower() in code.computer.label.lower()
                           and code_name_pattern.lower() in code.label.lower()]

    if not queue_name:
        # Case A): if no queue_name is supplied, only determine by computer_name and code_name_pattern
        codestring, error_msg = _select_codestring_from_filtered(codestrings_by_computer_code=cs_by_computer_code,
                                                                 filtered_codestrings=cs_by_computer_code)
        if error_msg:
            raise ValueError(error_msg)
    else:
        # first assume B) that code labels contain queue name for which they were compiled.
        # if that fails, assume C) that code labels contain architecture for which they were compiled,
        # and determine the code from hardcoded knowledge about the computer queues' architecture.

        # ------------------------------------
        # assume B): code labeled by queue

        cs_by_computer_code_queue = [cs for cs in cs_by_computer_code if queue_name.lower() in cs.lower()]
        codestring, error_msg = _select_codestring_from_filtered(codestrings_by_computer_code=cs_by_computer_code,
                                                                 filtered_codestrings=cs_by_computer_code_queue)
        if error_msg:
            # ---------------------------------------
            # assume C): code labeled by architecture

            # check that hardcoded-queues list still corresponds to dynamical one
            computer_queue_architectures = _hardcoded_queue_architectures()
            # The argument computer_name is exact, in order to find the desired Computer. But it might not match
            # the computer label used for hardcoded queue architectures. So first find out which of the
            # computer labels in there is the closest match to computer_name.
            all_computer_labels = list(computer_queue_architectures.keys())
            matching_computer_labels = [label for label in all_computer_labels
                                        if computer_name_pattern.lower() in label.lower()]
            if not matching_computer_labels:
                raise KeyError(f"For computer '{computer_name_pattern}', I have no hardcoded knowledge about "
                               f"queue architectures, only for computers {all_computer_labels}.")
            else:
                matching_computer_label = matching_computer_labels[0]
                computer_queue_architectures = computer_queue_architectures[matching_computer_label]
                if len(matching_computer_labels) > 1:
                    print(f"WARNING: '{get_code.__name__}()': For computer {computer_name_pattern}, "
                          f"found more than one harcoded queue-architecture entry: {matching_computer_labels}. "
                          f"Will choose first one. If this is a problem, contact developer.")

            queues_harcoded = list(computer_queue_architectures.keys())
            queues_queried = util_computer.get_queues(computer=computer, gpu=None, with_node_count=False)
            if not set(queues_harcoded) == set(queues_queried):
                raise ValueError(
                    f"Computer '{computer_name_pattern}' hardcoded queues {queues_harcoded} do not "
                    f"correspond anymore to queried queues {queues_queried}. Update code.")

            # now find the appropriate code for the given queue
            # assume that the codestring (code.label) has info about the architecture
            # (ie, architecture as a substring)
            architecture = computer_queue_architectures.get(queue_name, None)
            if not architecture:
                # since have just that hardcoded queues match actual queues, can conclude
                # that user has specified non-existant queue
                import inspect
                module_name = inspect.getmodulename(inspect.getfile(util_computer.get_queues))
                raise KeyError(f"Computer '{computer_name_pattern}' has no queue '{queue_name}'. Use "
                               f"'{module_name}.{util_computer.get_queues.__name__}()' to get list of queues.")

            cs_by_computer_code_arch = [cs for cs in cs_by_computer_code if architecture in cs]

            # now codestring should be unique
            msg_suffix = f"and determined queue architecture '{architecture}'"
            codestring, error_msg = _select_codestring_from_filtered(codestrings_by_computer_code=cs_by_computer_code,
                                                                     filtered_codestrings=cs_by_computer_code_arch,
                                                                     msg_suffix=msg_suffix)
            if error_msg:
                raise ValueError(error_msg)

    return Code.get_from_string(code_string=codestring)
