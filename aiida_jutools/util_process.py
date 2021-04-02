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
"""Tools for working with aiida Process and ProcessNode objects."""

import dataclasses as dc

from aiida.orm import QueryBuilder

from aiida_jutools.util_computer import QuotaQuerier


def get_process_states(terminated: bool = None, as_string: bool = True, with_legend: bool = False) -> list:
    """Get AiiDA process state available string values (of process_node.process_state).

    :param terminated: None: all states. True: all terminated states. False: all not terminated states.
    :param as_string: True: states as string representations. False: states as ProcessState Enum values.
    :param with_legend: add 2nd return argument: string of AiiDA process state classification
    :return:
    """
    from aiida.engine.processes import ProcessState as PS

    # first check that ProcessState implementation has not changed
    process_states_should = [PS.CREATED, PS.WAITING, PS.RUNNING, PS.FINISHED, PS.EXCEPTED, PS.KILLED]
    if not set(PS) == set(process_states_should):
        from aiida.engine.processes import ProcessState
        print(f"WARNING: {get_process_states.__name__}: predefined list of process states {process_states_should} "
              f"does not match {ProcessState.__name__} states {list(PS)} anymore. Update code.")

    def states_subset(terminated: bool):
        if terminated is None:
            return list(PS)
        return [PS.FINISHED, PS.EXCEPTED, PS.KILLED] if terminated else [PS.CREATED, PS.WAITING, PS.RUNNING]

    states = [ps.value for ps in states_subset(terminated)] if as_string else states_subset(terminated)

    if not with_legend:
        return states
    else:
        legend = """
AiiDA process state hierarchy:
- terminated
    - 'finished'
        - finished_ok ('exit_status' == 0)
        - failed      ('exit_status' >  0)
    - 'excepted'
    - 'killed'
- not terminated
    - 'created'
    - 'waiting'
    - 'running'        
"""
        return states, legend


def validate_process_states(process_states: list = [], as_string:bool=True) -> bool:
    """Check if list contains any non-defined process state.

    :param process_states: list of items 'created' 'running' 'waiting' 'finished' 'excepted' 'killed'.
    :param as_string: True: states as string representations. False: states as ProcessState Enum values.
    :return: True if all items are one of the above, False otherwise.
    """
    allowed_process_states = get_process_states(terminated=None, with_legend=False, as_string=as_string)
    return all([ps in allowed_process_states for ps in process_states])


def get_exit_codes(process_cls) -> list:
    """Get list of all exit codes for this process class.

    An ExitCode is a NamedTuple of exit_status, exit_message and some other things.

    :param process_cls: Process class. Must be subclass of aiida Process
    :return: list of all exit statuses and exit messages of this process class
    :rtype: list of ExitCode objects
    """
    from aiida.engine import Process
    assert issubclass(process_cls, Process)
    return list(process_cls.spec().exit_codes.values())


def validate_exit_statuses(process_cls, exit_statuses: list = []) -> bool:
    """Check if list contains any non-defined exit status for this process class.

    :param process_cls: Process class. Must be subclass of aiida Process
    :param exit_statuses: list of integers
    :return: True if all items are defined exit statuses for that process class, False otherwise.
    """
    from aiida.engine import Process
    assert issubclass(process_cls, Process)

    exit_codes = get_exit_codes(process_cls=process_cls)
    valid_exit_statuses = [exit_code.status for exit_code in exit_codes]
    exit_statuses_without_0 = [es for es in exit_statuses if es != 0]
    return all([exit_status in valid_exit_statuses for exit_status in exit_statuses_without_0])


def query_processes(label: str = None, process_label: str = None, process_states: list = None,
                    exit_statuses: list = None, failed: bool = False, paused: bool = False, node_types: list = None,
                    group=None):
    """Get all process nodes with given specifications. All arguments are optional.

    Examples:
    >>> from aiida_jutools.util_process import query_processes as qp, get_process_states as ps
    >>> qb1 = qp(label="Au:Cu", process_label='kkr_imp_wc')
    >>> states = ps(terminated=False)
    >>> qb2 = qp(process_states=states)

    :param label: node label
    :param process_label: process label. for workflows of plugins, short name of workflow class.
    :param process_states: list of items 'created' 'running' 'waiting' 'finished' 'excepted' 'killed'.
    :param exit_statuses: list of exit statuses as defined by the process label Process type.
    :param failed: True: Restrict to 'finished' processes with exit_status 0. Ignore process_states, exit_statuses.
    :param paused: restrict to paused processes.
    :param node_types: list of subclasses of ProcessNode or Process.
    :param group: restrict search to this group.
    :type group: Group
    :return: query builder
    :rtype: QueryBuilder

    Note: This method doesn't offer projections. Speed and memory-wise, this does not become an
    issue for smaller queries. To test this, measurements were taken of querying a database with ~1e5 nodes for
    ~1e3 WorkChainNodes in it (of process_label 'kkr_imp_wc'), and compared to aiida CalculationQueryBuilder,
    which only does projections (projected one attribute). Results: Speed: no difference (actually this method was
    ~1.5 times faster).  Memory: this method's result took up ~6 MB of memory, while CalculationQueryBuilder's result
    took up ~0.15 KB of memory, so 1/40-th of the size. So this only becomes an issue for querying e.g. for ~1e5 nodes.
    In that case, prefer CalculationQueryBuilder. Memory size measurements of query results were taken with
    python_util.SizeEstimator.sizeof_via_whitelist().

    Note: For kkr workchain queries based on input structures, see util_kkr.

    DEVNOTE: wasmer: filter by process label 'kkr_imp_wc' yields the correct result,
    filter by cls kkr_imp_wc (WorkChain, but qb resolves this) does not. dunno why.
    """
    from aiida.engine import Process
    from aiida.orm import QueryBuilder, ProcessNode, Group
    from aiida.cmdline.utils.query.calculation import CalculationQueryBuilder

    if not node_types:
        _node_types = [Process]
    else:
        _node_types = [typ for typ in node_types if typ is not None and issubclass(typ, (Process, ProcessNode))]
        difference = set(node_types) - set(_node_types)
        if not _node_types:
            _node_types = [Process]
        if difference:
            print(f"Warning: {query_processes.__name__}(): Specified node_types {node_types}, some of which are "
                  f"not subclasses of ({Process.__name__}, {ProcessNode.__name__}). Replaced with node_types "
                  f"{_node_types}.")

    filters = {}
    # Use CalculationQueryBuilder (CQB) to build filters.
    # This offers many conveniences, but also limitations. We will deal with the latter manually.
    builder = CalculationQueryBuilder()
    if exit_statuses:
        process_states = ['finished']
    filters = builder.get_filters(failed=failed, process_state=process_states, process_label=process_label,
                                  paused=paused)
    if not failed and exit_statuses:  # CQB only offers single exit_status query
        filters['attributes.exit_status'] = {'in': exit_statuses}
    if label:
        filters['label'] = {'==': label}

    qb = QueryBuilder()
    if not group:
        return qb.append(_node_types, filters=filters)
    else:
        qb.append(Group, filters={'label': group.label}, tag='group')
        return qb.append(_node_types, with_group='group', filters=filters)


class ProcessClassifier:
    def __init__(self):
        """Classifies processes by process_state and exit_status.
        """
        self.classified_processes = {}
        self.group = None
        self._temporary_groups = []

    def classify_by_state(self, processes: list = None, process_label: str = None, group=None,
                          types: list = None) -> dict:
        """Classify processes / process nodes (here: interchangeable) by process state.

        Result stored as class variable. It is a dict process_state : processes. process_state 'finished' is further
        subdivided into exit_status : processes.

        :param processes: list of processes. If None, will find matching processes by query from other arguments supplied.
        :param process_label: process label. for workflows of plugins, short name of workflow class.
        :param group: restrict search to this group. Default None.
        :type group: Group
        :param types: List of subclasses of ProcessNode. If None, choose [ProcessNode].
        :return class attribute classified_processes.
        """
        from aiida.orm import ProcessNode

        if not processes and not group:
            raise ValueError("Error: must either specify a list of processes or a group with process nodes.")

        if not processes:
            self.group = group
        else:
            # create a temporary group and work with that. delete afterwards.
            # (reason1: need some way to memorize processses across instance methods.
            # (reason2: can use same group-based code for both cases processes=list and processes=None.)
            # However, if group already exists (from previous call), use that one.
            create_group = False
            if self.group:
                # check that group contains exactly the specified processes.
                group_ids = set([node.uuid for node in self.group.nodes if isinstance(node, ProcessNode)])
                create_group = set([node.uuid for node in processes]) != group_ids

            if not self.group or create_group:
                from aiida.tools.groups import GroupPath
                from masci_tools.util import python_util
                from datetime import datetime

                created = False
                while not created:
                    group_label = "_".join(["process_classification",
                                            datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                                            python_util.random_string(length=16)])
                    group_path = GroupPath(path=group_label)
                    self.group, created = group_path.get_or_create_group()
                self._temporary_groups.append(self.group)
                print(f"Info: {ProcessClassifier.__name__}.{ProcessClassifier.classify_by_state.__name__}(): "
                      f"created temporary classification group {self.group}. To delete, call "
                      f"{ProcessClassifier.__name__}.{ProcessClassifier.cleanup.__name__}().")
                self.group.add_nodes(nodes=processes)

        _types = types if types else []

        def get_processes(state):
            return query_processes(group=self.group, process_label=process_label,
                                   process_states=[state], node_types=types).all(flat=True)

        self.classified_processes = {}

        from aiida.engine.processes import ProcessState as PS
        states = get_process_states(terminated=None)
        finished = PS.FINISHED.value
        states.pop(states.index(finished))

        for state in states:
            processes_of_state = get_processes(state)
            self.classified_processes[state] = processes_of_state if processes_of_state else []

        self.classified_processes[finished] = {}
        finished_processes = get_processes(finished)
        if finished_processes:
            for process in finished_processes:
                exit_status = process.exit_status
                if not self.classified_processes[finished].get(exit_status, None):
                    self.classified_processes[finished][exit_status] = [process]
                else:
                    self.classified_processes[finished][exit_status].append(process)

        return self.classified_processes

    def count(self, process_states: list = None) -> int:
        """Count processes classified under specified process states.

        :param process_states: process states. keys of classified processes dict. If None, count all.
        :return: sum of counts of processes of specified process states.
        """
        _process_states = process_states if process_states \
            else get_process_states(terminated=None, as_string=True, with_legend=False)

        total = 0
        for process_state in _process_states:
            if not process_state == 'finished':
                total += len(self.classified_processes.get(process_state, []))
            else:
                if self.classified_processes.get(process_state, None):
                    total += sum([len(v) for v in self.classified_processes[process_state].values()])
        return total

    def print_statistitics(self, with_legend: bool = True):
        """Pretty-print classification statistics.

        :param with_legend: True: with process states legend.
        """
        import json

        if with_legend:
            _, legend = get_process_states(with_legend=True)
            print(legend)

        print("Process classification statistics:")

        print(f"\nTotal terminated: {self.count(['excepted', 'killed', 'finished'])}.")
        print(f"Total not terminated: {self.count(['created', 'waiting', 'running'])}.")

        print(f"\nFull classification:")
        statistics = {process_state: len(processes) for process_state, processes in self.classified_processes.items()
                      if process_state != 'finished'}
        if self.classified_processes.get('finished', None):
            statistics['finished'] = {exit_status: len(processes)
                                      for exit_status, processes in self.classified_processes['finished'].items()}
        print(json.dumps(statistics, indent=4))

    def subgroup_classified_results(self, dry_run: bool = True, silent: bool = False):
        """Subgroup classified processes.

        Adds subgroups to classification group and adds classified process nodes by state.
        Current subgroup classification distinguishes 'finished_ok' ('finished' and exit_status 0)
        and 'failed' (all others).

        :param dry_run: True: Perform dry run, show what I *would* do.
        :param silent: True: Do not print any information.
        """
        if not self.classified_processes:
            print("INFO: No classification performed. Nothing to subgroup.")
        failed_exit_states = []
        if self.classified_processes.get('finished', None):
            failed_exit_states = [exit_status for exit_status in self.classified_processes['finished'] if exit_status]

        subgroups_classification = {
            'finished_ok': [{'finished': [0]}],
            'failed': ['excepted', 'killed', {'finished': failed_exit_states}]
        }

        if dry_run:
            from masci_tools.util.python_util import JSONEncoderTailoredIndent, NoIndent
            # prevent indent of subdicts for better readability
            for subgroup_name, process_states in subgroups_classification.items():
                subgroups_classification[subgroup_name] = [NoIndent(process_state) for process_state in process_states
                                                           if isinstance(process_state, dict)]

            group_info = f"subgroups of group {self.group.label}" if self.group else "groups"
            print(f"INFO: I will try to group classified states into subgroups as follows. In the displayed dict, the "
                  f"keys are the names of the '{group_info}' which I will load or create, while the values depict "
                  f"which sets of classified processes will be added to that group.")
            import json
            print(json.dumps(subgroups_classification, cls=JSONEncoderTailoredIndent, indent=4))
            print("This was a dry run. I will exit now.")
        else:
            from aiida.tools.groups import GroupPath
            group_path_prefix = self.group.label + "/" if self.group else ""
            for subgroup_name, process_states in subgroups_classification.items():
                count = 0
                group_path = GroupPath(group_path_prefix + subgroup_name)
                subgroup, created = group_path.get_or_create_group()
                for process_state in process_states:
                    if isinstance(process_state, str):
                        processes = self.classified_processes[process_state]
                        subgroup.add_nodes(processes)
                        count += len(processes)
                    if isinstance(process_state, dict):
                        for exit_status in process_state['finished']:
                            processes = self.classified_processes['finished'].get(exit_status, [])
                            subgroup.add_nodes(processes)
                            count += len(processes)
                if not silent:
                    print(f"Added {count} processes to subgroup '{subgroup.label}'")

    def cleanup(self):
        """Perform cleanup.

        - If temporary groups were created during classification, delete them (nodes remain).
        """
        from aiida_jutools import util_group
        util_group.delete_groups(group_labels=[group.label for group in self._temporary_groups],
                                 skip_nonempty_groups=False,
                                 silent=False)


def find_partially_excepted_processes(processes: list, to_depth: int = 1) -> dict:
    """Filter out processes with excepted descendants.

    Here, 'partially excepted' is defined as 'not itself excepted, but has excepted descendants'.

    Currently, to_depth > 1 not supported.

    Use case: Sometimes a workchain is stored as e.g. waiting or finished, but a descendant process (node) of it
    has excepted (process_state 'excepted'). For some downstream use cases, this workchain is then useless (for
    example, sometimes, export/import). This function helps to filter out such processes (from a list of processes
    e.g. retrieved via query_processes()) for further investigation or deletion.

    :param processes: list of process nodes.
    :param to_depth: Descend down descendants to this depth.
    :return: dict of process : list of excepted descendants.
    """
    from aiida.engine.processes import ProcessState as PS
    from aiida.orm import ProcessNode

    if to_depth > 1:
        raise NotImplementedError("Currently, to_depth > 1 not supported.")  # TODO

    processes_excepted = {}
    for process in processes:
        for child in process.get_outgoing(node_class=ProcessNode).all_nodes():
            if child.process_state == PS.EXCEPTED:
                if not processes_excepted.get(process, None):
                    processes_excepted[process] = []
                processes_excepted[process].append(child)

    return processes_excepted


def copy_metadata_options(parent_calc, builder):
    """Copy standard metadata options from parent calc to new calc.

    Reference: https://aiida-kkr.readthedocs.io/en/stable/user_guide/calculations.html#special-run-modes-host-gf-writeout-for-kkrimp

    :param parent_calc: performed calculation
    :type parent_calc: ProcessNode
    :param builder: new calculation
    :type builder: ProcessBuilder
    """
    attr = parent_calc.attributes
    builder.metadata.options = {
        'max_wallclock_seconds': attr['max_wallclock_seconds'],
        'resources': attr['resources'],
        'custom_scheduler_commands': attr['custom_scheduler_commands'],
        'withmpi': attr['withmpi']
    }


def verdi_calcjob_outputcat(calcjob) -> str:
    """Equivalent of verdi calcjob otuputcat NODE_IDENTIFIER

    Note: Apparently same as calling

    .. highlight:: python
    .. code-block:: python

        calcjob_node.outputs.retrieved.get_object_content('aiida.out')

    ```

    But the above can fail when this method here doesn't.

    Note: in an IPython environment, you can also use the capture magic instead of this function:

    .. highlight:: python
    .. code-block:: python

        %%capture output
        !verdi calcjob outputcat NODE_IDENTIFIER

    ```

    Then in the next cell, can call output(), output.stdout or output.stderr.

    Note: you can also call !verdi command >> filename, then read file.

    References: https://groups.google.com/g/aiidausers/c/Zvrk-3lFWd8

    :param calcjob: calcjob
    :type calcjob: CalcJobNode
    :return: string output
    """

    from shutil import copyfileobj
    from io import StringIO
    import errno

    try:
        retrieved = calcjob.outputs.retrieved
    except AttributeError:
        raise ValueError("No 'retrieved' node found. Have the calcjob files already been retrieved?")

        # Get path from the given CalcJobNode if not defined by user
    path = calcjob.get_option('output_filename')

    # Get path from current process class of CalcJobNode if still not defined
    if path is None:
        fname = calcjob.process_class.spec_options.get('output_filename')
        if fname and fname.has_default():
            path = fname.default

    if path is None:
        # Still no path available
        raise ValueError(
            '"{}" and its process class "{}" do not define a default output file '
            '(option "output_filename" not found).\n'
            'Please specify a path explicitly.'.format(calcjob.__class__.__name__, calcjob.process_class.__name__)
        )

    try:
        # When we `cat`, it makes sense to directly send the output to stdout as it is
        output = StringIO()
        with retrieved.open(path, mode='r') as fhandle:
            copyfileobj(fhandle, output)
        return output.getvalue()

    except OSError as exception:
        # The sepcial case is breakon pipe error, which is usually OK.
        # It can happen if the output is redirected, for example, to `head`.
        if exception.errno != errno.EPIPE:
            # Incorrect path or file not readable
            raise ValueError(f'Could not open output path "{path}". Exception: {exception}')


@dc.dataclass
class SubmissionSupervisorSettings:
    """Settings for SubmissionSupervisor. Use e.g. in a loop of many submissions.

    Time unit of 'wait' attributes is minutes.

    :param dry_run: True: don't submit, simulate with secondss_per_min=1 instead of 60
    :param max_top_processes_running: wait in line if surpassed: top processes (type of called builder)
    :param max_all_processes_running: wait in line if surpassed: all processes (top & children)
    :param wait_for_submit: interval to recheck if line is free now
    :param max_wait_for_submit: max time to wait in line, give up afterwards
    :param wait_after_submit: if could submit, wait this long until returning
    :param resubmit_failed: True: if found failed process of same label in group, resubmit. Default False.
    :param resubmit_failed_as_restart: True: submit get_builder_restarted() from failed instead of builder. Default True.
    :param delete_if_stalling: True: delete nodes of 'stalling' top processes. Default True.
    :param delete_if_stalling_dry_run: True: if delete_if_stalling, simulate delete_if_stalling to 'try it out'.
    :param max_wait_for_stalling: delete top process (node & descendants) if running this long. To avoid congestion.
    """
    dry_run: bool = False
    max_top_processes_running: int = 10
    max_all_processes_running: int = 100
    wait_for_submit: int = 5
    max_wait_for_submit: int = 120
    wait_after_submit: int = 2
    resubmit_failed: bool = True
    resubmit_failed_as_restart: bool = True
    delete_if_stalling: bool = False
    delete_if_stalling_dry_run: bool = False
    max_wait_for_stalling: int = 240


class SubmissionSupervisor:
    def __init__(self, settings: SubmissionSupervisorSettings, quota_querier: QuotaQuerier = None):
        """Class for supervised process submission to daemon.

        :param settings: supervisor settings
        :param quota_querier: computer quota querier for main code. Optional.
        """
        # set settings
        self.settings = settings
        self.__tmp_guard_against_delete_if_stalling()
        self.quotaq = quota_querier
        # queue for submitted workchains, k=wc, v=run_time in min
        self._submitted_top_processes = []

    def blocking_submit(self, builder, groups=None):
        """Submit calculation but wait if more than limit_running processes are running already.

        Note: processes are identified by their label (builder.metadata.label). Meaning: if the supervisor
        finds a process node labeled 'A' in one of the groups with state 'finished_ok', it will load and return
        that node instead of submitting.

        Note: if quota_querier is set, computer of main code of builder must be the same as computer set in
        quota_querier. This is not checked as a builder may have several codes using different computers.
        For instance, for workflow kkr_imp_wc, the computer for which the kkrimp code, which is another input
        for the builder, is configured. Ie, in that case, the quota_querier's computer must be the same as
        builder['kkrimp'].computer.

        :param builder: code builder. metadata.label must be set!
        :type builder: ProcessBuilder
        :param groups: restrict to processes in a group or list of groups (optional)
        :type group: Group or list of Group
        :return: tuple (next process, is process from db True or from submit False) or (None,None) if submit failed
        :rtype: tuple(ProcessNode, bool)
        """
        import time
        from datetime import timedelta
        from aiida.orm import load_node, Group
        from aiida.engine import submit
        from aiida.manage.database.delete.nodes import delete_nodes
        from masci_tools.util import python_util

        self.__tmp_guard_against_delete_if_stalling()

        wc_label = builder.metadata.label
        if not wc_label:
            raise ValueError("builder.metadata.label not set. This method doesn't work without an identifying process"
                             "label.")
        wc_process_label = builder._process_class.get_name()

        # get workchains from group(s)
        if isinstance(groups, Group):
            groups = [groups]
        workchains = []
        for group in groups:
            workchains.extend(
                query_processes(label=wc_label, process_label=wc_process_label, group=group).all(flat=True))
        # remove duplicates (ie if same wc in several groups)
        _wc_uuids = []

        def _is_duplicate(wc):
            if not wc.uuid in _wc_uuids:
                _wc_uuids.append(wc.uuid)
                return False
            return True

        workchains[:] = [wc for wc in workchains if not _is_duplicate(wc)]
        # classify db workchains by process state
        workchains_finished_ok = [proc for proc in workchains if proc.is_finished_ok]  # finished and exit status 0
        workchains_terminated = [proc for proc in workchains if proc.is_terminated]  # finished, excepted or killed
        workchains_not_terminated = [proc for proc in workchains if
                                     not proc.is_terminated]  # created, waiting or running

        if len(workchains) > 1:
            print(
                f"INFO: '{wc_label}': found multiple ({len(workchains)}) results in group(s) "
                f"{[group.label for group in groups]}, pks: {[wc.pk for wc in workchains]}")

        # handle for settings
        s = self.settings
        seconds_per_min = 1 if s.dry_run else 60

        def num_running(granularity: int):
            if granularity == 0:  # top processes
                return query_processes(process_label=wc_process_label,
                                       process_states=get_process_states(terminated=False)).count()
            if granularity == 1:  # all processes
                return query_processes(process_states=get_process_states(terminated=False)).count()

        # load or submit workchain/calc
        # (in the following comments, 'A:B' is used as exemplary wc_label value)
        if workchains_finished_ok:
            # found A:B in db and finished_ok
            next_process_is_from_db = True
            next_process = load_node(workchains_finished_ok[0].pk)
            print(f"loaded '{wc_label}' from db, finished_ok")
        else:
            # not found A:B in db with state finished_ok. try submitting
            if workchains_terminated and not s.resubmit_failed:
                # found A:B in db with state terminated and not_finished_ok, and no 'retry'
                next_process_is_from_db = True
                next_process = load_node(workchains_terminated[0].pk)
                info = f"process state {next_process.attributes['process_state']}"
                info = info if not next_process.attributes.get('exit_status', None) else \
                    info + f", exit status {next_process.attributes['exit_status']}"
                print(f"loaded '{wc_label}' from db, {info}, (retry modus {s.resubmit_failed})")

            elif workchains_not_terminated:
                # found A:B in db with state not terminated, so it's currently in the queue already
                next_process_is_from_db = False
                next_process = load_node(workchains_not_terminated[0].pk)
                self._submitted_top_processes.append(next_process)
                print(f"'{wc_label}' is not terminated")

            else:
                # not found A:B in db, so never submitted yet (or deleted since)
                # or found in db not_finished_ok, but terminated, and 'retry'
                # so only option left is submit
                _builder = builder

                info = f"staging submit '{wc_label}' "
                if s.resubmit_failed:
                    info_failed = [f"pk {wc.pk}, state {wc.attributes['process_state']}, " \
                                   f"exit status {wc.attributes.get('exit_status', None)}"
                                   for wc in workchains_terminated]
                    info += f", resubmit (previously failed: {info_failed})"
                    if s.resubmit_failed_as_restart:
                        wc_failed_first = workchains_terminated[0]
                        _builder = workchains_terminated[0].get_builder_restart()

                        # some things are not copied from the original builder, such as
                        # node label and description. so do that manually.
                        _builder.metadata.label = builder.metadata.label
                        _builder.metadata.description = builder.metadata.description

                        info += f", restart from first found previously failed, pk={wc_failed_first.pk}"

                        # check that supplied builder metadata correspond with found workchain
                        if (builder.metadata.label != wc_failed_first.label) or (
                                builder.metadata.description != wc_failed_first.description):
                            info += f"(WARNING: label, description supplied via builder ('{builder.metadata.label}', " \
                                    f"{builder.metadata.description}) do not correspond to label, description from " \
                                    f"first found previously failed ('{wc_failed_first.label}', " \
                                    f"{wc_failed_first.description}). Will use those supplied via builder.))"
                info += " ..."
                print(info)

                submitted = False

                if self.quotaq and not self.quotaq.is_min_free_space_left():
                    raise IOError(f"Abort: not enough free space {self.quotaq.settings.min_free_space} "
                                  f"left on remote workdir. Check this object's quotaq.")

                waited_for_submit = 0
                while waited_for_submit <= s.max_wait_for_submit:
                    # entered submit waiting line (length of one workchain with many subprocesses)
                    if s.delete_if_stalling or (not s.delete_if_stalling and s.delete_if_stalling_dry_run):
                        # delete stalling nodes and remove from watch queue
                        def stalling(wc):
                            # DEVNOTE TODO: measuring stalling time via delta= python_util.now()-wc.time does not work
                            #               as expected. It SHOULD delete all top processes that appear in verdi process
                            #               list, with time in verdi process list > max_stalling_time. Instead:
                            # - at every new blocking submit, all wc's deltas are back to zero.
                            # - python_util.now() as is now measureus UTC. wrong offset (+1 compared to ctime? need localization first?)
                            # - sometimes it DOES delete nodes, but i'm not sure if it was correct for those.
                            is_stalling = (python_util.now() - wc.mtime) > timedelta(minutes=s.max_wait_for_stalling)
                            # print(f"wc {wc.label} pk {wc.pk} last change time {python_util.now() - wc.mtime}, is stalling {is_stalling}")
                            if is_stalling:
                                info_msg_suffix = "would now delete its nodes nodes (delete_if_stalling dry run)" \
                                    if s.delete_if_stalling_dry_run else "deleting all its nodes"
                                info_msg = f"INFO: process pk={wc.pk} label='{wc.label}' exceeded max stalling " \
                                           f"time {s.max_wait_for_stalling} min, {info_msg_suffix} ..."
                                print(info_msg)

                                if not s.delete_if_stalling_dry_run:
                                    # note: we do not need to kill the top processnode's process first.
                                    # deleting its nodes will also kill all not terminated connected processes.
                                    delete_nodes(pks=[wc.pk], dry_run=False, force=True, verbosity=1)
                            return is_stalling

                        # print("while wait, check if any stalling")
                        self._submitted_top_processes[:] = [
                            wc for wc in self._submitted_top_processes if not stalling(wc)]

                    if num_running(0) > s.max_top_processes_running or num_running(1) > s.max_all_processes_running:
                        # process queue is too full, wait
                        waited_for_submit += s.wait_for_submit  # in minutes
                        time.sleep(s.wait_for_submit * seconds_per_min)
                    else:
                        # process queue is not too full, can submit
                        print(f"try submit (waited {waited_for_submit} min, "
                              f"queued: {num_running(0)} top, {num_running(1)} all processes; "
                              f"wait another {s.wait_after_submit} minutes after submission)")
                        if not s.dry_run:
                            next_process = submit(_builder)
                            self._submitted_top_processes.append(next_process)
                            for group in groups:
                                group.add_nodes([next_process])
                            print(f"submitted {wc_label}, pk {next_process.pk}")
                            time.sleep(s.wait_after_submit * seconds_per_min)
                        else:
                            print(f"dry_run: would now submit {wc_label}")
                            next_process = None
                        # submitted. exit waiting line
                        submitted = True
                        break
                next_process_is_from_db = False

                if not submitted:
                    print(f"WARNING: submission of '{wc_label}' timed out after {waited_for_submit} min waiting time.")
                    next_process, next_process_is_from_db = None, None
        return next_process, next_process_is_from_db

    def __tmp_guard_against_delete_if_stalling(self):
        """Setting delete_if_stalling is currently not safe, so guard against it.
        DEVNOTE: TODO see resp. DEVNOTEs in blocking_submit()
        """
        if self.settings.delete_if_stalling:
            print(f"WARNING: {SubmissionSupervisorSettings.__name__}.delete_if_stalling=True is currently "
                  f"not supported. Will instead set delete_if_stalling_dry_run=True to show what the setting "
                  f"*would* do.")
            self.settings.delete_if_stalling = False
            self.settings.delete_if_stalling_dry_run = True


def get_runtime(process_node):
    """Get estimate of elapsed runtime.

    Warning: if the process_node has not any callees, node's mtime-ctime is returned.
    This may not be wrong / much too large, eg if mtime changed later due to changed extras.

    :return:  estimate of runtime
    :rtype: datetime.timedelta
    """
    if process_node.called:
        return max([node.mtime for node in process_node.called]) - process_node.ctime
        # return max([node.mtime for node in process_node.called_descendants]) - process_node.ctime
    return process_node.mtime - process_node.ctime
    # wc.outputs.workflow_info.ctime - wc.ctime


def get_runtime_statistics(processes):
    import pandas as pd
    return pd.DataFrame(data=[get_runtime(proc) for proc in processes], columns=['runtime'])
