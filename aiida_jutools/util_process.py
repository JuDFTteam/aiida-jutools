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

import dataclasses as _dc
import datetime as _datetime
import errno as _errno
import io as _io
import json as _json
import shutil as _shutil
import time as _time
import typing as _typing

import aiida as _aiida
import aiida.cmdline.utils.query.calculation as _aiida_cmdline_calculation
import aiida.common.timezone as _aiida_timezone
import aiida.engine as _aiida_engine
import aiida.engine.processes as _aiida_processes
import aiida.orm as _orm
import aiida.tools as _aiida_tools
import aiida.tools.groups as _aiida_groups
import masci_tools.util.python_util as _masci_python_util
import pandas as _pd
from aiida.engine.processes import ProcessState as _PS

import aiida_jutools as _jutools
import aiida_jutools.util_group as _jutools_group


def get_process_states(terminated: bool = None,
                       as_string: bool = True,
                       with_legend: bool = False) -> _typing.Union[_typing.List[str], _typing.List[_PS]]:
    """Get AiiDA process state available string values (of ``process_node.process_state``).

    :param terminated: None: all states. True: all terminated states. False: all not terminated states.
    :param as_string: True: states as string representations. False: states as ProcessState Enum values.
    :param with_legend: add 2nd return argument: string of AiiDA process state classification
    :return: process states
    """
    # first check that ProcessState implementation has not changed
    process_states_should = [_PS.CREATED, _PS.WAITING, _PS.RUNNING, _PS.FINISHED, _PS.EXCEPTED, _PS.KILLED]
    if not set(_PS) == set(process_states_should):
        print(f"WARNING: {get_process_states.__name__}: predefined list of process states {process_states_should} "
              f"does not match {_PS.__name__} states {list(_PS)} anymore. Update code.")

    def states_subset(terminated: bool):
        if terminated is None:
            return list(_PS)
        return [_PS.FINISHED, _PS.EXCEPTED, _PS.KILLED] if terminated else [_PS.CREATED, _PS.WAITING, _PS.RUNNING]

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


def validate_process_states(process_states: _typing.Union[_typing.List[str], _typing.List[_PS]],
                            as_string: bool = True) -> bool:
    """Check if list contains any non-defined process state.

    :param process_states: list of items 'created' 'running' 'waiting' 'finished' 'excepted' 'killed'.
    :param as_string: True: states as string representations. False: states as ProcessState Enum values.
    :return: True if all items are one of the above, False otherwise.
    """
    allowed_process_states = get_process_states(terminated=None, with_legend=False, as_string=as_string)
    return all(ps in allowed_process_states for ps in process_states)


def get_exit_codes(process_cls: _typing.Type[_aiida_processes.Process],
                   as_dict: bool = False) -> _typing.Union[list, dict]:
    """Get collection of all exit codes for this process class.

    An ExitCode is a NamedTuple of exit_status, exit_message and some other things.

    :param process_cls: Process class. Must be subclass of aiida Process
    :param as_dict: return as dict {status : message} instead of as list of ExitCodes.
    :return: list of ExitCodes or dict.
    """
    assert issubclass(process_cls, _aiida_processes.Process)
    exit_codes = list(process_cls.spec().exit_codes.values())
    return exit_codes if not as_dict else {ec.status: ec.message for ec in exit_codes}


def validate_exit_statuses(process_cls: _typing.Type[_aiida_processes.Process],
                           exit_statuses: _typing.List[int] = []) -> bool:
    """Check if list contains any non-defined exit status for this process class.

    :param process_cls: Process class. Must be subclass of aiida Process
    :param exit_statuses: list of integers
    :return: True if all items are defined exit statuses for that process class, False otherwise.
    """
    assert issubclass(process_cls, _aiida_processes.Process)

    exit_codes = get_exit_codes(process_cls=process_cls)
    valid_exit_statuses = [exit_code.status for exit_code in exit_codes]
    exit_statuses_without_0 = [es for es in exit_statuses if es != 0]
    return all(
        exit_status in valid_exit_statuses
        for exit_status in exit_statuses_without_0
    )


def query_processes(label: str = None,
                    process_label: str = None,
                    process_states: _typing.Union[_typing.List[str], _typing.List[_PS]] = None,
                    exit_statuses: _typing.List[int] = None,
                    failed: bool = False,
                    paused: bool = False,
                    node_types: _typing.Union[
                        _typing.List[_orm.ProcessNode], _typing.List[_aiida_processes.Process]] = None,
                    group: _orm.Group = None,
                    timedelta: _datetime.timedelta = None) -> _orm.QueryBuilder:
    """Get all process nodes with given specifications. All arguments are optional.

    ``process_states`` can either be a list of process state strings ('created' 'running' 'waiting' 'finished'
    'excepted' 'killed'), or a list of :py:class:`~aiida.engine.processes.ProcessState` objects. See
    :py:meth:`~aiida_jutools.util_process.get_process_states`.

    Examples:
    >>> from aiida_jutools.util_process import query_processes as qp, get_process_states as ps
    >>> from aiida.orm import WorkChainNode
    >>> import datetime
    >>> process_nodes = qp(label="Au:Cu", process_label='kkr_imp_wc').all(flat=True)
    >>> states = ps(terminated=False)
    >>> num_processes = qp(process_states=states).count()
    >>> process_nodes = qp(node_types=[WorkChainNode], timedelta=datetime.timedelta(days=1)).all(flat=True)

    :param label: node label
    :param process_label: process label. for workflows of plugins, short name of workflow class.
    :param process_states: list of process states.
    :param exit_statuses: list of exit statuses as defined by the process label Process type.
    :param failed: True: Restrict to 'finished' processes with exit_status > 0. Ignore process_states, exit_statuses.
    :param paused: restrict to paused processes.
    :param node_types: list of subclasses of ProcessNode or Process.
    :param group: restrict search to this group.
    :param timedelta: if None, ignore. Else, include only recently created up to timedelta.
    :return: query builder

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

    if not node_types:
        _node_types = [_aiida_processes.Process]
    else:
        _node_types = [typ for typ in node_types if
                       typ is not None and issubclass(typ, (_aiida_processes.Process, _orm.ProcessNode))]
        difference = set(node_types) - set(_node_types)
        if not _node_types:
            _node_types = [_aiida_processes.Process]
        if difference:
            print(f"Warning: {query_processes.__name__}(): Specified node_types {node_types}, some of which are "
                  f"not subclasses of ({_aiida_processes.Process.__name__}, {_orm.ProcessNode.__name__}). "
                  f"Replaced with node_types {_node_types}.")

    filters = {}
    # Use CalculationQueryBuilder (CQB) to build filters.
    # This offers many conveniences, but also limitations. We will deal with the latter manually.
    builder = _aiida_cmdline_calculation.CalculationQueryBuilder()
    if exit_statuses:
        process_states = ['finished']
    filters = builder.get_filters(failed=failed, process_state=process_states, process_label=process_label,
                                  paused=paused)
    if not failed and exit_statuses:  # CQB only offers single exit_status query
        filters['attributes.exit_status'] = {'in': exit_statuses}
    if label:
        filters['label'] = {'==': label}
    if timedelta:
        filters['ctime'] = {'>': _aiida_timezone.now() - timedelta}
    qb = _orm.QueryBuilder()
    if not group:
        return qb.append(_node_types, filters=filters)
    qb.append(_orm.Group, filters={'label': group.label}, tag='group')
    return qb.append(_node_types, with_group='group', filters=filters)


class ProcessClassifier:
    """Classifies processes by process_state and exit_status.

    Developer's note: TODO: replace internal storage dict with dataframe.
    """
    _TMP_GROUP_LABEL_PREFIX = "process_classification"

    # DEVNOTE TODO: replace internal storage dict with dataframe.
    # Template see util_kkr > KkrConstantsVersionChecker
    # dataframe should have uuid, ctime, group, ..., process_state, exit_status, process_label, ...
    # then can also replace init parameter group with list of groups.
    # a dataframe is simply much better for sorting, slicing, statistical summaries, visualization of this kind of data.

    def __init__(self,
                 processes: _typing.List[_typing.Union[_orm.ProcessNode, _aiida_processes.Process]] = None,
                 group: _orm.Group = None,
                 id: str = 'uuid'):
        """Classifies processes by process state and optionally other attributes.

        Use e.g. :py:meth:`~aiida_jutools.util_process.query_processes` to get a list of processes to classify.

        Alternatively, supply a group of process nodes.

        :param processes: list of processes or process nodes to classify.
        :param group: Group of processes. If supplied, ``processes`` list will be ignored.
        :param id: Representation of processes in classification. None: node object. Other options: 'pk', 'uuid', ...
        """

        # reduce 2x2 possible input space to 2.
        if not (processes or group):
            _jutools.logging.log(l=_jutools.logging.LogLevel.ERROR, e=ValueError, o=self, f=self.__init__,
                                 m="I require either a list of processes or a group of processes.")

        # validate processes / group
        self._group_based = group is not None
        self._group = group
        self._unclassified_processes = processes

        if processes and group:
            _jutools.logging.log(l=_jutools.logging.LogLevel.WARNING, o=self, f=self.__init__,
                                 m=f"Supplied both list and group of processes. Parameters are "
                                    f"mutually exclusive. I will take the group and ignore the process list.")
            self._unclassified_processes = None

        # validate id
        self._unique_ids = {None, 'pk', 'uuid'}
        self._nonunique_ids = {'label', 'ctime', 'mtime'}
        self._allowed_ids = self._unique_ids.union(self._nonunique_ids)
        if id not in self._allowed_ids:
            _jutools.logging.log(l=_jutools.logging.LogLevel.WARNING, o=self, f=self.__init__,
                                 m=f"Chosen id '{id}' is not in allowed {self._allowed_ids}. "
                                    f"Will choose id='pk' instead.")
            id = 'pk'
        if id in self._nonunique_ids:
            _jutools.logging.log(l=_jutools.logging.LogLevel.WARNING, o=self, f=self.__init__,
                                 m=f"Chosen id '{id}' is a nonunique id. No override checks will be performed.")
        self._id = id

        # containers for results (public read via @property)
        self._classified = {}
        self._counted = {}

        # check if temporary groups from previous instances have not been cleaned up.
        # if so, delete them.
        qb = _orm.QueryBuilder()
        temporary_groups = qb.append(_orm.Group,
                                     filters={"label": {"like": ProcessClassifier._TMP_GROUP_LABEL_PREFIX + "%"}}).all(
            flat=True)
        if temporary_groups:
            _jutools.logging.log(l=_jutools.logging.LogLevel.INFO, o=self, f=self.__init__,
                                 m=f"Found temporary classification groups, most likely not cleaned up from a "
                                    f"previous {ProcessClassifier.__name__} instance. I will delete them now.")
            _jutools_group.delete_groups(group_labels=[group.label for group in temporary_groups],
                                         skip_nonempty_groups=False,
                                         silent=False)

    @property
    def classified(self) -> dict:
        """Classification result."""
        return self._classified

    @property
    def counted(self) -> dict:
        """Classification count result."""
        return self._counted

    def _group_for_classification(self) -> _orm.Group:
        """Create a temporary group to help in classification. delete group after all classified.

        :return: temporary classification group
        """
        if self._group_based:
            return self._group

        exists_already = True
        tmp_classification_group = None
        # create random group names until found one for which no such group exists already.
        while exists_already:
            group_label = "_".join([ProcessClassifier._TMP_GROUP_LABEL_PREFIX,
                                    _datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                                    _masci_python_util.random_string(length=16)])
            try:
                tmp_classification_group = _orm.Group.get(label=group_label)
            except _aiida.common.exceptions.NotExistent as err:
                tmp_classification_group = _orm.Group(label=group_label)
                tmp_classification_group.store()
                exists_already = False

        tmp_classification_group.add_nodes(nodes=self._unclassified_processes)
        return tmp_classification_group

    def classify(self,
                 type_attr: str = 'process_label'):
        """Run the classification.

        Will run two classifications: 1) by ``process_state`` and 2) optionally by a type attribute.

        For 1), ``finished`` processes will further be subclassified by ``exit_status``.

        For the latter, the default is ``process_label``. Other options are None, ``process_class``, ``process_type``.

        When finished, the classifications becomes available as class attribute
        :py:class:`~aiida_jutools.util_process.ProcessClassifier.classified`, a dictionary.

        :param type_attr: type attribute to use for the type classification.
        """
        _jutools.logging.log(l=_jutools.logging.LogLevel.INFO, o=self, f=self.classify,
                             m=f"Starting classification ...")

        # # classify
        self._classify_by_state()
        self._classify_by_type(type_attr=type_attr)
        # # count results
        total = self._count()

        _jutools.logging.log(l=_jutools.logging.LogLevel.INFO, o=self, f=self.classify,
                             m=f"Classified {total} processes.")

    def _classify_by_state(self) -> None:
        """Classify processes / process nodes (here: interchangeable) by process state.
        """

        tmp_classification_group = self._group_for_classification()

        def _get_processes(state):
            return query_processes(group=tmp_classification_group, process_states=[state]).all(flat=True)

        # container for results
        attr = 'process_state'
        by_attr = {}

        # get all states string representations
        states = get_process_states(terminated=None, as_string=True, with_legend=False)
        finished = _PS.FINISHED.value
        states.pop(states.index(finished))

        # classify by all states except finished
        for state in states:
            processes_of_state = _get_processes(state)
            if self._id:
                by_attr[state] = [getattr(process, self._id) for process in processes_of_state]
            else:
                by_attr[state] = processes_of_state

        # subclassify finished processes by exit_status
        by_attr[finished] = {}
        finished_processes = _get_processes(finished)
        if finished_processes:
            for process in finished_processes:
                exit_status = process.exit_status
                if not by_attr[finished].get(exit_status, None):
                    by_attr[finished][exit_status] = []
                if self._id:
                    by_attr[finished][exit_status].append(getattr(process, self._id))
                else:
                    by_attr[finished][exit_status].append(process)

        # save result
        self._classified[attr] = by_attr

        # cleanup.
        if not self._group_based:
            _jutools_group.delete_groups(group_labels=[tmp_classification_group.label],
                                         skip_nonempty_groups=False,
                                         silent=True)

    def _classify_by_type(self, type_attr: str = 'process_label'):
        """Classify processes by a valid type attribute.

        :param type_attr: type attribute to use for the type classification.
        """
        attr = type_attr
        allowed_attrs = ['process_label', 'process_class', 'process_type']
        if attr not in allowed_attrs:
            _jutools.logging.log(l=_jutools.logging.LogLevel.WARNING, o=self, f=self._classify_by_type,
                                 m=f"Type Attribute {attr} not one of allowed {allowed_attrs}. "
                                    f"Will use 'process_label' instead.")
            attr = 'process_label'

        # container for results
        by_attr = {}

        iterator = self._group.nodes if self._group_based else self._unclassified_processes

        for process in iterator:
            value = getattr(process, attr)
            if value not in by_attr:
                by_attr[value] = []
            if self._id:
                by_attr[value].append(getattr(process, self._id))
            else:
                by_attr[value].append(process)

        # save result
        self._classified[attr] = by_attr

    def _count(self) -> int:
        """Count classified processes.

        When finished, the count becomes available as class attribute
        :py:class:`~aiida_jutools.util_process.ProcessClassifier.counted`, a dictionary.

        :return: total count of all classified processes.
        """

        # tmp results storage
        counted = {attr: {} for attr in self._classified}
        total = 0

        # prep for process states subdict
        states = get_process_states(terminated=None, as_string=True, with_legend=False)
        finished = _PS.FINISHED.value
        states.pop(states.index(finished))

        # start counting
        for attr, by_attr in self._classified.items():
            if attr != 'process_state':
                for key, value in by_attr.items():
                    counted[attr][key] = len(by_attr.get(key, []))
            else:
                # for process states, include all states in account even if not present.
                # so don't iterate over key:value of subdict, but states:value.
                # also, do the total count here. we only need to do it in one subdict.
                for state in states:
                    num = len(by_attr.get(state, []))
                    total += num
                    counted[attr][state] = num
                if by_attr.get(finished, None):
                    # for state finished, have a subsubdict exit_status:processes, so special treatment here.
                    counted[attr][finished] = {exit_status: None for exit_status in by_attr[finished]}
                    for exit_status, processes in by_attr[finished].items():
                        num = len(by_attr[finished].get(exit_status, []))
                        total += num
                        counted[attr][finished][exit_status] = num

        self._counted = counted
        return total

    def print_statistics(self,
                         title: str = "",
                         with_legend: bool = True):
        """Pretty-print classification statistics.

        :param title: A title.
        :param with_legend: True: with process states legend.
        """

        _title = "" if not title else "\n" + title
        print(f"Process classification counts {_title}:")

        # print count of terminated / non-terminated classified processes
        attr = 'process_state'
        by_attr = self.counted[attr]
        states = get_process_states(terminated=True, as_string=True, with_legend=False)
        finished = _PS.FINISHED.value
        states.pop(states.index(finished))
        total = sum(by_attr.get(state, 0) for state in states)
        if by_attr.get(finished, None):
            total += sum(by_attr[finished].values())
        print(f"\nTotal terminated: {total}.")

        states = get_process_states(terminated=False, as_string=True, with_legend=False)
        total = sum(by_attr.get(state, 0) for state in states)
        print(f"Total not terminated: {total}.")
        print()

        # print detailed count
        print(_json.dumps(self.counted, indent=4))

        # print legend
        if with_legend:
            _, legend = get_process_states(with_legend=True)
            print(legend)

    def subgroup_classified_results(self,
                                    group: _orm.Group = None,
                                    require_is_subset: bool = True,
                                    dry_run: bool = True,
                                    silent: bool = False):
        """Subgroup classified processes.

        Adds subgroups to group of classified processes (if it exists) and adds classified process nodes by state.
        Current subgroup classification distinguishes 'finished_ok' ('finished' and exit_status 0)
        and 'failed' (all others).

        :param group: Base group under which to add the subgroups. Will be ignored if group was supplied at
                      initialization.
        :param require_is_subset: True: abort if subgroups already contain processes which are not in group.
        :param dry_run: True: Perform dry run, show what I *would* do.
        :param silent: True: Do not print any information.
        """
        if not (group or self._group_based):
            _jutools.logging.log(l=_jutools.logging.LogLevel.WARNING, o=self, f=self.subgroup_classified_results,
                                 m=f"Missing 'group' argument. I will do nothing.")
            return
        if self._id not in self._unique_ids:
            _jutools.logging.log(l=_jutools.logging.LogLevel.WARNING, o=self, f=self.subgroup_classified_results,
                                 m=f"Chose classification by nonunique id {self._id}. Cannot load unique processes. "
                                    f"I will do nothing.")
            return

        _group = self._group if self._group_based else group

        # check if all unclassified processes are really part of passed-in group.
        iterator = self._group.nodes if self._group_based else self._unclassified_processes
        proc_ids = {proc.uuid for proc in iterator}
        group_ids = {node.uuid for node in _group.nodes}
        if not proc_ids.issubset(group_ids):
            msg_suffix = " You required that they are a subset. I will do nothing." if require_is_subset else ""
            _jutools.logging.log(l=_jutools.logging.LogLevel.WARNING, o=self, f=self.subgroup_classified_results,
                                 m=f"The classified process nodes are not a subset "
                                    f"of the specified group '{_group.label}'.{msg_suffix}")
            if require_is_subset:
                return

        # use the process_state classification result for grouping
        by_attr = self.classified.get('process_state', None)

        if not by_attr:
            _jutools.logging.log(l=_jutools.logging.LogLevel.WARNING, o=self, f=self.subgroup_classified_results,
                                 m="No classification performed. Nothing to subgroup.")
        failed_exit_statuses = []
        finished = _PS.FINISHED.value
        if by_attr.get(finished, None):
            failed_exit_statuses = [exit_status for exit_status in by_attr[finished] if exit_status]

        subgroups_classification = {
            'finished_ok': [{finished: [0]}],
            'failed': [_PS.EXCEPTED.value, _PS.KILLED.value, {finished: failed_exit_statuses}]
        }

        if dry_run:
            # prevent indent of subdicts for better readability
            for subgroup_name, process_states in subgroups_classification.items():
                subgroups_classification[subgroup_name] = [_masci_python_util.NoIndent(process_state) for
                                                           process_state in
                                                           process_states
                                                           if isinstance(process_state, dict)]

            group_info = f"subgroups of group '{_group.label}'" if _group else "groups"
            dump = _json.dumps(subgroups_classification, cls=_masci_python_util.JSONEncoderTailoredIndent, indent=4)
            _jutools.logging.log(l=_jutools.logging.LogLevel.INFO, o=self, f=self.subgroup_classified_results,
                                 m=f"I will try to group classified states into subgroups as follows. In the "
                                    f"displayed dict, the keys are the names of the {group_info} which I will load or "
                                    f"create, while the values depict which sets of classified processes will be added "
                                    f"to that group.\n"
                                    f"{dump}\n"
                                    f"This was a dry run. I will exit now.")
        else:
            if not silent:
                _jutools.logging.log(l=_jutools.logging.LogLevel.INFO, o=self, f=self.subgroup_classified_results,
                                     m=f"Starting subgrouping processes by process state beneath base group "
                                        f"'{_group.label}'...")

            group_path_prefix = _group.label + "/" if _group else ""
            for subgroup_name, process_states in subgroups_classification.items():
                count = 0
                group_path = _aiida_groups.GroupPath(group_path_prefix + subgroup_name)
                subgroup, created = group_path.get_or_create_group()
                for process_state in process_states:
                    if isinstance(process_state, str):
                        processes = by_attr[process_state]
                        if self._id:
                            processes = [_orm.load_node(**{self._id: identifier}) for identifier in processes]
                        subgroup.add_nodes(processes)
                        count += len(processes)
                    if isinstance(process_state, dict):
                        for exit_status in process_state[finished]:
                            processes = by_attr[finished].get(exit_status, [])
                            if self._id:
                                processes = [_orm.load_node(**{self._id: identifier}) for identifier in processes]
                            subgroup.add_nodes(processes)
                            count += len(processes)
                if not silent:
                    _jutools.logging.log(l=_jutools.logging.LogLevel.INFO, o=self, f=self.subgroup_classified_results,
                                         m=f"Added {count} processes to subgroup '{subgroup.label}'")

            if not silent:
                _jutools.logging.log(l=_jutools.logging.LogLevel.INFO, o=self, f=self.subgroup_classified_results,
                                     m=f"Finished subgrouping processes beneath base group '{_group.label}'.")


def find_partially_excepted_processes(processes: _typing.List[_orm.ProcessNode],
                                      to_depth: int = 1) -> _typing.Dict[
    _orm.ProcessNode, _typing.List[_orm.ProcessNode]]:
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
    if to_depth > 1:
        _jutools.logging.log(l=_jutools.logging.LogLevel.ERROR, e=NotImplementedError,
                             f=find_partially_excepted_processes,
                             m="Currently, to_depth > 1 not supported.")  # TODO

    processes_excepted = {}
    for process in processes:
        for child in process.get_outgoing(node_class=_orm.ProcessNode).all_nodes():
            if child.process_state == _PS.EXCEPTED:
                if not processes_excepted.get(process, None):
                    processes_excepted[process] = []
                processes_excepted[process].append(child)

    return processes_excepted


def copy_metadata_options(parent_process: _orm.ProcessNode,
                          builder: _aiida_processes.ProcessBuilder):
    """Copy standard metadata options from parent calc to new calc.

    Reference: https://aiida-kkr.readthedocs.io/en/stable/user_guide/calculations.html#special-run-modes-host-gf-writeout-for-kkrimp

    :param parent_process: finished process
    :param builder: builder of new process
    """
    attr = parent_process.attributes
    builder.metadata.options = {
        'max_wallclock_seconds': attr['max_wallclock_seconds'],
        'resources': attr['resources'],
        'custom_scheduler_commands': attr['custom_scheduler_commands'],
        'withmpi': attr['withmpi']
    }


def verdi_calcjob_outputcat(calcjob: _orm.CalcJobNode) -> str:
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
    :return: string output
    """
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
        output = _io.StringIO()
        with retrieved.open(path, mode='r') as fhandle:
            _shutil.copyfileobj(fhandle, output)
        return output.getvalue()

    except OSError as exception:
        # The sepcial case is breakon pipe error, which is usually OK.
        # It can happen if the output is redirected, for example, to `head`.
        if exception.errno != _errno.EPIPE:
            # Incorrect path or file not readable
            raise ValueError(f'Could not open output path "{path}". Exception: {exception}')


@_dc.dataclass
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
    :param resubmit_failed_as_restart: True: submit get_builder_restarted() from failed instead of builder.
           Default True.
    :param delete_if_stalling: True: delete nodes of 'stalling' top processes. Default True.
    :param delete_if_stalling_dry_run: True: if delete_if_stalling, simulate delete_if_stalling to 'try it out'.
    :param max_wait_for_stalling: delete top process (node & descendants) if running this long. To avoid congestion.
    """
    dry_run: bool = True
    max_top_processes_running: int = 30
    max_all_processes_running: int = 60
    wait_for_submit: int = 5
    max_wait_for_submit: int = 240
    wait_after_submit: int = 2
    resubmit_failed: bool = False
    resubmit_failed_as_restart: bool = False
    delete_if_stalling: bool = False
    delete_if_stalling_dry_run: bool = False
    max_wait_for_stalling: int = 240


class SubmissionSupervisor:
    """Class for supervised process submission to daemon."""

    # TODO check if outdated because of https://github.com/aiidateam/aiida-submission-controller
    def __init__(self,
                 settings: SubmissionSupervisorSettings,
                 quota_querier: _jutools.computer.QuotaQuerier = None):
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

    @property
    def submitted_top_processes(self) -> _typing.List[_orm.ProcessNode]:
        """Database nodes of top processes submitted to the verdi daemon."""
        return self._submitted_top_processes

    def blocking_submit(self,
                        builder: _aiida_processes.ProcessBuilder,
                        groups: _typing.Union[_orm.Group, _typing.List[_orm.Group]] = None) -> _typing.Tuple[
        _orm.ProcessNode, bool]:
        """Submit calculation but wait if more than limit_running processes are running already.

        Note: processes are identified by their label (builder.metadata.label). Meaning: if the supervisor
        finds a process node labeled 'A' in one of the groups with state 'finished_ok' (process state 'finished',
        exit status 0), it will load and return that node instead of submitting.

        Note: if quota_querier is set, computer of main code of builder must be the same as computer set in
        quota_querier. This is not checked as a builder may have several codes using different computers.
        For instance, for workflow kkr_imp_wc, the computer for which the kkrimp code, which is another input
        for the builder, is configured. Ie, in that case, the quota_querier's computer must be the same as
        builder['kkrimp'].computer.

        :param builder: code builder. metadata.label must be set!
        :param groups: restrict to processes in a group or list of groups (optional)
        :return: tuple (next process, is process from db True or from submit False) or (None,None) if submit failed
        """
        self.__tmp_guard_against_delete_if_stalling()

        wc_label = builder.metadata.label
        if not wc_label:
            raise ValueError("builder.metadata.label not set. This method doesn't work without an identifying process"
                             "label.")
        wc_process_label = builder._process_class.get_name()

        # get workchains from group(s)
        if isinstance(groups, _orm.Group):
            groups = [groups]
        workchains = []
        for group in groups:
            workchains.extend(
                query_processes(label=wc_label, process_label=wc_process_label, group=group).all(flat=True))
        # remove duplicates (ie if same wc in several groups)
        _wc_uuids = []

        def _is_duplicate(wc):
            if wc.uuid not in _wc_uuids:
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
            next_process = _orm.load_node(workchains_finished_ok[0].pk)
            print(f"loaded '{wc_label}' from db, finished_ok")
        else:
            # not found A:B in db with state finished_ok. try submitting
            if workchains_terminated and not s.resubmit_failed:
                # found A:B in db with state terminated and not_finished_ok, and no 'resubmit_failed'
                next_process_is_from_db = True
                next_process = _orm.load_node(workchains_terminated[0].pk)
                info = f"process state {next_process.attributes['process_state']}"
                info = info if not next_process.attributes.get('exit_status', None) else \
                    info + f", exit status {next_process.attributes['exit_status']}"
                print(f"loaded '{wc_label}' from db, {info}, (retry modus {s.resubmit_failed})")

            elif workchains_not_terminated:
                # found A:B in db with state not terminated, so it's currently in the queue already
                next_process_is_from_db = False
                next_process = _orm.load_node(workchains_not_terminated[0].pk)
                self._submitted_top_processes.append(next_process)
                print(f"'{wc_label}' is not terminated")

            else:
                # not found A:B in db, so never submitted yet (or deleted since)
                # or found in db not_finished_ok, but terminated, and 'retry'
                # so only option left is submit
                _builder = builder

                info = f"staging submit '{wc_label}' "
                if workchains_terminated and s.resubmit_failed:
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
                            # - python_util.now() as is now measureus UTC. wrong offset (+1 compared to ctime? need
                            # localization first?)
                            # - sometimes it DOES delete nodes, but i'm not sure if it was correct for those.
                            is_stalling = (_masci_python_util.now() - wc.mtime) > _datetime.timedelta(
                                minutes=s.max_wait_for_stalling)
                            # print(f"wc {wc.label} pk {wc.pk} last change time {python_util.now() - wc.mtime}, is
                            # stalling {is_stalling}")
                            if is_stalling:
                                info_msg_suffix = "would now delete its nodes nodes (delete_if_stalling dry run)" \
                                    if s.delete_if_stalling_dry_run else "deleting all its nodes"
                                info_msg = f"INFO: process pk={wc.pk} label='{wc.label}' exceeded max stalling " \
                                           f"time {s.max_wait_for_stalling} min, {info_msg_suffix} ..."
                                print(info_msg)

                                if not s.delete_if_stalling_dry_run:
                                    # note: we do not need to kill the top processnode's process first.
                                    # deleting its nodes will also kill all not terminated connected processes.
                                    _aiida_tools.delete_nodes(pks=[wc.pk], dry_run=False, force=True, verbosity=1)
                            return is_stalling

                        # print("while wait, check if any stalling")
                        self._submitted_top_processes[:] = [
                            wc for wc in self._submitted_top_processes if not stalling(wc)]

                    if num_running(0) > s.max_top_processes_running or num_running(1) > s.max_all_processes_running:
                        # process queue is too full, wait
                        waited_for_submit += s.wait_for_submit  # in minutes
                        _time.sleep(s.wait_for_submit * seconds_per_min)
                    else:
                        # process queue is not too full, can submit
                        print(f"try submit (waited {waited_for_submit} min, "
                              f"queued: {num_running(0)} top, {num_running(1)} all processes; "
                              f"wait another {s.wait_after_submit} minutes after submission)")
                        if not s.dry_run:
                            next_process = _aiida_engine.submit(_builder)
                            self._submitted_top_processes.append(next_process)
                            for group in groups:
                                group.add_nodes([next_process])
                            print(f"submitted {wc_label}, pk {next_process.pk}")
                            _time.sleep(s.wait_after_submit * seconds_per_min)
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


def get_runtime(process_node: _orm.ProcessNode) -> _datetime.timedelta:
    """Get estimate of elapsed runtime.

    Warning: if the process_node has not any callees, node's mtime-ctime is returned.
    This may not be wrong / much too large, eg if mtime changed later due to changed extras.

    :return:  estimate of runtime
    """
    if process_node.called:
        return max(node.mtime for node in process_node.called) - process_node.ctime
        # return max(node.mtime for node in process_node.called_descendants) - process_node.ctime
    return process_node.mtime - process_node.ctime


def get_runtime_statistics(processes) -> _pd.DataFrame:
    return _pd.DataFrame(data=[get_runtime(proc) for proc in processes], columns=['runtime'])
