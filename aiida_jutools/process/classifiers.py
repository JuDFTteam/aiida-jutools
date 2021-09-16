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
"""Tools for working with AiiDA ``Process`` and ``ProcessNode`` objects: classifiers."""

import datetime as _datetime
import json as _json
import typing as _typing

import aiida as _aiida
from aiida import orm as _orm
from aiida.engine import processes as _aiida_processes
from aiida.tools import groups as _aiida_groups
from masci_tools.util import python_util as _masci_python_util
from plumpy import ProcessState as _PS

import aiida_jutools as _jutools

class ProcessClassifier:
    """Classifies processes by process_state and exit_status.

    Developer's note: TODO: replace internal storage dict with dataframe.
    """
    _TMP_GROUP_LABEL_PREFIX = "process_classification"

    # DEVNOTE TODO: replace internal storage dict with dataframe.
    # Template for this approach, see kkr.KkrConstantsVersionChecker.
    # dataframe should have uuid, ctime, group, ..., process_state, exit_status, process_label, ...
    # then can also replace init parameter group with list of groups.
    # a dataframe is simply much better for sorting, slicing, statistical summaries, visualization of this kind of data.

    def __init__(self,
                 processes: _typing.List[_typing.Union[_orm.ProcessNode, _aiida_processes.Process]] = None,
                 group: _orm.Group = None,
                 id: str = 'uuid'):
        """Classifies processes by process state and optionally other attributes.

        Use e.g. :py:meth:`~.process.query_processes` to get a list of processes to classify.

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
            _jutools.group.delete_groups(group_labels=[group.label for group in temporary_groups],
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
        :py:attr:`~.classified`, a dictionary.

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
            return _jutools.process.query_processes(group=tmp_classification_group,
                                                    process_states=[state]).all(flat=True)

        # container for results
        attr = 'process_state'
        by_attr = {}

        # get all states string representations
        states = _jutools.process.get_process_states(terminated=None,
                                                     as_string=True,
                                                     with_legend=False)
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
            _jutools.group.delete_groups(group_labels=[tmp_classification_group.label],
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
        :py:attr:`~.counted`, a dictionary.

        :return: total count of all classified processes.
        """

        # tmp results storage
        counted = {attr: {} for attr in self._classified}
        total = 0

        # prep for process states subdict
        states = _jutools.process.get_process_states(terminated=None,
                                                     as_string=True,
                                                     with_legend=False)
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
        states = _jutools.process.get_process_states(terminated=True,
                                                     as_string=True,
                                                     with_legend=False)
        finished = _PS.FINISHED.value
        states.pop(states.index(finished))
        total = sum(by_attr.get(state, 0) for state in states)
        if by_attr.get(finished, None):
            total += sum(by_attr[finished].values())
        print(f"\nTotal terminated: {total}.")

        states = _jutools.process.get_process_states(terminated=False,
                                                     as_string=True,
                                                     with_legend=False)
        total = sum(by_attr.get(state, 0) for state in states)
        print(f"Total not terminated: {total}.")
        print()

        # print detailed count
        print(_json.dumps(self.counted, indent=4))

        # print legend
        if with_legend:
            _, legend = _jutools.process.get_process_states(with_legend=True)
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