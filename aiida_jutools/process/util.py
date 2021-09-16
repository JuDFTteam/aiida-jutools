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
"""Tools for working with AiiDA ``Process`` and ``ProcessNode`` objects: utils."""

import datetime as _datetime
import errno as _errno
import shutil as _shutil
import typing as _typing

import pandas as _pd
from aiida import orm as _orm
from aiida.cmdline.utils.query import calculation as _aiida_cmdline_calculation
from aiida.common import timezone as _aiida_timezone
from aiida.engine import processes as _aiida_processes
from plumpy import ProcessState as _PS

import aiida_jutools as _jutools
import io as _io


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
    :py:func:`~.get_process_states`.

    Examples:

    >>> import aiida_jutools as jutools
    >>> from aiida.orm import WorkChainNode
    >>> import datetime
    >>> process_nodes = jutools.process.query_processes(label="Au:Cu", process_label='kkr_imp_wc').all(flat=True)
    >>> states = jutools.process.get_process_states(terminated=False)
    >>> num_processes = jutools.process.query_processes(process_states=states).count()
    >>> process_nodes = jutools.process.query_processes(node_types=[WorkChainNode],
    ...                                                 timedelta=datetime.timedelta(days=1)).all(flat=True)

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

    Note: For kkr workchain queries based on input structures, see :py:func:`~.kkr.query_kkr_wc`.

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