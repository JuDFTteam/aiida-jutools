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

# aiida imports
from aiida.orm import Computer, Dict, Group


def get_computers(computer_name_pattern: str = ""):
    """Query computer.

    :param computer_name_pattern: (sub)string of computer name, case-insensitive, no regex. default = "": get all computers.
    :return: aiida Computer if unique, list of Computers if not, empty list if no match
    """
    from aiida.orm import QueryBuilder
    # version compatibility check: old: computer.name, new: computer.label. else error.
    qb = QueryBuilder()
    computer = None
    computers = qb.append(Computer, filters={'name': {'ilike': f"%{computer_name_pattern}%"}}).all(flat=True)
    return computers


def shell_command(computer: Computer, command: str) -> tuple:
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


def get_queues(computer: Computer, gpu: bool = None, with_node_count: bool = True, silent: bool = False) -> list:
    """Get list of the remote computer (cluster's) queues (slurm: partitions) sorted by highest number of idle nodes descending.

    Implementations available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.

    :param computer: aiida computer.
    :type computer: Computer
    :param gpu: False: exclude gpu queues. True exclude non-gpu partitions. None: ignore this option.
    :param with_node_count: True: return queue names with resp. idle nodes count, False: just queue names.
    :param silent: True: do not print out any info.
    :return: list of [queue/partition name, idle nodes count] or of queue names
    :raise: NotImplementedError if get queues not implemented for that type (by label substring) of computer.

    DEVNOTES: TODO: replace filter by shell command with sinfo -> pandas.Dataframe -> apply filters.
    """
    if 'iffslurm' in computer.label:

        iffslurm_cmd_sorted_partitions_list = """{ for p in $(sinfo --noheader --format="%R"); do echo "$p $(sinfo -p "${p}" --noheader --format="%t %n" | awk '$1 == "idle"' | wc -l)"; done } | sort -k 2 -n -r"""
        iffslurm_partition_max_idle_nodes = """{ for p in $(sinfo --noheader --format="%R"); do echo "$p $(sinfo -p "${p}" --noheader --format="%t %n" | awk '$1 == "idle"' | wc -l)"; done } | sort -k 2 -n -r | awk 'NR == 1 { print $1 }'"""

        exit_code, stdout, stderr = shell_command(computer=computer, command=iffslurm_cmd_sorted_partitions_list)
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

        if not with_node_count:
            idle_nodes = [pn[0] for pn in idle_nodes]

        return idle_nodes
    else:

        raise NotImplementedError(f"{get_queues.__name__} not implemented for computer {computer.label}")


def get_least_occupied_queue(computer: Computer, gpu: bool = None,
                             with_node_count: bool = True, silent: bool = False):
    """Get name of the remote computer (cluster's) queue (slurm: partition) with the highest number of idle nodes.

    Implementations available for these computers:
    - 'iffslurm': FZJ PGI-1 iffslurm cluster.

    :param computer: aiida computer.
    :type computer: Computer
    :param gpu: False: exclude gpu queues. True exclude non-gpu queues. None: ignore this option.
    :param with_node_count: True: queue name with idle nodes count, False: just queue name.
    :param silent: True: do not print out any info.
    :return: tuple of queue name, idle nodes count, or just queue name
    :raise: NotImplementedError if get queues not implemented for that type (by label substring) of computer.
    """
    idle_nodes = get_queues(computer=computer, gpu=gpu, with_node_count=True, silent=silent)
    # if anything is left, get the first queue (ie the one with most idle nodes)
    queue_name, idle_nodes_count = idle_nodes[0] if idle_nodes else (None, None)
    return (queue_name, idle_nodes_count) if with_node_count else queue_name


@dc.dataclass(init=True, repr=True, eq=True, order=False, frozen=False)
class _OptionsQueryConfig:
    """Database query configuration for :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_options`.

    :param ignored: these options fields are ignored for querying options of this config.
    :param mandatory: these options fields are mandatory for querying options of this config.
    :param optional: these options fields are optional for querying options of this config.
    """
    from typing import AnyStr as _AnyStr, List as _List
    from masci_tools.util.python_util import dataclass_default_field as _field
    ignored: _List[_AnyStr] = _field([])
    mandatory: _List[_AnyStr] = _field([])
    optional: _List[_AnyStr] = _field(["queue_name", "account", "withmpi", "gpu"])


@dc.dataclass(init=True, repr=True, eq=True, order=False, frozen=False)
class _OptionsDefaultCreationValues:
    """Default options creation values for :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_options`.

    :param conditional: options fields with values which depend on other fields.
    :param unconditional: options fields with values which do not depend on other fields.

    Adding parameters with default values during runtime works for unconditional only. Adding conditional
    values might require to adapt. :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_options`.

    Keys which begin with an underscore '_' are ignored.
    """

    from typing import Any as _Any, AnyStr as _AnyStr, Dict as _Dict
    from masci_tools.util.python_util import dataclass_default_field as _field
    conditional: _Dict[_AnyStr, _Any] = _field({
        "withmpi": {
            False: {
                "resources": {"num_machines": 1, "tot_num_mpiprocs": 1},
            },
            True: {
                "resources": {"num_machines": 1},
                "_COMMENT": "tot_num_mpiprocs/num_mpiprocs_per_machine = user-specified. if not user-specified: "
                            "tot_num max from default options of same queue_name. if no such options: num_procs "
                            "default from associated computer. if no computer found: min(tot_num_mpiprocs) of all "
                            "options of this config."
            },
        },
    })
    unconditional: _Dict[_AnyStr, _Any] = _field({
        "max_wallclock_seconds": (60 ** 2) * 24  # one day
    })


@dc.dataclass(init=True, repr=True, eq=False, order=False, frozen=False)
class _OptionsConfig:
    """A class to define and group-manage default computer options (builder.metdata.options) for AiiDA processes.

    This class defines a 'config' of computer options. This is a group-managed set of option nodes.

    This class should not be used directly. Instead, use
    :py:class:`~aiida_jutools.util_computer.ComputerOptionsManager`, which is a managed collection of instances of
    this class. There, default values for a particular config instance valid for a particular AiiDA computer can
    be defined.

    When an instance is created, the instances options and groups are unstored default options/groups. When
    :py:meth:`~aiida_jutools.util_computer._OptionsConfig.initialize` is called, the instance will look for
    corresponding groups with those nodes in the database. If present, it will load them. If not, it will create and
    store them.
    """
    from typing import AnyStr as _AnyStr, List as _List, Dict as _Dict
    from masci_tools.util.python_util import dataclass_default_field as _field
    from abc import ABCMeta as _ABCMeta
    from aiida.schedulers import JobResource as _JobResource
    name: _AnyStr = ""
    _groups: _List[Group] = _field([])
    _group_extras: _Dict = _field(
        {"comments": ["These options are also available as iffaiida import, see extras/references."],
         "references": ["https://iffgit.fz-juelich.de/aiida/aiida_nodes",
                        "https://github.com/JuDFTteam/aiida-jutools"]})
    _options: _List[Dict] = _field([])
    _query_config: _OptionsQueryConfig = _field(_OptionsQueryConfig())
    _jobresource_cls: _ABCMeta = _JobResource
    _default_option_creation_values: _OptionsDefaultCreationValues = _field(_OptionsDefaultCreationValues())
    _computers: _List[Computer] = _field([])
    _silent: dc.InitVar[bool] = False

    @dc.dataclass(init=True, repr=True, eq=True, order=False, frozen=False)
    class _HelpConfig:
        """Internal helper class for :py:class:`~aiida_jutools.util_computer._OptionsConfig`.

        Defines input/output keywords used by :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_help`.

        :param keys_mode: input keyword.
        :param desc_mode: input keyword.
        :param keys_mode_return_key_options: keys_mode output dict keyword.
        :param keys_mode_return_key_resources: keys_mode output dict keyword.
        """
        keys_mode: str = 'keywords'
        keys_mode_return_key_options: str = 'options_fields'
        keys_mode_return_key_rescources: str = 'resources_fields'
        desc_mode: str = 'descriptions'

        @property
        def modes(self):
            return [self.keys_mode, self.desc_mode]

    def __post_init__(self, _silent):
        self._is_initialized = False

        # set group extras
        for group in self._groups:
            group.set_extra_many(self._group_extras)

        # for defining internal keywords used by help method.
        self._help_config = _OptionsConfig._HelpConfig()

        # DEVNOTE: one could use the defined default options to get a
        # default

        if not _silent:
            self._log("Info", None, f"Call {self.initialize.__name__}() before use.")

    def _log(self, level: str = None, func=None, msg: str = "", name: bool = True):
        """Basic logging.

        TODO replace with real logging / aiida logging.
        """
        level = f"{level}: " if level else ""
        cls_name = self.__class__.__name__
        config_name = f" '{self.name}'" if name else ""
        func_name = f", {func.__name__}()" if func else ""
        print(f"{level}{cls_name}{config_name}{func_name}: {msg}")

    def _check_if_initalized(self):
        """Checks if :py:meth:`~aiida_jutools.util_computer._OptionsConfig.initialize` has been called.

        Internal helper.
        """
        if not self._is_initialized:
            self._log('Warning', None, f"Not initialized. Call {self.initialize.__name__}() first.")
        return self._is_initialized

    @property
    def computers(self) -> _List[Computer]:
        """This config's associated AiiDA computers.

        Can optionally be set in defaults, or during runtime by user. Otherwise it is guessed by this instance
        when needed, and set.

        To set manually, set underlying list :py:attr:`~aiida_jutools.util_computer.OptionsConfig._computers`.
        """
        if self._computers:
            return self._computers
        else:
            return get_computers(computer_name_pattern=self.name)

    @property
    def groups(self) -> _List[Group]:
        """This config's groups (with options nodes).

        The underlying list :py:attr:`~aiida_jutools.util_computer.OptionsConfig._groups` must be set
        in defaults when instance is created. The groups do not have to exist yet.

        When :py:meth:`~aiida_jutools.util_computer._OptionsConfig.initialize` is called, the specified groups
        get loaded or created. We allow to specify several groups, e.g., to realize backwards compatibility with
        older config groups exports with different group names.

        Groups are identified via their labels, not their database keys.
        """
        self._check_if_initalized()
        return self._groups

    def _update_options(self):
        """Synchronize the config's groups' option nodes with the instances options property.
        """
        # refresh options attribute as collection of all distinct Dicts from all groups
        if self._is_initialized:
            from aiida.orm import QueryBuilder
            qb = QueryBuilder()
            group_names = [group.label for group in self._groups]
            qb.append(Group, filters={"label": {"in": group_names}}, tag="group")
            self._options = qb.append(Dict, with_group="group").distinct().all(flat=True)

    @property
    def options(self) -> _List[Dict]:
        """All of this config's computer options (builder.metatdata.options) for AiiDA processes.

        The underlying list :py:attr:`~aiida_jutools.util_computer.OptionsConfig._options` must be set
        in defaults when instance is created. The Dicts do not have to exist yet.

        To instead load or create a specific options node,
        use :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_options`.

        When :py:meth:`~aiida_jutools.util_computer._OptionsConfig.initialize` is called, the specified options
        get loaded or created. They are searched in or added to the config's groups.
        """
        if self._check_if_initalized():
            self._update_options()
        return self._options

    def initialize(self, alternative_group_names: _List[_AnyStr] = [], silent: bool = False):
        """Initialize this options config.

        Will load or create attribute- or argument- specified groups and load or create attribute-specified
        computer option nodes therein.

        Attribute-specified groups and options are protected by an underscore '_' name, but may be manipulated
        directly before this intialization.

        :param alternative_group_names: Optional. Either to replace names of attribute-specified group(s),
            or to replace attribute-specified group(s) with existing ones to load from db.
        :param silent: True: do not print out any info.
        """
        import copy
        from aiida.common.exceptions import NotExistent

        # Step1): group initialize:
        #
        # note that group instances in _groups attribute after init are unstored groups.
        # they may be replaced if they can be loaded from db instead. else they get 'created' (stored).
        #
        # if any groups get loaded from db, ditch all temporary groups in their favor.
        # if no groups get loaded from db, create (store) all temporary ones.

        if self._is_initialized:
            if not silent:
                self._log("Info", self.initialize, "Is already initialized.")
            return

        primary_group = self._groups[0]
        default_groups_count = len(self._groups)

        # add alternative groups, unstored
        for group_name in alternative_group_names:
            group = Group(label=group_name,
                          description=primary_group.description)
            group.set_extra_many(self._group_extras)
            self._groups.append(group)
        group_names = [group.label for group in self._groups]

        # load already stored groups
        for i, group_name in enumerate(group_names):
            try:
                stored_group = Group.get(label=group_name)
                self._groups[i] = stored_group
            except NotExistent as err:
                pass

        # if no default groups loaded but alternative groups loaded, get rid of default groups
        if (not any([group.is_stored for group in self._groups[:default_groups_count]])) \
                and (any([group.is_stored for group in self._groups[default_groups_count:]])):
            self._groups[:] = self._groups[default_groups_count:]

        # if now any groups loaded, skip creation and remove the unstored ones
        if any([group.is_stored for group in self._groups]):
            self._groups[:] = [group for group in self._groups if group.is_stored]

        # now we have our final group list.
        loaded_or_created = "Loaded" if all([g.is_stored for g in self._groups]) else "Created"

        # if now no groups loaded, ditch all groups except the primary one, and 'create' (store)
        if not any([group.is_stored for group in self._groups]):
            self._groups[:] = [self._groups[0]]
            self._groups[0].store()

        # Step2): options nodes initialize:
        #
        # note that Dict instances in _options attribute after init are unstored options.
        # they may be replaced if they can be loaded from db instead. else they get 'created' (stored).
        #
        # look for default options in all groups.
        # if any option is missing, create it and add it to all groups.

        # replace temporary options with loaded option where it exists
        for i in range(len(self._options)):
            option = self._options[i]
            found = False
            for group in self._groups:
                if not found:
                    group_options = [node for node in list(group.nodes) if (isinstance(node, Dict) and node.is_stored)]
                    for group_option in group_options:
                        if option.attributes == group_option.attributes:
                            # before overwrite, preserve labels if any
                            if self._options[i] and not group_option.label:
                                group_option.label = self._options[i].label
                            self._options[i] = group_option
                            found = True
                            break

        num_loaded = len([option for option in self._options if option.is_stored])
        num_created = len(self._options) - num_loaded

        # 'create' (store) all remaining unstored options and add to all groups
        for option in self._options:
            if not option.is_stored:
                option.store()
                for group in self._groups:
                    group.add_nodes([option])

        # initialize is done.
        self._is_initialized = True

        if not silent:
            print(f"OptionsConfig '{self.name}':\n"
                  f"{loaded_or_created} computer options groups: {[group.label for group in self._groups]}.\n"
                  f"Loaded {num_loaded}, created {num_created} default computer options nodes. "
                  f"Use {self.get_options.__name__}() to load or create options nodes.")

    def get_options(self, store_if_not_exist: bool = True, as_Dict: bool = True, silent: bool = False,
                    computer_name: str = None, gpu: bool = None, withmpi: bool = True, queue_name: str = None,
                    account: str = None, **kwargs) -> list:
        """Get matching options most closely matching given parameters.

        This is the central method of both :py:class:`~aiida_jutools.util_computer._OptionsConfig`and its collection
        class :py:class:`~aiida_jutools.util_computer.ComputerOptionsManager`. You can call it with no arguments
        at all, and it will give you a valid options node based on best guesses from the default options and/or
        this config's associated computer. Or you can fully specify every valid available computer options field,
        and it will create an options node for that. And everything in between. The method first checks if a
        matching options node exists in the databse, then it returns that (from config's group). Else it creates one.
        This implements the load-or-create paradigm to promote data reuse over redundancy.

        The first four arguments are not defined computer options fields. Instead, they define the method's
        behavior. For example, if you are unsure whether the specified options should be created and you first want
        to check what kind of options node the method would create. The remaining arguments are all defined computer
        options fields. You can get a full list or descriptions of them by calling
        :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_help`.

        If you supply kwargs (i.e., additional options field values), do it like field_name=value, so e.g.
        get_options(..., scheduler_stderr='stderr.txt', ...).

        :param store_if_not_exist: True: if the matching options Dict doesn't exist yet, store it before returning it.
        :param as_Dict: True: return matching options as AiiDA Dicts. False: as dicts.
        :param silent: True: do not print out any info. False: only print warnings.
        :param computer_name: Optional: name associated computer. Useful if, e.g., method needs to find queue_name.
        :param gpu: used if method needs to find queue_name. False: exclude GPU queues. True: include. None: don't care.
        :param withmpi: options field.
        :param queue_name: options field. Queue/partition name. If not given but needed, I will guess it.
        :param account: options field. For configs (computers) with account-based queue assignment (e.g., claix).
        :param kwargs: Optional: other options fields. See :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_help`.
        :return: list of options.
        """
        # DEVNOTE: adjust this if signature changes! (ie if other options keywords are moved from kwargs to named
        #          method arguments). This affects also option query and creation sections below.
        explicit_argument_keywords = ['withmpi', 'queue_name', 'account']

        if not self._check_if_initalized():
            return []

        if not silent:
            self._log('Info', self.get_options)

        # first, validate the remaining options keywords (kwargs) before starting the query
        help = self.get_help(mode='keywords')
        all_options_keys = help.pop(self._help_config.keys_mode_return_key_options)
        resources_keys = help.pop(self._help_config.keys_mode_return_key_rescources)
        # need to pop explicit argument keywords to avoid double occurrence
        all_options_keys[:] = [k for k in all_options_keys if k not in explicit_argument_keywords]
        invalid_kwargs = {k: v for k, v in kwargs.items() if not k in all_options_keys}
        valid_kwargs = kwargs if not invalid_kwargs else {k: v for k, v in kwargs.items() if k in all_options_keys}
        if invalid_kwargs:
            self._log('Warning', self.get_options,
                      f"Supplied some invalid options keywords: {list(invalid_kwargs.keys())}. "
                      f"I will ignore these. Call {self.get_help.__name__} to list allowed keywords.")
        # special treatment for keyword 'resources': value must be a dict and its allowed keywords are defined
        # by the computer's scheduler's JobResource.
        if 'resources' in valid_kwargs:
            if not isinstance(valid_kwargs['resources'], dict):
                self._log('Warning', self.get_options,
                          f"Supplied keyword 'resources' with non-dict value {valid_kwargs['resources']}. "
                          f"But value must be a dict. I will ignore this keyword.")
                valid_kwargs.pop('resources')

        def missing_mandatory_arg_err_msg(argname, msg_suffix):
            return f"Argument '{argname}' mandatory for config '{self.name}' but not supplied. {msg_suffix}."

        # if some mandatory arguments need validation or are missing,
        # try getting them from the computer config it it allows it
        idx_mandatory_key = 0
        for idx_mandatory_key, mandatory_key in enumerate(self._query_config.mandatory):
            if mandatory_key == "queue_name":
                if not silent:
                    print(f"Missing mandatory argument 'queue_name'. Try find matching computer and "
                          f"call {get_queues.__name__}().")
                    # query the currently least occupied queue and return options for that.
                    # but for that, need the associated computer node
                queue_names = None

                # first try to get computer from config associated computers, if any
                if not silent:
                    print(f"Try to get computer from config's assigned computers.")
                idx_computer = 0
                idx_remove_computer = []
                while (not queue_names) and (idx_computer < len(self._computers)):
                    computer = self._computers[idx_computer]
                    try:
                        queue_names = get_queues(computer=computer, gpu=gpu, with_node_count=False, silent=silent)
                    except NotImplementedError as err:
                        self._log('Warning', self.get_options,
                                  f"Config's computer {computer.label} is not compatible with this config. "
                                  f"Reason: {get_queues.__name__} not implemented for this type "
                                  f"of computer). I will remove it from the config.")
                        idx_remove_computer.append(idx_computer)
                    idx_computer += 1
                self._computers[:] = [computer for idx, computer in enumerate(self._computers)
                                      if not idx in idx_remove_computer]
                if not queue_names:
                    # next try to get computer by query
                    computer_name_pattern = computer_name if computer_name else self.name
                    if not silent:
                        print(f"Try to get computer from name pattern '{computer_name_pattern}'.")
                    computers = get_computers(computer_name_pattern=computer_name_pattern)
                    if not computers:
                        # next, try decomposing config name into words and get computer from words
                        import re
                        confwords = (" ".join(re.findall("[a-zA-Z]+", self.name))).split(" ")
                        idx_words = 0
                        while (not computers) and (idx_words < len(confwords)):
                            computers = get_computers(computer_name_pattern=confwords[idx_words])
                            idx_words += 1
                    if not computers:
                        from aiida.common.exceptions import NotExistent
                        raise NotExistent(
                            missing_mandatory_arg_err_msg("queue_name",
                                                          f"Found no matching computer with name pattern '{computer_name_pattern}'."))
                    else:
                        idx_computer = 0
                        while (not queue_names) and (idx_computer < len(computers)):
                            computer = computers[idx_computer]
                            queue_names = get_queues(computer=computer, gpu=gpu, with_node_count=False, silent=silent)
                            if queue_names:
                                self._computers.append(computer)
                            idx_computer += 1
                if queue_names and not silent:
                    print(f"Found queue_names '{queue_names}'.")
                if not queue_names:
                    raise ValueError(
                        missing_mandatory_arg_err_msg("queue_name",
                                                      f"Could not determine queue_name from computer."))

                if queue_name:
                    # make sure that the supplied queue name actually exists on that computer.
                    if not queue_name in queue_names:
                        raise ValueError(f"Supplied queue_name {queue_name} does not exist / not available for "
                                         f"associated computer {computer} of options config {self.name}.")
                else:
                    if not silent:
                        print("Choosing least occupied queue.")
                    queue_name = queue_names[0]

            if mandatory_key == "account":
                if not account:
                    raise NotImplementedError(
                        missing_mandatory_arg_err_msg("account",
                                                      f"I have no implementation to determine it automatically."))
        if self._query_config.mandatory and (idx_mandatory_key != (len(self._query_config.mandatory) - 1)):
            self._log('Warning', self.get_options, "Did not handle all mandatory options keywords for this config. "
                                                   "If it works regardless, adjust query config. Else, contact "
                                                   "developer.")

        # now either load or create options
        loaded, stored = True, True

        # now try to load (query) the desired options node first
        from aiida.orm import QueryBuilder, Group
        qb = QueryBuilder()
        tag_group = "group"
        group_names = [group.label for group in self._groups]
        qb.append(Group, filters={"label": {"in": group_names}}, tag=tag_group)
        filters = {"and": []}
        if queue_name:
            filters["and"].append({"attributes.queue_name": queue_name})
        if account:
            filters["and"].append({"attributes.custom_scheduler_commands": {"ilike": f"%--account={account}%"}})
            # DEVNOTE: in theory, we should instead require ...
            # filters["and"].append({"attributes.account": account})
            # ... but the latter does not work on all systems (case in point: claix18, slurm scheduler),
            # whereas the former works on at least one system (case in point: claix18, slurm scheduler).
            # With 'does not work' meaning, that the generated _aiidasubmit.sh slurm batch job script
            # does not contain the account when the latter is used.
            # Based on this, we adopt the former (custom_scheduler_commands) approach for ALL systems,
            # at least for now.
        if withmpi:
            filters["and"].append({"attributes.withmpi": withmpi})
        # now add user-specified other option attributes to query
        for attr, value in valid_kwargs.items():
            if isinstance(value, list):
                self._log("Warning", self.get_options,
                          f"Supplied options query attribute '{attr}':{value}. "
                          f"No support for lists. I will ignore this attribute.")
                continue
            if isinstance(value, dict):
                for subattr, subvalue in value.items():
                    if isinstance(subvalue, (dict, list)):
                        self._log("Warning", self.get_options,
                                  f"Supplied options query attribute '{attr}':'{subattr}':{subvalue}. "
                                  f"No support for lists or nested attributes above level 2. "
                                  f"I will ignore this subattribute.")
                        continue
                    attr_string = f"attributes.{attr}.{subattr}"
                    filters["and"].append({attr_string: subvalue})
            else:
                attr_string = f"attributes.{attr}"
                filters["and"].append({attr_string: value})

        qb.append(Dict, with_group=tag_group, filters=filters)
        res = qb.all(flat=True)

        # if no results, create a temporary options node (ie without storing).
        # storing only if store_if_not_exist True.
        if not res:
            loaded, stored = False, False
            if not silent:
                store_or_dont = "store" if store_if_not_exist else "do not store"
                print(f"Did not find specified computer options in config. Create options node and {store_or_dont}.")

            opt_dict = {}
            opt_label = f"options_{self.name}"
            # fill in user-specified argument:value pairs
            if withmpi:
                opt_dict["withmpi"] = withmpi
            if queue_name:
                opt_dict["queue_name"] = queue_name
                opt_label += f"_{queue_name}"
            if account:
                if not opt_dict.get("custom_scheduler_commands", None):
                    opt_dict["custom_scheduler_commands"] = "#SBATCH"
                opt_dict["custom_scheduler_commands"] += f" --account={account}"
                # DEVNOTE: in theory, we should instead require ...
                # opt_dict["account"] = account
                # ... but the latter does not work on all systems (case in point: claix18, slurm scheduler),
                # whereas the former works on at least one system (case in point: claix18, slurm scheduler).
                # With 'does not work' meaning, that the generated _aiidasubmit.sh slurm batch job script
                # does not contain the account when the latter is used.
                # Based on this, we adopt the former (custom_scheduler_commands) approach for ALL systems,
                # at least for now.
                opt_label += f"_{account}"
            if not withmpi:
                opt_label += f"_serial"

            # # # # #
            # DEVNOTE:
            # for (probably all) keywords, these two have the same effect (example account):
            #
            # # opt_dict["account"] = account
            #
            # and
            #
            # # if not opt_dict.get('custom_scheduler_commands', None):
            # #     opt_dict["custom_scheduler_commands"] = "#SBATCH"
            # # opt_dict["custom_scheduler_commands"] += f' --account={account}'
            #
            # In fact, aiida will resolve the former into the latter before actual job
            # submission. but the latter is harder to handle wrt node loading (querying) and creation.
            # So always prefer options keywords over custom commands where available.
            # # # # #

            # now add default argument:values
            # # unconditional arguments
            for attr, value in self._default_option_creation_values.unconditional.items():
                if not attr.startswith("_"):
                    opt_dict[attr] = value
            # # conditional arguments
            for attr, cases in self._default_option_creation_values.conditional.items():
                if not attr.startswith("_"):
                    # # # conditional: withmpi:
                    if attr == "withmpi":
                        # # # # get the matching case argument:value pairs
                        for cond_attr, value in cases[withmpi].items():
                            if not cond_attr.startswith("_"):
                                resources_mpi_keys = ["tot_num_mpiprocs", "num_mpiprocs_per_machine"]
                                if (cond_attr == "resources") and (withmpi) \
                                        and not any(value.get(rkey, None) for rkey in resources_mpi_keys):
                                    # in this case tot_num_mpi_procs is neither default nor user-specified (through kwargs),
                                    # so must determine.
                                    tot_num_mpiprocs = None
                                    mpiprocs_per_mac = None
                                    resources_value = None

                                    # first try: if queue_name is given, existing options with that queue name.
                                    # assumption here: the max of mpi_procs of all options of that queue is still
                                    # valid and a good guess.

                                    if queue_name:
                                        queue_opts = [opt for opt in self.options if
                                                      opt.attributes.get("queue_name", None) == queue_name]
                                        if queue_opts:
                                            node_totmpi = []
                                            node_mpiper = []
                                            for opt_node in queue_opts:
                                                if opt_node.attributes.get("withmpi", None) and \
                                                        opt_node.attributes.get("resources", None):
                                                    totmpi = opt_node.attributes["resources"].get("tot_num_mpiprocs",
                                                                                                  None)
                                                    mpiper = opt_node.attributes["resources"].get(
                                                        "num_mpiprocs_per_machine",
                                                        None)
                                                    if totmpi:
                                                        node_totmpi.append(totmpi)
                                                    if mpiper:
                                                        node_mpiper.append(mpiper)
                                            tot_num_mpiprocs = max(node_totmpi) if node_totmpi else None
                                            mpiprocs_per_mac = max(node_mpiper) if node_mpiper else None

                                    # if that failed (ie if no computers): go through existing option nodes and take the minimum.
                                    # if none exist, choose value 1.
                                    if not tot_num_mpiprocs and not mpiprocs_per_mac:
                                        node_totmpi = []
                                        node_mpiper = []
                                        for opt_node in self.options:
                                            try:
                                                if opt_node.attributes["withmpi"]:
                                                    node_totmpi.append(
                                                        opt_node.attributes["resources"]["tot_num_mpiprocs"])
                                                    node_mpiper.append(
                                                        opt_node.attributes["resources"]["num_mpiprocs_per_machine"])
                                            except KeyError as err:
                                                pass
                                        tot_num_mpiprocs = min(node_totmpi) if node_totmpi else 1
                                        mpiprocs_per_mac = min(node_mpiper) if node_mpiper else 1

                                    # if that failed, try via computer
                                    if not tot_num_mpiprocs and not mpiprocs_per_mac:
                                        computers = self.computers
                                        idx_computer = 0
                                        while (not mpiprocs_per_mac) and (idx_computer < len(computers)):
                                            mpiprocs_per_mac = computers[
                                                idx_computer].get_default_mpiprocs_per_machine()

                                    if tot_num_mpiprocs and tot_num_mpiprocs > 1:
                                        value["tot_num_mpiprocs"] = tot_num_mpiprocs
                                    elif mpiprocs_per_mac and mpiprocs_per_mac > 1:
                                        value["num_mpiprocs_per_machine"] = mpiprocs_per_mac
                                    else:
                                        value["tot_num_mpiprocs"] = tot_num_mpiprocs

                                opt_dict[cond_attr] = value

            # now add user-specified other option attributes.
            # these my overwrite default values.
            for attr, value in valid_kwargs.items():
                if (not isinstance(value, dict)) or (not opt_dict.get(attr, None)):
                    opt_dict[attr] = value
                else:
                    # guard against dict values override, nesting level0
                    for subattr, subvalue in value.items():
                        opt_dict[attr][subattr] = subvalue

            # now turn opt_dict into a option node and store in group, if so specified.
            opt_Dict = Dict(label="", dict=opt_dict)
            opt_Dict.label = opt_label
            if store_if_not_exist:
                stored = True
                opt_Dict.store()
                for group in self.groups:
                    group.add_nodes([opt_Dict])

            # and turn into return value
            res = [opt_Dict]

        # resources is mandatory and (if created), if not user-supplied, should be set via creation defaults.
        invalid_stored_opt_nodes = []
        invalid_unstored_opt_Dicts = []
        for opt in res:
            idx_to_pop = []
            if not opt.attributes.get("resources", None):
                if opt.is_stored:
                    invalid_stored_opt_nodes.append(opt)
                else:
                    invalid_unstored_opt_Dicts.append(opt)
            else:
                # even if resources is there, subfields might clash (e.g. user args and default creation values clash).
                resources = opt.attributes["resources"]
                clashable_subkeys = ['num_machines', 'num_mpiprocs_per_machine', 'tot_num_mpiprocs']
                if all(subkey in resources for subkey in clashable_subkeys):
                    self._log("Warning", self.get_options, f"One or more determined options' 'resources' have all "
                                                           f"three subfields {clashable_subkeys} defined. Check "
                                                           f"that these don't clash.")
        res[:] = [opt for opt in res if opt.attributes.get("resources", None)]
        if invalid_stored_opt_nodes or invalid_unstored_opt_Dicts:
            msg = f"The determined options are missing the mandatory field 'resources'. Either because configs group " \
                  f"contains such invalid options nodes, or because during creation, 'resources' was not supplied or " \
                  f"could not be determined. In case of stored invalid nodes, I strongly suggest, you delete them " \
                  f"via {self.delete_options.__name__}(). I will exclude these options from the return value and " \
                  f"instead list them here.\n" \
                  f"Invalid created options:\n{[opt.attributes for opt in invalid_unstored_opt_Dicts]}.\n" \
                  f"Invalid loaded options:\n{invalid_stored_opt_nodes}."
            self._log("Warning", self.get_options, msg)
            return []

        # return the load_or_create result
        if not silent:
            loaded_or_created = "Loaded" if loaded else "Created"
            stored_or_not = " and stored" if not loaded and stored else ""
            nodes_or_not = " node(s)" if loaded or (not loaded and stored) else ""
            print(f"{loaded_or_created}{stored_or_not} options {nodes_or_not} {res}.")

            if not loaded and gpu:
                print(f"Warning: Supplied gpu={gpu} and created options. I currently make no distinction between "
                      f"options creation for non-GPU and GPU queues/partitions. So for the latter, these options "
                      f"may be incorrect.")

        return res if as_Dict else [node.attributes for node in res]

    def get_help(self, mode: str, *args) -> dict:
        """Get list of valid computer options keywords, optionally with descriptions.

        Has two modes: either return full lists of keywords, or return keywords with their descriptions.

        - get_help('keywords'): Full lists of all keywords. All other args ignored.
        - get_help('descriptions'): Full dict of all keywords plus their descriptions.
        - get_help('descriptions', 'withmpi', 'queue_name'): descriptions for those keywords only.
        - get_help('descriptions', *['withmpi', 'queue_name']): as above.

        The 'resources' options keyword (field) is special because it has 'sub'-fields (ie, in an options Dict, its
        value is itself a dict). For this reason, keywords-mode returns a dict with two entries, one with full list of
        options fields, and one with full list of resources fields valid for this config's associated computer.

        :param mode: 'keywords' or 'description'
        :param args: optional for 'description' mode: reduce output to these keywords (e.g., 'withmpi', ...)
        :return: if keys_only, dict with options and resources keywords lists. Else dict of descriptions.
        """
        modes = self._help_config.modes
        if mode not in modes:
            self._log('Warning', self.get_help, f"Undefined mode. Mode must be one of {modes}.")
            return

        is_mode_keys = (mode == self._help_config.keys_mode)
        is_mode_desc = (mode == self._help_config.desc_mode)

        from aiida.engine import CalcJob
        all_options_keys = sorted(list(CalcJob.spec_options.keys()))

        if is_mode_desc:
            invalid_options_keys = [k for k in args if not k in all_options_keys]
            valid_options_keys = args if not invalid_options_keys else [k for k in args if k in all_options_keys]
            if invalid_options_keys:
                self._log('Warning', self.get_help, f"Supplied some invalid options keywords: "
                                                    f"{list(invalid_options_keys)}.")

            selected_keys = valid_options_keys if valid_options_keys else all_options_keys
            all_descriptions = CalcJob.spec_options.get_description()
            descriptions = {k: v for k, v in all_descriptions.items() if k in selected_keys}
            return {k: descriptions[k] for k in sorted(descriptions)}

        elif is_mode_keys:
            # special treatment for keyword 'resources': its value is a dict with sub-keywords. those allowed keywords are
            # defined by the computer's scheduler's JobResource.
            resources_keys = []

            # for need corresponding JobResource. First try to get from class attribute.
            # If that fails, try to get computer, to get associated JobResource.
            from aiida.schedulers import JobResource

            if self._jobresource_cls:
                if not issubclass(self._jobresource_cls, JobResource):
                    self._log('Warning', self.get_options, f"Config's jobresource attribute should be a subclass "
                                                           f"of {JobResource.__module__}.{JobResource.__name__}. "
                                                           f"It is not. I will ignore it.")
                resources_keys = self._jobresource_cls.get_valid_keys()
            if not resources_keys:
                # this likely means that base JobResource class is used, which defines no valid keys.
                # but this may not be the actual corresponding computer's JobResource class. So try
                # getting it via the computer now.
                i = 0
                while (not resources_keys) and (i < len(self.computers)):
                    resources_keys = self.computers[i].get_scheduler().job_resource_class.get_valid_keys()
                    i += 1
            if not resources_keys:
                # since could not determine appropriate resources keys for this config, do sth else instead:
                # gather all resources keys defined in all immediate JobResource subclasses and use that.
                # note that this might be a bit unstable wrt aiida version changes.
                from aiida.schedulers import NodeNumberJobResource, ParEnvJobResource
                from aiida.schedulers.plugins.lsf import LsfJobResource
                resources_keys = sorted(list(set(NodeNumberJobResource.get_valid_keys()) +
                                             set(ParEnvJobResource.get_valid_keys()) +
                                             set(LsfJobResource.get_valid_keys())))

            return {self._help_config.keys_mode_return_key_options: all_options_keys,
                    self._help_config.keys_mode_return_key_rescources: resources_keys}

        else:
            raise NotImplementedError("This code should never be reached. Fix code or contact developer.")

    def delete_options(self, options_nodes: _List[Dict], dry_run: bool = True, verbosity: int = 1):
        """Delete some of the config's stored options from the database.

        This is useful, for instance, if you created and stored some undesired options via
        :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_options` by accident. Keeping them in the
        config / database worsens the precision of what that method returns.

        :param options_nodes: list of stored option nodes you want to have deleted.
        :param dry_run: True: only show what I would do. False: do it.
        :param verbosity: node deletion verbosity. 0: silent, 1: number of nodes, 2: full node list.
        """
        if not self._check_if_initalized():
            return

        # safety checks
        if not isinstance(options_nodes, list):
            options_nodes = [options_nodes]
        if not all([isinstance(node, Dict) for node in options_nodes]):
            self._log("Warning", self.delete_options, f"Not all supplied nodes are {Dict.__name__} nodes. Aborting.")
            return
        if not all([node.is_stored for node in options_nodes]):
            self._log("Warning", self.delete_options, f"Not all supplied nodes are stored. Aborting.")
            return
        config_options = self.options
        is_from_config = []
        is_not_from_config_pks = []
        for node in options_nodes:
            found = False
            for opt in self.options:
                if node.pk == opt.pk:
                    found = True
                    break
            is_from_config.append(found)
            if not found:
                is_not_from_config_pks.append(node.pk)
        if not all(is_from_config):
            self._log("Warning", self.delete_options, f"Some supplied nodes ({is_not_from_config_pks}) are not nodes "
                                                      f"from this config.")

        # delete options nodes
        pks = [node.pk for node in options_nodes]
        if verbosity > 0:
            self._log("Info", self.delete_options, f"Deleting specified option nodes {pks} from config ...")
        from aiida.manage.database.delete.nodes import delete_nodes
        delete_nodes(pks=pks, dry_run=dry_run, force=True, verbosity=verbosity)


@dc.dataclass(init=True, repr=True, eq=False, order=False, frozen=False)
class ComputerOptionsManager:
    """Manage computer options (builder.metdata.options) for AiiDA processes.

    :param localhost: localhost config
    :param iffslurm: iffslurm config
    :param claix18: claix 2018 config
    :param claix16: claix 2016 config

    Call :py:meth:`~aiida_jutools.util_computer.ComputerOptionsManager.initialize` after creating instance.

    - sets of sensible defaults options for common computers called 'configs'
    - group-based options management which promotes load or create (reuse over redundancy)
    - easily accessible help on options fields

    Default configs available for these computers categories:

    - 'iffslurm': FZJ PGI-1 iffslurm cluster.
    - 'claix18': RWTH CLAIX 2018 cluster
    - 'claix16': RWTH CLAIX 2016 cluster
    - 'localhost': a generic localhost computer.

    The configs become available as attributes, and expose their methods, like get options from them.
    The manager provides most of the same methods with a list selection argument to include subsets
    of configs. Depending on the method, using the configs' method directly (e.g. get options), or using
    the manager's method (initialize all configs) is more useful.

    The manager is basically a collection of named :py:class:`~aiida_jutools.util_computer._OptionsConfig` instances.

    >>> import aiida
    >>> aiida.load_profile()
    >>> from aiida_jutools import util_computer
    >>> optman = util_computer.ComputerOptionsManager()
    >>> optman.initialize()
    >>> optman.iffslurm.get_options()
    >>> optman.claix18.get_help()
    """
    from typing import List as _List, AnyStr as _AnyStr
    from masci_tools.util.python_util import dataclass_default_field as _field
    from aiida.schedulers.plugins.slurm import SlurmJobResource as _SlurmJobResource
    localhost: _OptionsConfig = _field(_OptionsConfig(
        name="localhost",
        _groups=[Group(label="computer_options/localhost",
                       description="Default computer options (Dict nodes) for the a generic local computer.")],
        _options=[Dict(label="options_localhost_serial",
                       dict={'max_wallclock_seconds': (60 ** 2),
                             'withmpi': False,
                             'resources': {'num_machines': 1, 'tot_num_mpiprocs': 1}}),
                  Dict(label="options_localhost",
                       dict={'max_wallclock_seconds': (60 ** 2),
                             'withmpi': False,
                             'resources': {'num_machines': 1, 'tot_num_mpiprocs': 4}})],
        _query_config=_OptionsQueryConfig(ignored=["queue_name", "account", "gpu"],
                                          mandatory=[],
                                          optional=["withmpi"]),
        _silent=True
    ), deepcopy=False)
    iffslurm: _OptionsConfig = _field(_OptionsConfig(
        name="iffslurm",
        _groups=[Group(label="computer_options/iffslurm",
                       description="Default computer options (Dict nodes) for the FZJ PGI iffslurm computer."),
                 Group(label="iffslurm_options",
                       description="Default computer options (Dict nodes) for the FZJ PGI iffslurm computer.")],
        _options=[Dict(label="options_iffslurm_oscar",
                       dict={"queue_name": "oscar", "max_wallclock_seconds": (60 ** 2) * 24,
                             "withmpi": True,
                             "resources": {"num_machines": 1, "tot_num_mpiprocs": 12}}),
                  Dict(label="options_iffslurm_oscar_serial",
                       dict={"queue_name": "oscar", "max_wallclock_seconds": (60 ** 2) * 24,
                             "withmpi": False,
                             "resources": {"num_machines": 1, "tot_num_mpiprocs": 1}}),
                  Dict(label="options_iffslurm_th1",
                       dict={"queue_name": "th1", "max_wallclock_seconds": (60 ** 2) * 24,
                             "withmpi": True,
                             "resources": {"num_machines": 1, "tot_num_mpiprocs": 12}}),
                  Dict(label="options_iffslurm_th1_serial",
                       dict={"queue_name": "th1", "max_wallclock_seconds": (60 ** 2) * 24,
                             "withmpi": False,
                             "resources": {"num_machines": 1, "tot_num_mpiprocs": 1}}),
                  Dict(label="options_iffslurm_th1-2020-32",
                       dict={"queue_name": "th1-2020-32", "max_wallclock_seconds": (60 ** 2) * 24,
                             "withmpi": True,
                             "resources": {"num_machines": 1, "tot_num_mpiprocs": 32}}),
                  Dict(label="options_iffslurm_th1-2020-32_serial",
                       dict={"queue_name": "th1-2020-32", "max_wallclock_seconds": (60 ** 2) * 24,
                             "withmpi": False,
                             "resources": {"num_machines": 1, "tot_num_mpiprocs": 1}}),
                  Dict(label="options_iffslurm_th1-2020-64",
                       dict={"queue_name": "th1-2020-64", "max_wallclock_seconds": (60 ** 2) * 24,
                             "withmpi": True,
                             "resources": {"num_machines": 1, "tot_num_mpiprocs": 64}}),
                  Dict(label="options_iffslurm_viti",
                       dict={"queue_name": "viti", "max_wallclock_seconds": (60 ** 2) * 24,
                             "withmpi": True,
                             "resources": {"num_machines": 1, "tot_num_mpiprocs": 20}})],
        _query_config=_OptionsQueryConfig(ignored=["account"],
                                          mandatory=["queue_name"],
                                          optional=["gpu", "withmpi"]),
        _jobresource_cls=_SlurmJobResource,
        _silent=True
    ), deepcopy=False)
    claix18: _OptionsConfig = _field(_OptionsConfig(
        name="claix18",
        _groups=[Group(label="computer_options/claix18",
                       description="Default computer options (Dict nodes) for the RWTH claix 2018 computer.")],
        _options=[
            Dict(label="options_claix18",
                 dict={'max_wallclock_seconds': (60 ** 2) * 24,
                       'withmpi': True,
                       'resources': {'num_machines': 1, 'tot_num_mpiprocs': 48},
                       'custom_scheduler_commands': ""})
        ],
        _query_config=_OptionsQueryConfig(ignored=["queue_name", "gpu"],
                                          mandatory=[],
                                          optional=["account", "withmpi"]),
        _jobresource_cls=_SlurmJobResource,
        _silent=True
    ), deepcopy=False)
    claix16: _OptionsConfig = _field(_OptionsConfig(
        name="claix16",
        _groups=[Group(label="computer_options/claix16",
                       description="Default computer options (Dict nodes) for the RWTH claix 2016 computer.")],
        _options=[
            Dict(label="options_claix16",
                 dict={'max_wallclock_seconds': (60 ** 2) * 24,
                       'withmpi': True,
                       'resources': {'num_machines': 1, 'tot_num_mpiprocs': 24},
                       'custom_scheduler_commands': ""})
        ],
        _query_config=_OptionsQueryConfig(ignored=["queue_name", "gpu"],
                                          mandatory=[],
                                          optional=["account", "withmpi"]),
        _jobresource_cls=_SlurmJobResource,
        _silent=True
    ), deepcopy=False)

    def __post_init__(self):
        # note: we need this for the configs property. has to be updated when an option is added.
        # DEVNOTE: using getattr() / getmembers on self, with type checking, instead throws RecursionError.
        self._configs = [self.localhost, self.iffslurm, self.claix18, self.claix16]
        self._help_config = _OptionsConfig._HelpConfig()

        self._log("Info", None, f"Call {self.initialize.__name__}() before use.")

    def _log(self, level: str = None, func=None, msg: str = "", name: bool = True):
        """Basic logging.

        TODO replace with real logging / aiida logging.
        """
        level = f"{level}: " if level else ""
        cls_name = self.__class__.__name__
        func_name = f", {func.__name__}()" if func else ""
        print(f"{level}{cls_name}{func_name}: {msg}")

    @property
    def configs(self) -> _List[_OptionsConfig]:
        """Get all configs.
        """
        return self._configs

    @property
    def config_names(self) -> _List[_AnyStr]:
        """Get the names of all configs.
        """
        return [config.name for config in self.configs]

    @property
    def groups(self) -> _List[Group]:
        """Get the groups of all configs.
        """
        return self.get_groups()

    def get_groups(self, config_names: list = []) -> _List[Group]:
        """Get the groups of a selection of configs.

        :param config_names: selection of configs. If empty, select all configs.
        :return: groups of those configs.
        """
        valid_config_names, _, _ = self._get_valid_config_names_from(config_names=config_names, silent=True)

        groups = []
        for config in self.configs:
            if config.name in valid_config_names:
                groups.extend(config.groups)
        return groups

    def _get_valid_config_names_from(self, config_names: list = [], silent: bool = True):
        """Helper. Check supplied config names against names of actual configs and return valid names.

        :param config_names: selection of config names.
        :param silent: True: do not print out any info. False: only print warnings.
        :return: tuple of (valid, unselected, invalid config names) of this config
        """
        supplied_config_names = config_names if config_names else self.config_names
        valid_config_names = [config.name for config in self.configs if config.name in supplied_config_names]
        invalid_config_names = list(set(supplied_config_names) - set(valid_config_names))
        unselected_config_names = list(set(self.config_names) - set(valid_config_names))

        if not silent and invalid_config_names:
            print(f"Ignoring invalid config names: {invalid_config_names}.")

        return valid_config_names, unselected_config_names, invalid_config_names

    def initialize(self, config_names: list = [], silent: bool = False, delete_other: bool = False,
                   delete_dry_run: bool = True, delete_verbosity: int = 1):
        """Initialize the manager's configs. Must be called before usage.

        Each config is a :py:class:`~aiida_jutools.util_computer._OptionsConfig` instance. These
        contain a set of unstored default options nodes, a group for them. When this method is called,
        the manager checks for each config whether the group and nodes exist. Then it loads them from
        the database, or creates (stores) them. Then they are available for usage.

        Available config names: 'localhost', 'iffslurm', 'claix18', 'claix16'.

        Note: If using delete_other=True, always first perform a dry run. Because this may delete simulations
        which were run with these options.

        :param config_names: Empty: init all configs. Subset: init these configs, remove the rest from manager.
        :param silent: True: do not print out any info. False: only print warnings.
        :param delete_other: delete unselected configs' groups from database, if they exist.
        :param delete_dry_run: True: show what would be done if delete_other. False: do it.
        :param delete_verbosity: node deletion verbosity. 0: silent, 1: number of nodes, 2: full node list.
        """
        valid_config_names, unselected_config_names, invalid_config_names = \
            self._get_valid_config_names_from(config_names=config_names, silent=silent)

        if not silent:
            print(f"Initializing computer options configs: {valid_config_names}.")

        # get rid of unselected configs
        # # first delete in db if so specified
        if delete_other:
            if not unselected_config_names:
                if not silent:
                    method_name = f"{self.__class__.__name__}.{self.initialize.__name__}()"
                    print(
                        f"Info: Specified to delete unselected configs, but none present. If you selected only a "
                        f"subset of config names and expected to see some deletions now, this may be because you ran "
                        f"{method_name} in delete dry run mode previously. Unselected config attributes get removed "
                        f"from the instance in any case, but, for safety, only get deleted from the database as well "
                        f"if delete True and delete dry run mode False. If you want that done, please re-instantiate "
                        f"this class to get back the unselected config attributes, then run {method_name} again with "
                        f"delete dry run False.")
            else:
                from aiida.common.exceptions import NotExistent
                if not silent:
                    print(f"Deleting groups and nodes for unselected configs {unselected_config_names} "
                          f"from database, if already stored, dry run: {delete_dry_run}.")
                from aiida_jutools import util_group

                # not using get_groups() here to suppress not initialized warning
                groups = []
                for config in self.configs:
                    if config.name in unselected_config_names:
                        groups.extend(config._groups)
                stored_groups = []
                for group in groups:
                    # since we are not necessarily initialized yet, the groups may or may not exist in db.
                    # so wrap in try except block.
                    try:
                        stored_group = Group.get(label=group.label)
                        stored_groups.append(stored_group)
                    except NotExistent as err:
                        pass
                util_group.delete_groups_with_nodes(group_labels=[group.label for group in stored_groups],
                                                    dry_run=delete_dry_run, verbosity=delete_verbosity,
                                                    leave_groups=False)

        # # then remove unselected configs from instance
        self._configs[:] = [config for config in self._configs if config.name in valid_config_names]
        for config_name in unselected_config_names:
            delattr(self, config_name)

        # finally, initialize selected configs
        for config in self.configs:
            if config.name in valid_config_names:
                if not config._is_initialized:
                    config.initialize(silent=silent)
                elif not silent:
                    print(f"Config '{config.name}' is already initialized.")

    def get_options(self, config_names: list = [], store_if_not_exist: bool = True, as_Dict: bool = True,
                    silent: bool = False, computer_name: str = None, gpu: bool = None, withmpi: bool = True,
                    queue_name: str = None, account: str = None, **kwargs):
        """Get matching options from specified configs most closely matching given parameters.

        Note: Often it is easier to address the desired config by manager attribute and call its
        method :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_options` directly. The
        manager's method here calls each of these in turn and collects the result options in one list. Most of
        the time, one only wants options for a specific computer, i.e., from a specific config.

        See :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_options` for details.

        :param config_names: Empty: on all configs. Subset: based on these configs.
        :param store_if_not_exist: True: if the matching options Dict doesn't exist yet, store it before returning it.
        :param as_Dict: True: return matching options as AiiDA Dicts. False: as dicts.
        :param silent: True: do not print out any info. False: only print warnings.
        :param computer_name: Optional: name associated computer. Useful if, e.g., method needs to find queue_name.
        :param gpu: used if method needs to find queue_name. False: exclude GPU queues. True: include. None: don't care.
        :param withmpi: options field.
        :param queue_name: options field. Queue/partition name. If not given but needed, I will guess it.
        :param account: options field. For configs (computers) with account-based queue assignment (e.g., claix).
        :param kwargs: Optional: other options fields. See :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_help`.
        :return: list of options from selected configs.
        """
        valid_config_names, _, _ = self._get_valid_config_names_from(config_names=config_names, silent=silent)

        options = []
        for config in self.configs:
            if config.name in valid_config_names:
                options.extend(
                    config.get_options(store_if_not_exist=store_if_not_exist, as_Dict=as_Dict, silent=silent,
                                       computer_name=computer_name, gpu=gpu, withmpi=withmpi, queue_name=queue_name,
                                       account=account, **kwargs))

        return options

    def get_help(self, mode: str, *args) -> dict:
        """Get help on valid computer options keywords.

        This method calls the manager's configs' :py:meth:`~aiida_jutools.util_computer._OptionsConfig.get_help` method.
        The only difference of what both return is in the keywords mode. In the keywords mode, this method returns
        all option keywords (fields), and all options 'resources' fields for each config. The latter may differ based
        on the config's associated computer/scheduler.

        :param mode: 'keywords': list all options fields. 'descriptions': full descriptions for all or supplied fields.
        :param args: for descriptions mode. Absent: all descriptions. Present: descriptions only for those fields.
        :return: dict of lists of keywords or of descriptions.
        """
        # in case of description mode, use any config to get options keywords
        # and all configs to get resources keywords
        modes = self._help_config.modes
        if mode not in modes:
            self._log('Warning', self.get_help, f"Undefined mode. Mode must be one of {modes}.")
            return

        if not self.configs:
            self._log('Warning', self.get_help, f"No configs available. Re-instantiate, or add a config.")
            return

        is_mode_keys = (mode == self._help_config.keys_mode)
        is_mode_desc = (mode == self._help_config.desc_mode)

        if is_mode_desc:
            return self.configs[0].get_help(mode, *args)

        if is_mode_keys:
            all_options_keys = None
            resources_keys = {}
            for config in self.configs:
                help = config.get_help(mode=mode)
                if not all_options_keys:
                    all_options_keys = help.pop(self._help_config.keys_mode_return_key_options)
                resources_keys[config.name] = help.pop(self._help_config.keys_mode_return_key_rescources)

            return {self._help_config.keys_mode_return_key_options: all_options_keys,
                    self._help_config.keys_mode_return_key_rescources: resources_keys}

    def delete_options(self, config_names: _List[_AnyStr] = [], options_nodes: _List[Dict] = [], dry_run: bool = True,
                       verbosity: int = 1):
        """Delete some of the specified configs' stored options from the database.

        This methods calls each of the manager's configs' corresponding methods
        :py:meth:`~aiida_jutools.util_computer._OptionsConfig.delete_options` in turn.

        :param config_names: Empty: on all configs. Subset: based on these configs.
        :param options_nodes: list of stored option nodes you want to have deleted.
        :param dry_run: True: only show what I would do. False: do it.
        :param verbosity: node deletion verbosity. 0: silent, 1: number of nodes, 2: full node list.
        """
        valid_config_names, _, _ = self._get_valid_config_names_from(config_names=config_names, silent=(not verbosity))

        for config in self.configs:
            if config.name in valid_config_names:
                config.delete_options(options_nodes=options_nodes, dry_run=dry_run, verbosity=verbosity)

    def add_config(self, config: _OptionsConfig, initialize: bool = False, silent: bool = False):
        """Add a config (set of group-managed options nodes with defaults) to the manager.

        The config will become available as named attribute and will be included in the manager's collective
        configs methods.

        :param config: a properly set up config. shouldn't clash with existing manager configs. no checks performed.
        :param initialize: True: call initialize on the config. Useful if the manager's configs are already inited.
        :param silent: True: do not print out any info. False: only print warnings.
        """
        # DEV TODO: validate config
        print("Currently no check is done if config is configured correctly. So use with caution.")
        if initialize:
            config.initialize(silent=silent)
        setattr(self, config.name, config)
        if not silent:
            print(f"Added computer options config: {config.name}.")


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
    from typing import AnyStr as _AnyStr, List as _List
    command: _AnyStr = "quota"
    header_line_count: int = 1
    column_space_used: _AnyStr = "used"
    column_space_hard: _AnyStr = "hard"
    dirname_pattern: _AnyStr = ""
    min_free_space: _AnyStr = "10G"
    comment: _List[_AnyStr] = python_util.dataclass_default_field([""])


class QuotaQuerier:
    def __init__(self, computer: Computer, settings: QuotaQuerierSettings):
        """Convenience methods for getting the quota of aiida configured computer nodes.

        Use this class' builder :py:class:`~aiida_jutools.util_computer.QuotaQuerierBuilder` to create an instance.

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
