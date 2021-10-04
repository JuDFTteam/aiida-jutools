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
"""Tools for working with aiida Node objects : tabulation (Nodes -> DataFrame)."""

import copy as _copy
import json
import json as _json
import pandas as _pd
import aiida.orm as _orm
import numpy as _np
from masci_tools.util import python_util as _masci_python_util
import abc as _abc
import typing as _typing
import enum as _enum

import functools as _functools
import operator as _operator


def get_from_nested_dict(a_dict: dict, path: list) -> _typing.Any:
    """Example: `(a_dict, ['key_on_level1', 'key_on_level2'])`."""
    return _functools.reduce(_operator.getitem, path, a_dict)


def set_in_nested_dict(a_dict: dict, path: list, value: _typing.Any):
    """Example: `(a_dict, ['key_on_level1', 'key_on_level2'], value)`."""
    if path:
        if len(path) == 1:
            a_dict[path[0]] = value
        else:
            get_from_nested_dict(a_dict, path[:-1])[path[-1]] = value


class PropertyTransformer():
    """Specify how to transformer an object's properties for use in :py:class:`Tabulator`.

    To subclass, you have to implement the :py:meth:`~transformer` method.
    """

    def transform(self, keypath: _typing.Union[str, _typing.List[str]],
                  value: _typing.Any,
                  obj: _typing.Any = None,
                  **kwargs) -> _typing.Tuple[_typing.Union[None, _typing.Any, dict], bool]:
        """Specify how to transform properties, based on their property path and type.

        This default transformer returns all property values unchanged, and so has no effect. To define
        transformations, create a subclass and overwrite with a custom transform method.

         Example: Say, a nested dictionary is passed. It has a property
         `a_dict:{outputs:{last_calc_output_parameters:{attributes:{total_charge_per_atom:[...]`, which
         is a numerical list. We would like the list, and its maximum tabulated as individual columns.
         All other properties included in the tabulation shall be not transformed. We would write
         that transformation rule like this.

        .. code-block:: python

           if property_path == ['outputs', 'last_calc_output_parameters', 'total_charge_per_atom']:
               # assert isinstance(value, list) # optional
               return { 'total_charge_per_atom' : value, 'maximum_total_charge' : max(value) }
           return value

        All kinds of transformation rules for all kinds of properties can be tailored in this way
        to the specific use-case. Keep in mind that if a include list is used, the property (path) has
        to be included in the include list.

        :param keypath:
        :param value: The value of the current property.
        :param obj: Optionally, the object containing the property can be passed along. This enables to
                     transform the current value in combination with other property values.
        :param kwargs: Additional keyword arguments for subclasses.
        :return: A tuple (transformed_value:object, with_new_columns:bool). If the latter is False, this 
                 means the transformed output property has the same name as the input property (in/out referring
                 here to the input/output of the transform method, not the input/output properties of the object). If
                 the former is a dict, and the latter is set to True, this is understood as such that new output
                 properties were created from the input property, and the output should be interpreted as
                 {property_name: property_value}, possibly with one being the original property name. In a tabulator, 
                 new columns would be created for these new properties.    
        """
        return value, False


class Tabulator(_abc.ABC):
    """For tabulation of a collection of objects' (common) properties into a dict or dataframe."""

    def __init__(self,
                 exclude_list: dict = {},
                 include_list: dict = {},
                 transformer: PropertyTransformer = None,
                 **kwargs):
        """Initialize a tabulator object.

        The attributes :py:attr:`~.include_list` and :py:attr:`~.exclude_list` control whic properties
        are to be tabulated. They may be set in a derived class definition, or at runtime on an instance.

        Subclasses define the nature of the objects to be tabulated by making assumptions on their
        property structure. That way, if both include and exclude list are empty, by default the 'complete'
        set of properties of the objects will be tabulated, where the subclass defines the notion of 'complete'.

        If neither exclude nor include list is given, the full set of properties according to implementation
        will be tabulated.

        Format of the include and exclude lists:



        :param exclude_list: Optional list of properties to exclude. May be set later.
        :param include_list: Optional list of properties to include. May be set later.
        :param transform: Specifies special transformations for certain properties for tabulation.
        :param kwargs: Additional keyword arguments for subclasses.
        """
        # note: for the in/ex lists, using the public setter here,
        # to trigger conversion
        self.exclude_list = exclude_list
        self.include_list = include_list
        self._transformer = transformer

        self._scalar_types = (bool, int, float, str, complex)
        self._nonscalar_types = (list, dict, tuple, set, _np.ndarray)
        self._nested_types = tuple([dict])
        self._simple_types = tuple(
            set(self._scalar_types).union(
                set(self._nonscalar_types)).union(
                set(self._nested_types))
        )

    @_abc.abstractmethod
    def default_include_list(self,
                             obj: _typing.Any,
                             **kwargs) -> _typing.Optional[dict]:
        """Create the complete list of properties tabulatable from a given object.

        This gives an overview of the complete list of properties the tabulator implementation would extract,
        if no restriction is given by a custom include list.

        The returned dictionary can be used as template to define a smaller, customized include list.

        If no include list is provided, this will taken as the default include list.

        :param obj: An example object of a type which the tabulator knows how to tabulate.
        :param kwargs: Additional keyword arguments for subclasses.
        :return: an include list of all properties this tabulator knows to extract. None if error.
        """
        pass

    @staticmethod
    def _withList_to_withNone_format(a_dict: dict,
                                     unpack_level: int = 99) -> dict:
        if not a_dict:
            return {}

        return _masci_python_util.modify_dict(a_dict=a_dict,
                                              transform_value=lambda v: {k: None for k in v}
                                              if isinstance(v, list) else v,
                                              to_level=unpack_level)

    @staticmethod
    def _check_keypaths(a_list: list,
                        in_or_ex: str) -> bool:
        is_list = isinstance(a_list, list)
        is_all_lists = is_list and all(isinstance(path, list) for path in a_list)
        if not is_all_lists:
            raise TypeError(f"Could not generate keypaths of required type {_typing.List[list]} "
                            f"from {in_or_ex}clude list. Either specified list in wrong format "
                            f"(see class init docstring for examples), or lists stumbled over untreated special "
                            f"case for some unpacked property.")
        return is_all_lists

    @property
    def exclude_list(self) -> dict:
        return self._exclude_list

    @exclude_list.setter
    def exclude_list(self, exclude_list: _typing.Union[dict, list]):
        self._exclude_list = exclude_list
        if isinstance(exclude_list, dict):
            # convert from with-List to with-None format for convert to keypaths
            # convert to keypaths (upper: done inside this one anyway)
            self._exclude_list = Tabulator.generate_keypaths(a_dict=self._exclude_list)
        Tabulator._check_keypaths(self._exclude_list, "ex")

    @property
    def include_list(self) -> dict:
        return self._include_list

    @include_list.setter
    def include_list(self, include_list: _typing.Union[dict, list]):
        self._include_list = include_list
        if isinstance(include_list, dict):
            # convert from with-List to with-None format for convert to keypaths
            # convert to keypaths (upper: done inside this one anyway)
            self._include_list = Tabulator.generate_keypaths(self._include_list)
        Tabulator._check_keypaths(self._include_list, "in")

    @staticmethod
    def generate_keypaths(a_dict: dict) -> _typing.List[list]:
        """Generate paths from a possibly nested dictionary.

        This method can be used for handling include lists, exclude lists, and when writing
        new :py:class:`~PropertyTransformer` transform methods.

        :param a_dict: a dictionary.
        :return: List of paths to each value within the dict as tuples (path, value).
        """
        if not a_dict:
            return []

        # convert from include list with-list format with-none format:
        # same-level subkeys mentioned as list [k1,k2] -> dict {k1:None, k2:None}.
        _a_dict = Tabulator._withList_to_withNone_format(a_dict=a_dict)

        # _a_dict = a_dict

        def _keypaths_recursive(sub_dict: dict,
                                path: list = []):
            paths = []
            for k, v in sub_dict.items():
                if isinstance(v, dict):
                    paths += _keypaths_recursive(v, path + [k])
                paths.append((path + [k], v))
            return paths

        keypaths = _keypaths_recursive(sub_dict=_a_dict,
                                       path=[])
        # the result consists of sets of subpaths. For each subset, there is
        # an additianal entry where the value contains the whole subdict from
        # which the paths were generated. We are not interested in those duplicate
        # entries, so remove them.
        keypaths = [tup for tup in keypaths if not isinstance(tup[1], dict)]

        # now list should be like [(path1, None), (path2, None), ...].
        # check that. if not, something is wrong.
        # otherwise, just return the paths.
        if all(tup[1] is None for tup in keypaths):
            return [tup[0] for tup in keypaths]
        else:
            print(f"Warning: To be able to reduce return type just to the paths, I expected format to "
                  f"arrive at format {_typing.List[_typing.Tuple[list, _typing.Any]]}, but didn't. "
                  f"Something went wrong. Instead, I will return this unreduced format.")
            return keypaths

    @_abc.abstractmethod
    def tabulate(self,
                 collection: _typing.Any,
                 return_type: _typing.Type = _pd.DataFrame,
                 **kwargs) -> _typing.Optional[_typing.Any]:
        """Tabulate the common properties of a collection of objects.

        :param collection: collection of objects with same set of properties.
        :param return_type: Type of the tabulated data. Usually a pandas DataFrame or a dict.
        :param kwargs: Additional keyword arguments for subclasses.
        :return: Tabulated objects' properties.
        """
        pass


class NodePropertyTransformer(PropertyTransformer):
    """Specify how to transformer a node's properties for use in :py:class:`~.NodeTabulator`."""

    def transform(self, keypath: _typing.Union[str, _typing.List[str]],
                  value: _typing.Any,
                  obj: _orm.Node = None,
                  **kwargs) -> _typing.Tuple[_typing.Union[None, _typing.Any, dict], bool]:
        """Specify how to transform properties, based on their property path and type.
        
        Extends :py:meth:`~.PropertyTransformer.transform`. See also its docstring.

        This default transformer returns all property values unchanged, and so has no effect. To define
        transformations, create a subclass and overwrite with a custom transform method.

         Example: Say, a nested dictionary is passed. It has a property
         `a_dict:{outputs:{last_calc_output_parameters:{attributes:{total_charge_per_atom:[...]`, which
         is a numerical list. We would like the list, and its maximum tabulated as individual columns.
         All other properties included in the tabulation shall be not transformed. We would write
         that transformation rule like this.

        .. code-block:: python

           if property_path == ['outputs', 'last_calc_output_parameters', 'total_charge_per_atom']:
               # assert isinstance(value, list) # optional
               return { 'total_charge_per_atom' : value, 'maximum_total_charge' : max(value) }
           return value

        All kinds of transformation rules for all kinds of properties can be tailored in this way
        to the specific use-case. Keep in mind that if a include list is used, the property (path) has
        to be included in the include list.

        :param keypath:
        :param value: The value of the current property.
        :param obj: Optionally, the object containing the property can be passed along. This enables to
                     transform the current property value in combination with other property values.
        :param kwargs: Additional keyword arguments for subclasses.
        :return: A tuple (transformed_value:object, with_new_columns:bool). If the latter is False, this 
                 means the transformed output property has the same name as the input property (in/out referring
                 here to the input/output of the transform method, not the input/output properties of the object). If
                 the former is a dict, and the latter is set to True, this is understood as such that new output
                 properties were created from the input property, and the output should be interpreted as
                 {property_name: property_value}, possibly with one being the original property name. In a tabulator,
                 new columns would be created for these new properties.
        """
        return value, False


class NodeTabulator(Tabulator):
    """For tabulation of a collection of nodes' (of same type) properties into a dict or dataframe.

    Class extends :py:class:`~.Tabulator`. See also its docstring.

    The top-level properties tabulated by default can be seen by calling :py:attr:`~node_type_include_list`.

    From `.inputs` / `.outputs` nodes, only properties (attributes) of :py:class:`~aiida.orm.Dict` nodes get
    included in the tabulation.

    Nested properties get unpacked and their subproperties tabulated unto a certain level.

    When defining include list or exclude list, this unpacking limit has to be taken into accound.

    In the transformer, a higher-level nested property may shadow subproperties such that they do not get
    unpacked or transformed.
    """

    def __init__(self,
                 exclude_list: dict = {},
                 include_list: dict = {},
                 transformer: NodePropertyTransformer = None,
                 unpack_extras_level: int = 2,
                 unpack_inputs_level: int = 3,
                 unpack_outputs_level: int = 3,
                 **kwargs):
        """Init node tabulator.

        Class extends :py:class:`~.Tabulator`. See also its docstring.

        The attributes :py:attr:`~.include_list` and :py:attr:`~.exclude_list` control whic properties
        are to be tabulated. They may be set in a derived class definition, or at runtime on an instance.

        Subclasses define the nature of the objects to be tabulated by making assumptions on their
        property structure. That way, if both include and exclude list are empty, by default the 'complete'
        set of properties of the objects will be tabulated, where the subclass defines the notion of 'complete'.

        If neither exlude nor include list is given, the full set of properties according to implementation
        will be tabulated.

        :param exclude_list: Optional list of properties to exclude. May be set later.
        :param include_list: Optional list of properties to include. May be set later.
        :param transformer: Specifies special transformations for certain properties for tabulation.
        :param unpack_extras_level: Include extras properties up to this nesting level.
        :param unpack_inputs_level: Include inputs properties up to this nesting level.
        :param unpack_outputs_level: Include outputs properties up to this nesting level.
        :param kwargs: Additional keyword arguments for subclasses.
        """
        super().__init__(exclude_list=exclude_list,
                         include_list=include_list,
                         transformer=transformer,
                         **kwargs)
        self._unpack_extras_level = unpack_extras_level
        self._unpack_inputs_level = unpack_inputs_level
        self._unpack_outputs_level = unpack_outputs_level

        self._return_types = [
            dict,
            _pd.DataFrame
        ]

        self._pandas_column_policies = [
            'flat',
            'flat_full_path',
            'multiindex'
        ]

        self._node_type_include_list = {
            _orm.Node: [
                'uuid',
                'label',
                'extras',
            ],
            _orm.ProcessNode: [
                'inputs',
                'outputs',
                'process_label',
                'process_state',
                'exit_status'
            ]
        }

    @property
    def node_type_include_list(self) -> _typing.Dict[_typing.Type[_orm.Node], _typing.List[str]]:
        """The node type include list is a list of node types and top-level property string names
        (node attributes).
        The include and exclude list can only contain properties or subproperties of the attributes listed here.
        Note that 'include_list' and 'exclude_list' are a different concept. For that, refer to
        :py:meth:`~.default_include_list`."""
        return self._node_type_include_list

    @node_type_include_list.setter
    def node_type_include_list(self,
                               node_type_include_list: _typing.Dict[_typing.Type[_orm.Node], _typing.List[str]]):
        self._node_type_include_list = node_type_include_list

    def default_include_list(self, obj: _orm.Node,
                             pretty_print: bool = False,
                             **kwargs) -> _typing.Optional[dict]:
        """Create the complete list of properties tabulatable from a given object.

        This gives an overview of the complete list of properties the tabulator implementation would extract,
        if no restriction is given by a custom include list.

        The returned dictionary can be used as template to define a smaller, customized include list.

        If no include list is provided, this will taken as the default include list.

        :param obj: An example object of a type which the tabulator knows how to tabulate.
        :param pretty_print: True: Print the list in pretty format.
        :param kwargs: Additional keyword arguments for subclasses.
        :return: an include list of all properties this tabulator knows to extract. None if error.
        """
        if not isinstance(obj, _orm.Node):
            return
        # get all Dict input/output node names.
        node = obj

        include_list = {}

        for node_type, attr_names in self._node_type_include_list.items():
            for attr_name in attr_names:
                is_extras = (issubclass(node_type, _orm.Node) and attr_name == 'extras')
                is_inputs = (issubclass(node_type, _orm.ProcessNode) and attr_name == 'inputs')
                is_outputs = (issubclass(node_type, _orm.ProcessNode) and attr_name == 'outputs')
                is_special = (is_extras or is_inputs or is_outputs)

                try:
                    attr = getattr(node, attr_name)
                except AttributeError as err:
                    print(f"Warning: Could not get attr '{attr_name}'. Skipping.")
                    continue

                if not is_special:
                    include_list[attr_name] = None
                    continue

                # now handle the special cases

                if is_extras:
                    # note: in future, could use ExtraForm sets here for standardized extras.
                    # get extras structure up to the specified unpacking leve
                    extras = attr
                    props = _masci_python_util.modify_dict(a_dict=extras,
                                                           transform_value=lambda v: None,
                                                           to_level=self._unpack_extras_level)
                    include_list[attr_name] = _copy.deepcopy(props)

                if is_inputs or is_outputs:
                    # get all Dict output link triples
                    link_triples = node.get_incoming(node_class=_orm.Dict).all() \
                        if is_inputs else node.get_outgoing(node_class=_orm.Dict).all()

                    # Now get all keys in all input/output `Dicts`, sorted alphabetically.
                    all_io_dicts = {
                        lt.link_label: lt.node.attributes for lt in link_triples
                    }

                    # now get structure for all the inputs/outputs
                    to_level = self._unpack_inputs_level \
                        if is_inputs else self._unpack_outputs_level
                    props = {
                        link_label: _masci_python_util.modify_dict(a_dict=out_dict,
                                                                   transform_value=lambda v: None,
                                                                   to_level=to_level)
                        for link_label, out_dict in all_io_dicts.items()
                    }
                    include_list[attr_name] = _copy.deepcopy(props)

        if pretty_print:
            print(_json.dumps(include_list,
                              indent=4))

        if not self.include_list:
            # note: using the public setter here, to trigger
            # automatic conversion
            self.include_list = include_list
        return include_list

    def tabulate(self,
                 collection: _typing.Union[_typing.List[_orm.Node], _orm.Group],
                 return_type: _typing.Union[_typing.Type[dict], _typing.Type[_pd.DataFrame]] = _pd.DataFrame,
                 pandas_column_policy: str = 'flat',
                 pass_node_to_transformer: bool = True,
                 verbose: bool = True,
                 **kwargs) -> _typing.Union[None, dict, _pd.DataFrame]:
        """This method extends :py:meth:`~.Tabulator.tabulate`. See also its docstring.

        For unpacking standardized extras, .:py:class:`~aiida_jutools.meta.extra.ExtraForm` sets may be used.

        :param collection: group or list of nodes.
        :param return_type: table as pandas DataFrame or dict.
        :param pandas_column_policy: 'flat': Flat dataframe, name conflicts produce warnings. 'flat_full_path':
               Flat dataframe, column names are full property paths separated by '_', 'multiindex': dataframe
               with MultiIndex columns, reflecting the full properties' path hierarchies.
        :param pass_node_to_transformer: True: Pass current node to transformer. Enables more complex transformations,
                                         but may be slower for large collections.
        :param verbose: True: Print warnings.
        :param kwargs: Additional keyword arguments for subclasses.
        :return: Tabulated objects' properties as dict or pandas DataFrame.
        """

        if return_type not in self._return_types:
            print(f"Warning: Unknown {return_type=}. Choosing default return type "
                  f"{_pd.DataFrame} instead.")
            return_type = _pd.DataFrame

        if return_type == _pd.DataFrame and verbose \
                and (pandas_column_policy not in self._pandas_column_policies
                     or pandas_column_policy in {'flat_full_path', 'multiindex'}):
            print(f"Warning: Unknown pandas column policy '{pandas_column_policy}'. Will switch to "
                  f"default policy 'flat'.")
            pandas_column_policy = 'flat'

        assert isinstance(collection, (list, _orm.Group))
        is_group = isinstance(collection, _orm.Group)

        group = None
        nodes = None
        if is_group:
            group = collection
        else:
            nodes = collection

        # get a single node
        node = None
        if is_group:
            for node in group.nodes:
                break
        else:
            node = nodes[0] if nodes else None

        if not node and verbose:
            print(f"Warning: {collection=} is empty. Will do nothing.")
            return

        # get inc/ex lists. assume that they are in valid keypaths format already
        include_keypaths = self._include_list if self._include_list \
            else self.default_include_list(obj=node,
                                           pretty_print=False)
        exclude_keypaths = self._exclude_list

        # in case of flat column policy, print a warning if name collisions exist
        def remove_collisions(keypaths: list,
                              in_or_ex: str):
            name_collisions = {

            }
            for path in keypaths:
                name = path[-1]
                if name not in name_collisions:
                    name_collisions[name] = [path]
                else:
                    name_collisions[name].append(path)
            name_collisions = {name: colls for name, colls in name_collisions.items() if len(colls) > 1}
            if name_collisions:
                print_name_collisions = {name: [_masci_python_util.NoIndent(path) for path in paths]
                                         for name, paths in name_collisions.items()}
                print_name_collisions = _json.dumps(print_name_collisions,
                                                    indent=4,
                                                    cls=_masci_python_util.JSONEncoderTailoredIndent)
                if verbose:
                    print(f"Found path name collisions in {in_or_ex}clude keypaths, see list below. "
                          f"Will select first or shortest path for each and discard the rest.\n"
                          f"{print_name_collisions}")
            for name, paths in name_collisions.items():
                shortest_path = min(paths, key=len)
                paths.remove(shortest_path)
                for path in paths:
                    keypaths.remove(path)

        remove_collisions(include_keypaths, "in")

        # remove excluded paths
        failed_removes = []
        for keypath in exclude_keypaths:
            try:
                include_keypaths.remove(keypath)
            except ValueError as err:
                failed_removes.append(keypath)
        if failed_removes and verbose:
            print(f"Warning: Failed to remove exclude keypaths from include keypaths:\n"
                  f"{failed_removes}")

        # now we cann finally build the table
        table = {keypath[-1]: [] for keypath in include_keypaths}
        failed_paths = {tuple(keypath): [] for keypath in include_keypaths}
        failed_transforms = {tuple(keypath): [] for keypath in include_keypaths}
        generator = (node for node in group.nodes) if is_group else (node for node in nodes)

        for node in generator:
            row = {keypath[-1]: None for keypath in include_keypaths}

            for keypath in include_keypaths:
                column = keypath[-1]
                value = None

                attr = None
                attr_name = keypath[0]
                try:
                    attr = getattr(node, attr_name)
                except AttributeError as err:
                    row[column] = None
                    failed_paths[tuple(keypath)].append(node.uuid)
                    continue

                if len(keypath) == 1:
                    value = attr

                elif attr_name == 'extras':
                    extras = attr
                    try:
                        value = get_from_nested_dict(a_dict=extras,
                                                     path=keypath[1:])
                    except KeyError as err:
                        row[column] = None
                        failed_paths[tuple(keypath)].append(node.uuid)
                        continue

                elif attr_name in ['inputs', 'outputs']:
                    in_or_outputs = attr
                    link_label = keypath[1]
                    try:
                        io_node = getattr(in_or_outputs, link_label)
                    except AttributeError as err:
                        row[column] = None
                        failed_paths[tuple(keypath)].append(node.uuid)
                        continue
                    if not isinstance(io_node, _orm.Dict):
                        row[column] = None
                        failed_paths[tuple(keypath)].append(node.uuid)
                        continue

                    outdict = io_node.attributes
                    try:
                        value = get_from_nested_dict(a_dict=outdict,
                                                     path=keypath[2:])
                    except KeyError as err:
                        row[column] = None
                        failed_paths[tuple(keypath)].append(node.uuid)
                        continue

                try:
                    _node = node if pass_node_to_transformer else None
                    trans_value, with_new_columns = self._transformer.transform(keypath=keypath,
                                                                                value=value,
                                                                                node=_node)
                    if with_new_columns and isinstance(trans_value, dict):
                        for t_column, t_value in trans_value.items():
                            row[t_column] = t_value
                    else:
                        row[column] = trans_value

                except (ValueError, KeyError, TypeError) as err:
                    row[column] = None
                    failed_transforms[tuple(keypath)].append(node.uuid)
                    continue

            for column, value in row.items():
                # if transformer created new columns in row, need to add them here as well first.
                if column not in table:
                    table[column] = []
                table[column].append(value)

        failed_paths = {path: uuids for path, uuids in failed_paths.items() if uuids}
        failed_transforms = {path: uuids for path, uuids in failed_paths.items() if uuids}
        if verbose:
            if failed_paths:
                print(f"Warning: Failed to tabulate keypaths for some nodes:\n"
                      f"{json.dumps(failed_paths, indent=4)}")
            if failed_transforms:
                print(f"Warning: Failed to transform keypath values for some nodes:\n"
                      f"{json.dumps(failed_transforms, indent=4)}")

        if return_type == _pd.DataFrame:
            return _pd.DataFrame.from_dict(table)
        else:
            # return dict
            return table
