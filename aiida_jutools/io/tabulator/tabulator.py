# -*- coding: utf-8 -*-
# pylint: disable=unused-import
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
"""Tools for working with AiiDA IO: tabulation: Tabulator."""

import abc as _abc
import copy as _copy
import json
import json as _json
import typing as _typing

import aiida.orm as _orm
import pandas as _pd
from masci_tools.util import python_util as _masci_python_util
from masci_tools.io.parsers.tabulator import Recipe, Tabulator, TransformedValue

import aiida_jutools as _jutools


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
                 recipe: Recipe = None,
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

        :param recipe: A recipe with optionally things like include list, exclude list, transformer.
        :param unpack_dicts_level: Include dict properties up to this nesting level.
        :param unpack_inputs_level: Include inputs properties up to this nesting level.
        :param unpack_outputs_level: Include outputs properties up to this nesting level.
        :param kwargs: Additional keyword arguments for subclasses.
        """
        super().__init__(recipe=recipe,
                         **kwargs)

        self._table_types = [
            dict,
            _pd.DataFrame
        ]

        self._pandas_column_policies = [
            'flat',
            'flat_full_path',
            'multiindex'
        ]

        self._autolist_search_paths = {
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
            ],
            _orm.StructureData: [
                "attributes",
                "cell",
                "cell_angles",
                "cell_lengths",
                "get_cell_volume",
                "get_formula",
                "kinds",
                "pbc",
                "sites",
            ]
        }
        self._autolist_unpack_levels = {
            dict: 2,
            _orm.Dict: 2,
            'inputs': 3,
            'outputs': 3
        }

    @property
    def autolist_search_paths(self) -> _typing.Dict[_typing.Type[_orm.Node], _typing.List[str]]:
        """The autolist search paths is a list of node types and top-level property string names
        (node attributes).
        The autolist method only searches down these top-level paths."""
        return self._autolist_search_paths

    @autolist_search_paths.setter
    def autolist_search_paths(self,
                              search_paths: _typing.Dict[_typing.Type[_orm.Node], _typing.List[str]]):
        assert all(issubclass(key, _orm.Entity) for key in search_paths.keys())
        assert all(isinstance(value, list) for value in search_paths.values())

        self._autolist_search_paths = search_paths

    @property
    def autolist_unpack_levels(self) -> _typing.Dict[_typing.Any, int]:
        """The autolist unpack levels specify at which nesting level the autolist method should stop to
        unpack properties."""
        return self._autolist_unpack_levels

    @autolist_unpack_levels.setter
    def autolist_unpack_levels(self,
                               unpack_levels: _typing.Dict[_typing.Any, int]):
        assert all(isinstance(value, int) for value in unpack_levels.values())
        assert all(key in unpack_levels for key in [dict, _orm.Dict, 'inputs', 'outputs'])

        self._autolist_unpack_levels = unpack_levels

    def autolist(self,
                 obj: _orm.Node,
                 overwrite: bool = False,
                 pretty_print: bool = False,
                 **kwargs):
        """Auto-generate an include list of properties to be tabulated from a given object.

        This can serve as an overview for customized include and exclude lists.

        :param obj: An example object of a type compatible with the tabulator.
        :param overwrite: True: replace recipe list with the auto-generated list. False: Only if recipe list empty.
        :param pretty_print: True: Print the generated list in pretty format.
        :param kwargs: Additional keyword arguments for subclasses.
        """
        if not isinstance(obj, _orm.Node):
            return
        # get all Dict input/output node names.
        node = obj

        include_list = {}

        for node_type, attr_names in self._autolist_search_paths.items():
            if isinstance(node, node_type):
                for attr_name in attr_names:
                    try:
                        attr = getattr(node, attr_name)
                    except AttributeError as err:
                        print(f"Warning: Could not get attr '{attr_name}'. Skipping.")
                        continue

                    is_inputs = attr_name == 'inputs'
                    is_outputs = attr_name == 'outputs'
                    is_dict = isinstance(attr, (dict, _orm.Dict)) and attr_name not in ['inputs', 'outputs']
                    is_special = (is_dict or is_inputs or is_outputs)

                    if not is_special:
                        include_list[attr_name] = None
                        continue

                    # now handle the special cases

                    if is_dict:
                        # for instance: node.extras.
                        # note: in future, could use ExtraForm sets here for standardized extras.
                        # get dict structure up to the specified unpacking leve
                        is_aiida_dict = isinstance(attr, _orm.Dict)
                        attr = attr.attributes if is_aiida_dict else attr

                        to_level = self.autolist_unpack_levels[type(attr)]
                        props = _masci_python_util.modify_dict(a_dict=attr,
                                                               transform_value=lambda v: None,
                                                               to_level=to_level)
                        if is_aiida_dict:
                            include_list[attr_name] = {'attributes': _copy.deepcopy(props)}
                        else:
                            include_list[attr_name] = _copy.deepcopy(props)

                    if is_inputs or is_outputs:
                        # get all Dict output link triples
                        link_triples = node.get_incoming(node_class=_orm.Dict).all() \
                            if is_inputs else node.get_outgoing(node_class=_orm.Dict).all()

                        # Now get all keys in all input/output `Dicts`, sorted alphabetically.
                        all_io_dicts = {lt.link_label: lt.node.attributes for lt in link_triples}

                        # now get structure for all the inputs/outputs
                        in_or_out = 'inputs' if is_inputs else 'outputs'
                        to_level = self.autolist_unpack_levels[in_or_out]
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

        if overwrite or not self.recipe.include_list:
            self.recipe.include_list = include_list

    def tabulate(self,
                 collection: _typing.Union[_typing.List[_orm.Node], _orm.Group],
                 table_type: _typing.Union[_typing.Type[dict], _typing.Type[_pd.DataFrame]] = _pd.DataFrame,
                 pandas_column_policy: str = 'flat',
                 pass_node_to_transformer: bool = True,
                 verbose: bool = True,
                 **kwargs) -> _typing.Union[None, dict, _pd.DataFrame]:
        """This method extends :py:meth:`~.Tabulator.tabulate`. See also its docstring.

        For unpacking standardized extras, .:py:class:`~aiida_jutools.meta.extra.ExtraForm` sets may be used.

        :param collection: group or list of nodes.
        :param table_type: table as pandas DataFrame or dict.
        :param pandas_column_policy: 'flat': Flat dataframe, name conflicts produce warnings. 'flat_full_path':
               Flat dataframe, column names are full keypaths, 'multiindex': dataframe with MultiIndex columns,
               reflecting the full properties' path hierarchies.
        :param pass_node_to_transformer: True: Pass current node to transformer. Enables more complex transformations,
                                         but may be slower for large collections.
        :param verbose: True: Print warnings.
        :param kwargs: Additional keyword arguments for subclasses.
        :return: Tabulated objects' properties as dict or pandas DataFrame.
        """

        if table_type not in self._table_types:
            print(f"Warning: Unknown {table_type=}. Choosing default return type "
                  f"{_pd.DataFrame} instead.")
            table_type = _pd.DataFrame

        if table_type == _pd.DataFrame and verbose \
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
        # (via property setter auto-conversion)
        if not self.recipe.include_list:
            self.autolist(obj=node,
                          overwrite=True,
                          pretty_print=False)
        include_keypaths = self.recipe.include_list
        exclude_keypaths = self.recipe.exclude_list
        assert isinstance(include_keypaths, list)
        assert isinstance(exclude_keypaths, list)

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
                    print(f"Warning: Found path name collisions in {in_or_ex}clude keypaths, see list below. "
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

        # now we can finally build the table
        table = {keypath[-1]: [] for keypath in include_keypaths}
        failed_paths = {tuple(keypath): [] for keypath in include_keypaths}
        failed_transforms = {tuple(keypath): [] for keypath in include_keypaths}
        generator = (node for node in group.nodes) if is_group else (node for node in nodes)

        for node in generator:
            print('-----------------')
            print(f'node {node.label}')
            row = {keypath[-1]: None for keypath in include_keypaths}

            for keypath in include_keypaths:
                column = keypath[-1]
                value, err = _jutools.node.get_from_nested_node(node=node,
                                                                keypath=keypath)
                if err:
                    row[column] = None
                    failed_paths[tuple(keypath)].append(node.uuid)
                    continue

                # tmp: testing: print
                # print(f'\n{column}:', end=' ')
                if not self.recipe.transformer:
                    # print(f'no transformer, value: {value}')
                    row[column] = value
                else:
                    try:
                        _node = node if pass_node_to_transformer else None
                        trans_value = self.recipe.transformer.transform(keypath=keypath,
                                                                        value=value,
                                                                        obj=_node,
                                                                        **kwargs)
                        if trans_value.is_transformed:
                            for t_column, t_value in trans_value.value.items():
                                row[t_column] = t_value
                        else:
                            row[column] = trans_value.value

                    except (ValueError, KeyError, TypeError) as err:
                        # print(f'trans failed, setting value None, err: {err}')
                        row[column] = None
                        failed_transforms[tuple(keypath)].append(node.uuid)
                        continue

            for column, value in row.items():
                # if transformer created new columns in row, need to add them here as well first.
                if column not in table:
                    table[column] = []
                table[column].append(value)

        failed_paths = {path: uuids for path, uuids in failed_paths.items() if uuids}
        failed_transforms = {path: uuids for path, uuids in failed_transforms.items() if uuids}
        if verbose:
            # json dumps cannot handle tuple keys. so for print, convert to a string.
            if failed_paths:
                failed_paths = {str(list(path)): uuids for path, uuids in failed_paths.items()}
                print(f"Warning: Failed to tabulate keypaths for some nodes:\n"
                      f"{json.dumps(failed_paths, indent=4)}")
            if failed_transforms:
                failed_transforms = {str(list(path)): uuids for path, uuids in failed_transforms.items()}
                print(f"Warning: Failed to transform keypath values for some nodes:\n"
                      f"{json.dumps(failed_transforms, indent=4)}")

        if table_type == _pd.DataFrame:
            self._table = _pd.DataFrame.from_dict(table)
        else:
            # dict
            self._table = table

        return self.table
