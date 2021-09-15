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
"""Tools for working with AiiDA StructureData nodes: utils."""
import typing as _typing

from aiida import orm as _orm
from aiida.tools import groups as _aiida_groups

import aiida_jutools as _jutools


def query_elemental_structure(symbol: str,
                              group: _orm.Group = None) -> _typing.List[_orm.StructureData]:
    """Query structures for a single chemical element.

    :param symbol: chemical element symbo case-senstive, like 'He'
    :param group: optionally, search only within this group
    :return: list of results
    """
    qb = _orm.QueryBuilder()
    if group:
        qb.append(_orm.Group, filters={'label': group.label}, tag='group')
        qb.append(_orm.StructureData, with_group='group', filters={'attributes.kinds.0.name': symbol})
    else:
        qb.append(_orm.StructureData, filters={'attributes.kinds.0.name': symbol})  # more general
        # # alternative: require extras
        # qb.append(StructureData, with_group='group', filters={"extras.symbol": symbol})

    # return qb.first()[0]
    return qb.all(flat=True)
    # # DEVNOTE:
    # # alternative: require extras
    # the following eqvt. solution is ~3x times slower (for a group of ~1e2 structures):
    # return next((structure for structure in structures if structure.extras['symbol']  == symbol), None)


def load_or_rescale_structures(input_structure_group: _orm.Group,
                               output_structure_group_label: str,
                               scale_factor: _orm.Float,
                               set_extra: bool = True,
                               dry_run: bool = True,
                               silent: bool = False) -> _orm.Group:
    """Rescale a group of structures and put them in a new or existing group.

    Only input structures which do not already have a rescaled output structure in the output structure group
    will be rescaled.

    :param input_structure_group: group with StructureData nodes to rescale. Ignores other nodes in the group.
    :param output_structure_group_label: name of group for rescaled structures. Create if not exist.
    :param scale_factor: scale factor with which to scale the lattice constant of the input structure
    :param set_extra: True: set extra 'scale_factor' : scale_factor.value to structures rescaled in this run.
    :param dry_run: default True: perform a dry run and print what the method *would* do.
    :param silent: True: do not print info messages
    :return: output group of rescaled structures
    """
    assert isinstance(scale_factor, _orm.Float)

    would_or_will = "would" if dry_run else "will"

    out_structure_grouppath = _aiida_groups.GroupPath(path=output_structure_group_label)
    out_structure_group, created = out_structure_grouppath.get_or_create_group()

    inp_structures = {node.uuid: node for node in input_structure_group.nodes if isinstance(node, _orm.StructureData)}
    already_rescaled = {}

    if dry_run or not silent:
        print(40 * '-')
        print(f"Task: Rescale {len(inp_structures.keys())} {_orm.StructureData.__name__} nodes in group "
              f"'{input_structure_group.label}' with scale factor {scale_factor.value}.\nPerform dry run: {dry_run}.")

    # try load structures
    out_structures_existing = [node for node in out_structure_group.nodes if isinstance(node, _orm.StructureData)]
    # now pop out the input nodes which already have been rescaled
    for out_struc in out_structures_existing:
        inps_from_out = query_modified_input_structure(modified_structure=out_struc, invariant_kinds=True)
        if inps_from_out:
            uuids = [inp.uuid for inp in inps_from_out]
            for uuid in uuids:
                if uuid in inp_structures:
                    already_rescaled[uuid] = inp_structures.pop(uuid)

    # now rescale the remaining ones
    if dry_run or not silent:
        print(
            f"I {would_or_will} rescale {len(inp_structures.keys())} {_orm.StructureData.__name__} nodes from "
            f"the input group.  I would add the new nodes to output group '{output_structure_group_label}'.\n"
            f"{len(already_rescaled.keys())} {_orm.StructureData.__name__} of the input nodes already have been "
            f"rescaled and added to this output target previously.")
    if not dry_run:
        for inp_structure in inp_structures.values():
            out_structure = _jutools.process_functions.rescale_structure(input_structure=inp_structure,
                                                                         scale_factor=scale_factor)
            if set_extra:
                out_structure.set_extra("scale_factor", scale_factor.value)
            out_structure_group.add_nodes([out_structure])
    if not dry_run and not silent:
        print(
            f"Created {len(inp_structures.keys())} {_orm.StructureData.__name__} nodes and added them to group "
            f"'{output_structure_group_label}'.")
    return out_structure_group


def query_modified_input_structure(modified_structure: _orm.StructureData,
                                   invariant_kinds: bool = False) -> _typing.List[_orm.StructureData]:
    """Given a structure modified via a CalcFunction, query its input structure(s).

    :param modified_structure: structure modified via a single CalcFunction
    :param invariant_kinds: to make query more precise., assume that the 'kinds' attribute has not been modified.
    :return: list of input structures, if any.
    """

    def _filter_from_attribute(attribute: list) -> dict:
        """Unpack a complex attribute into an 'and'-query filter.

        :param attribute: attribute of a node. Assumes list of dicts of simple types or list thereof.
        :return: a query filter for nodes with that attribute and those values
        """
        filters = {'and': []}
        for i, kind in enumerate(attribute):
            for key, value in kind.items():
                if not isinstance(value, list):
                    filters['and'].append({f"attributes.kinds.{i}.{key}": attribute[i][key]})
                else:
                    for j, val in enumerate(value):
                        filters['and'].append({f"attributes.kinds.{i}.{key}.{j}": attribute[i][key][j]})
        return filters

    if not invariant_kinds:
        input_structure_filters = {}
    else:
        output_kinds = modified_structure.attributes['kinds']
        input_structure_filters = _filter_from_attribute(attribute=output_kinds)

    qb = _orm.QueryBuilder()
    # qb.append(Group, filters={'label': output_structure_group.label}, tag='group')
    # qb.append(StructureData, with_group='group', filters={"uuid" : modified_structure.uuid}, tag='out_struc')
    qb.append(_orm.StructureData, filters={"uuid": modified_structure.uuid}, tag='out_struc')
    qb.append(_orm.CalcFunctionNode, with_outgoing='out_struc', tag='calc_fun')
    qb.append(_orm.StructureData, with_outgoing='calc_fun', filters=input_structure_filters)
    return qb.all(flat=True)
