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
"""Tools for working with aiida Node objects."""

import pprint as _pprint
import typing as _typing

import aiida.orm as _orm
import aiida.engine as _aiida_engine

from masci_tools.io.kkr_params import kkrparams as _kkrparams

_SIMPLE_TYPES = [
    bool,
    complex,
    float,
    int,
    list,
    set,
    tuple,
]
# define class member lists of some aiida entity types. of course these lists are non-exhaustive.
# selected for interesting stuff, inspired by aiida cheat sheet / this tutorial.
MEMBER_LISTS: _typing.Dict[object, _typing.List[str]] = {
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
    ],
    _orm.CifData: [
        "get_content",
    ],
    _kkrparams: [
        "get_all_mandatory",
        "get_missing_keys",
    ],
    _aiida_engine.ProcessBuilder: [
        "metadata",
        "parameters",
        "parent_KKR",
        "potential_overwrite",
        "structure",
    ]
}


def is_same_node(node: _orm.Node,
                 other: _orm.Node,
                 comparator: str = "uuid") -> bool:
    """Basic node comparator.

    Note: since aiida-core v.1.6.0, the base Node class now evaluates equality based on the node's UUID. Yet,
    class specific, equality relationships will still override the base class behaviour, for example:
    Int(99) == Int(99). In case of doubt, prefer this method.

    References:

    - v1.6.0 https://github.com/aiidateam/aiida-core/blob/develop/CHANGELOG.md#v160---2021-03-15
    - v1.5.2- https://www.nature.com/articles/s41597-020-00638-4

    :param node: a node.
    :param other: another node.
    :param comparator: "uuid" (default), "pk", or "hash" (warning: slow)
    :return: True if same node, False otherwise.
    """
    allowed_comparators = ["uuid", "pk", "hash"]
    if comparator not in allowed_comparators:
        return KeyError(f"Allowed comparators: {allowed_comparators}, used: {comparator}.")

    if comparator == "hash":
        att_node = node.get_hash()
        att_other = other.get_hash()
    else:
        att_node = getattr(node, comparator)
        att_other = getattr(other, comparator)

    return att_node == att_other


def intersection(nodes: _typing.List[_orm.Node],
                 others: _typing.List[_orm.Node]) -> _typing.List[_orm.Node]:
    """Computes intersection set of nodes from both lists.

    DEVNOTE: outer loop over longer list seems to guarantee symmetry.
    (without it, computing difference list(set(longer)-set(intersection))==shorter seems to not be guaranteed.)

    :param nodes:
    :param others:
    :return: intersection
    """
    intersection = []
    if len(nodes) > len(others):
        longer = nodes
        shorter = others
    else:
        longer = others
        shorter = nodes

    for nlo in longer:
        for nsho in shorter:
            if is_same_node(nlo, nsho):
                intersection.append(nlo)
                continue

    return intersection


def print_attributes(aiida_object: object,
                     member_list: _typing.List[str]):
    """Easily print-inspect the values of an aiida object.

    :param aiida_object: aiida object
    :param member_list: attributes or callables (methods) without parameters

    :example:

    >>> from aiida.orm import StructureData
    >>> from aiida.plugins import DataFactory
    >>> StructureData = DataFactory('structure')
    >>> # fill in values for copper...
    >>> Cu29 = StructureData()
    >>> print_attributes(Cu29, "Cu", MEMBER_LISTS["StructureData"])
    """
    label = getattr(aiida_object, 'label', None)
    pk = getattr(aiida_object, 'pk', None)
    full_identifier = f"type {type(aiida_object)}"
    full_identifier += f", pk={pk}" if pk else ""
    full_identifier += f", label='{label}'" if label else ""
    identifier = f"'{label}'" if label else f"{type(aiida_object)}"

    pp = _pprint.PrettyPrinter(indent=4)
    sep = "-------------------------------"
    print(sep)
    print(f"Attributes of entity {full_identifier}:")
    print(sep)
    for attr_str in member_list:
        try:
            attr = getattr(aiida_object, attr_str)
            if callable(attr):
                try:
                    print(f"{identifier}.{attr_str}: {attr()}")
                except TypeError as err:
                    print(f"{identifier}.{attr_str}: needs additional input arguments")
            elif type(attr) in _SIMPLE_TYPES:
                print(f"{identifier}.{attr_str}: {attr}")
            else:
                print(f"{identifier}.{attr_str}:")
                pp.pprint(attr)
        except AttributeError as err:
            print(f"{identifier}.{attr_str}: no such attribute")
    print(sep)


def list_differences(nodes: _typing.List[_orm.Node],
                     subnode_type: _typing.Type[_orm.Node],
                     member_name: str,
                     outgoing: bool = True):
    """Print attributes (e.g. output files of calculations) for first node in list, and only differences from this
    list for subsequent nodes.

    Example: nodes are a list of calculations, and want to see differences in their generated output files lists.

    Note: for comparing Dict nodes, prefer library DeepDiff.

    Note: currently only picks first

    DEVNOTE: TODO redo as tree algorithm navigating via `get_incoming(KkrCalculation)` / `get_outgoing(RemoteData)`
    given a single node instead of via node list.

    :param nodes: list of nodes with incoming or outoing subnodes, e.g. process nodes
    :param subnode_type: incoming or outgoing node type whose member to inspect for differences
    :param member_name: member name of incoming or outgoing node type. attribute or method without parameters.
    :param outgoing: True: outgoing node, else incoming.

    :example:

    >>> from aiida.orm import load_node, Dict, FolderData, RemoteData
    >>> voro_calc = load_node(1)
    >>> kkr_calc = load_node(2)
    >>> kkr_calc_converged = load_node(3)
    >>> hostGF_calc = load_node(4)
    >>> calcs = [voro_calc, kkr_calc, kkr_calc_converged, hostGF_calc]
    >>> list_differences(calcs, RemoteData, "listdir")           # outputs.remote_folder
    >>> list_differences(calcs, FolderData, "list_object_names") # outputs.retrieved
    >>> list_differences(calcs, Dict, "attributes")              # outputs.output_parameters
    """

    def _print_items(items):
        if type(items) == list:
            for item in items:
                print(f"\t{item}")
        elif type(items) == dict:
            for k, v in items.items():
                print(f"\t{k} : {v}")

    items = {}
    title = f"Output {subnode_type}.{member_name} items:"
    sep = "-"
    print("{}\n{:{fill}^{w}}".format(title, sep, fill=sep, w=len(title)))

    for i, node in enumerate(nodes):
        if not outgoing:
            subnode = node.get_incoming(subnode_type).all_nodes()[0]
        else:
            subnode = node.get_outgoing(subnode_type).all_nodes()[0]
        return_items = getattr(subnode, member_name)
        if callable(return_items):  # i.e. member is a method, not an attribute
            return_items = return_items()
        if type(return_items) == list:
            return_items = sorted(return_items)
        items[node] = return_items

        if i == 0:
            print(f"...of node no.{i} '{node.label}':")
            _print_items(items[node])
            continue

        previous_node = nodes[i - 1]
        print(f"additional items of node no.{i} '{node.label}':")
        difference = sorted(list(set(items[node]) - set(items[previous_node])))
        if type(items[node]) == dict:
            keys = difference.copy()
            # get all k,v pairs from difference list
            difference = {}
            for k in keys:
                difference[k] = items[node][k]
        _print_items(difference)
    print("\n")
