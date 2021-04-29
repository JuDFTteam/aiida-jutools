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

import pprint
import typing

from aiida.orm import Node

simple_types = [int, bool, float, complex, list, tuple, set]
# define attribute lists of some aiida types. of course these lists are non-exhaustive.
# selected for interesting stuff, inspired by aiida cheat sheet / this tutorial.
attribute_string_lists: typing.Dict[str, typing.List[str]] = {
    "StructureData": ["cell", "sites", "kinds", "pbc", "get_formula", "get_cell_volume",
                      "cell_angles", "cell_lengths",
                      "attributes"],
    "CifData": ["get_content"
                ],
    "kkrparams": ["get_all_mandatory", "get_missing_keys"],
    "ProcessBuilder": ["metadata", "parameters", "parent_KKR", "potential_overwrite", "structure"]
}


def is_same_node(node: Node, other: Node, comparator: str = "uuid"):
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
    :rtype: bool
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


def intersection(nodes: typing.List[Node], others: typing.List[Node]):
    """Computes intersection set of nodes from both lists.

    DEVNOTE: outer loop over longer list seems to guarantee symmetry.
    (without it, computing difference list(set(longer)-set(intersection))==shorter seems to not be guaranteed.)

    :param nodes:
    :param others:
    :return:
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


def print_attributes(obj, obj_name, attr_str_list):
    """easily print-inspect the values of an aiida object we created.

    :param obj: aiida object
    :param obj_name: name
    :param attr_str_list: attributes

    :example:

    >>> from aiida.orm import StructureData
    >>> from aiida.plugins import DataFactory
    >>> StructureData = DataFactory('structure')
    >>> # fill in values for copper...
    >>> Cu29 = StructureData()
    >>> print_attributes(Cu29, "Cu", attribute_string_lists["StructureData"])
    """
    pp = pprint.PrettyPrinter(indent=4)
    sep = "-------------------------------"
    print(sep)
    print(f"Attributes of object '{obj_name}' of type {type(obj)}:")
    print(sep)
    for attr_str in attr_str_list:
        try:
            attr = getattr(obj, attr_str)
            if callable(attr):
                try:
                    print(f"{obj_name}.{attr_str}: {attr()}")
                except TypeError as err:
                    print(f"{obj_name}.{attr_str}: needs additional input arguments")
            elif type(attr) in simple_types:
                print(f"{obj_name}.{attr_str}: {attr}")
            else:
                print(f"{obj_name}.{attr_str}:")
                pp.pprint(attr)
        except AttributeError as err:
            print(f"{obj_name}.{attr_str}: no such attribute")
    print(sep)


def list_differences(calculation_sequence: list, node_type: Node, member_name: str, outgoing: bool = True):
    """Print attributes (e.g. output files) of nodes in list, and only differences in list between subsequent nodes.

    Note for comparing Dict nodes, prefer library DeepDiff.

    DEVNOTE: TODO redo as tree algorithm navigating via `get_incoming(KkrCalculation)` / `get_outgoing(RemoteData)`
    given a sinlge node instead of via node list.

    :param calculation_sequence:
    :param node_type:
    :param member_name:
    :param outgoing:

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

    def print_items(items):
        if type(items) == list:
            for item in items:
                print(f"\t{item}")
        elif type(items) == dict:
            for k, v in items.items():
                print(f"\t{k} : {v}")

    items = {}
    title = f"Output {node_type}.{member_name} items:"
    sep = "-"
    print("{}\n{:{fill}^{w}}".format(title, sep, fill=sep, w=len(title)))

    for i, calc in enumerate(calculation_sequence):
        if not outgoing:
            node = calc.get_incoming(node_type).all_nodes()[0]
        else:
            node = calc.get_outgoing(node_type).all_nodes()[0]
        return_items = getattr(node, member_name)
        if callable(return_items):  # i.e. member is a method, not an attribute
            return_items = return_items()
        if type(return_items) == list:
            return_items = sorted(return_items)
        items[calc] = return_items
        if i == 0:
            print(f"...of calc. no.{i} '{calc.label}':")
            print_items(items[calc])
            continue

        parent_calc = calculation_sequence[i - 1]
        print(f"additional items of calc. no.{i} '{calc.label}':")
        difference = sorted(list(set(items[calc]) - set(items[parent_calc])))
        if type(items[calc]) == dict:
            keys = difference.copy()
            # get all k,v pairs from difference list
            difference = {}
            for k in keys:
                difference[k] = items[calc][k]
        print_items(difference)
    print("\n")
