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
"""Tools for working with aiida Node objects: utils."""
import functools as _functools
import operator as _operator
import typing as _typing

from aiida import orm as _orm


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


def get_from_nested_dict(a_dict: dict,
                         keypath: list) -> _typing.Any:
    """Get value from inside a nested dictionary.

    Example: `(nested_dict, ['key_on_level1', 'key_on_level2'])` returns value of 'key_on_level2'.

    :param a_dict: A nested dictionary.
    :param keypath: A list of keys, one per nesting level.
    :return: value of the last key in the path.
    """
    return _functools.reduce(_operator.getitem, keypath, a_dict)


def set_in_nested_dict(a_dict: dict,
                       keypath: list,
                       value: _typing.Any):
    """Set value inside a nested dictionary.

    Example: `(nested_dict, ['key_on_level1', 'key_on_level2'], sets)` value of 'key_on_level2'.

    :param a_dict: A nested dictionary.
    :param keypath: A list of keys, one per nesting level.
    :param value: value for the last key in the path.
    """
    if keypath:
        if len(keypath) == 1:
            a_dict[keypath[0]] = value
        else:
            get_from_nested_dict(a_dict, keypath[:-1])[keypath[-1]] = value


def get_from_nested_node(node: _orm.Node,
                         keypath: list) -> _typing.Tuple[_typing.Any, _typing.Optional[Exception]]:
    """Get value from a node, including values nested inside attached `Dict` nodes or attached dictionaries.

    Examples:

    - `(node, ['uuid'])` returns uuid from node.
    - `(node, ['extras', 'my_extra', 'my_subextra'])` returns value of 'my_subextra' from node extras.
    - `(a_workchain, ['outputs', 'workflow_info', 'converged'])` returns value of 'converged' from workchain outputs
      node 'workflow_info.
    - `(node, ['attributes', 'key_level1', 'key_level2'])` returns value of 'key_level2' from `Dict` node.
    - `(cif, ['get_content'])` returns file content from `CifData` node.

    If the node member at the start of the keypath is not an attribute but a method which can be called without
    parameters, and if it returns a node or dict, then the keypath gets followed down that structure as well.

    :param node: An AiiDA node.
    :param keypath: A list of keys, one per nesting level. First nesting level is the node attribute name. In case
                    of inputs and outputs node, the second key is 'inputs' or 'outputs' and the third the input or
                    output name that `node.outputs.` displays on tab completion (which, under the hood, comes from the
                    in- or outgoing link_label).
    :return: value of the last key in the path.
    """
    if not keypath:
        err = KeyError("Keypath is empty.")
        return None, err

    # first item in keypath always refers to a node attribute.
    attr = None
    attr_name = keypath[0]
    try:
        attr = getattr(node, attr_name)
    except AttributeError as err:
        return None, err

    if len(keypath) == 1:
        return attr, None

    elif isinstance(attr, dict):
        # applies e.g. to extras
        a_dict = attr
        try:
            value = get_from_nested_dict(a_dict=a_dict,
                                         keypath=keypath[1:])
            return value, None
        except KeyError as err:
            return None, err

    elif isinstance(attr, _orm.Dict):
        # applies e.g. to extras
        a_dict = attr.attributes
        try:
            value = get_from_nested_dict(a_dict=a_dict,
                                         keypath=keypath[1:])
            return value, None
        except KeyError as err:
            return None, err

    elif attr_name in ['inputs', 'outputs']:
        in_or_outputs = attr
        link_label = keypath[1]
        # note: link_label are the outputs 'names'. another way to get them is
        # link_triples = node.get_outgoing(node_class=_orm.Dict).all()
        # out_dicts = {lt.link_label: lt.node.attributes for lt in link_triples}
        # ####
        try:
            io_node = getattr(in_or_outputs, link_label)
        except AttributeError as err:
            return None, err
        if not isinstance(io_node, _orm.Dict):
            err = TypeError(f"{attr_name} node {link_label} is not {_orm.Dict}. Not supported.")
            return None, err

        in_or_out_dict = io_node.attributes
        try:
            value = get_from_nested_dict(a_dict=in_or_out_dict,
                                         keypath=keypath[2:])
            return value, None
        except KeyError as err:
            return None, err

    elif callable(attr):
        try:
            # only works with parameter-less functions
            called_attr = attr()
            if isinstance(called_attr, _orm.Node):
                value, err = get_from_nested_node(node=called_attr,
                                                  keypath=keypath[1:])
                return value, err
            elif isinstance(called_attr, dict):
                value = get_from_nested_dict(a_dict=called_attr,
                                             keypath=keypath[1:])
                return value, None
        except TypeError as err:
            return None, err

    else:
        err = ValueError(f"Reading sub-properties from node attribute '{keypath[0]}' is not supported yet.")
        return None, err


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
