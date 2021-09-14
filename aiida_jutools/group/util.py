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
"""Tools for working with AiiDA Group entities: utils."""

import copy as _copy
import datetime as _datetime
import json as _json
import sys as _sys
import typing as _typing

import aiida as _aiida
import pytz as _pytz
from aiida import orm as _orm
from aiida.tools import groups as _aiida_groups

import logging as _logging


class GroupHierarchyMaker:
    """Load or create a nested group hierarchy from a dictionary. Useful for organizing large data collections.

    TODO: Create a ``GroupHierarchy`` class which takes the top group of a group hierarchy as input, then
          recursively builds a nested runtime object whose attributes are the respective subgroups, with
          attribute name = group label, which supports tab completion (e.g. NameTuples or dataclasses).
          With that object, user can navigate to every desired group instantly. For subgroup labels with
          numeric values in label, offer option to access desired subgroup via method taking resp. numeric
          value. That way, can navigate to subgroup also programmatically. Then HierarchyMaker can return
          such an object on load or create.
    """
    TEMPLATE = {
        "INSERT_IN_ALL": {
            "TO_DEPTH": _sys.maxsize,
            "INSERT": {
                "extras": {
                    "version": "",
                    "generating_code_urls": {},
                    "comment": [""],
                },
            }
        },  # group
        "TEMPLATE": {
            "description": "",
            "SUBGROUPS": {
            }  # subgroups
        }  # group
    }
    _ignored_keys = ["TEMPLATE", "INSERT_IN_ALL"]
    _insert_key = "INSERT_IN_ALL"

    @staticmethod
    def get_template(with_example_group: bool = True,
                     print_dict: bool = True,
                     indent: int = 4) -> dict:
        """Print a valid example group hierarchy with nested groups as input for load or create method.

        :param with_example_group: add a valid example group entry to template.
        :param print_dict: pretty print the template as well as returning it
        :param indent: indent for the printed template
        :return: valid example group structure
        """
        template = _copy.deepcopy(GroupHierarchyMaker.TEMPLATE)
        if with_example_group:
            template["my_base_group1"] = {
                "description": "Short description of this group.",
                "SUBGROUPS": {
                    "my_subgroupA": {
                        "description": "Short description of this group.",
                        "extras": {
                            "local_extra" : ["only set for this subgroup, ",
                                             "as opposed to global extras in 'INSERT_IN_ALL'."]
                        }
                    },  # subgroup
                    "my_subgroupB": {
                        "description": "Short description of this group.",
                    }  # subgroup
                }  # base subgroups
            }  # base group
        if print_dict:
            print(_json.dumps(template, indent=indent))
        return template

    def load_or_create(self,
                       template: dict,
                       overwrite_extras: bool = True) -> _typing.List[_orm.Group]:
        """Given a dictionary describing a group hierarchy, create or load the latter in the database.

        If a group in the hierarchy exists, will just be loaded. But extras will be modified according to dict.

        :param template: group hierarchy. See GroupHierarchyMaker.TEMPLATE for valid template.
        :param overwrite_extras: replace if True, add if False
        :return: list of created or loaded groups
        """
        # TODO validate dict: group hierarchy against class TEMPLATE

        self._to_insert = None
        self._insert_to_depth = None
        if template.get(GroupHierarchyMaker._insert_key, None):
            self._to_insert = template[GroupHierarchyMaker._insert_key].get("INSERT", None)
            self._insert_to_depth = template[GroupHierarchyMaker._insert_key].get("TO_DEPTH", None)
        self._overwrite_extras = overwrite_extras

        depth = 0
        group_path_str = ""
        groups = []
        self._create_or_load(depth=depth,
                             group_path_str=group_path_str,
                             group_structure=template,
                             groups=groups)
        return groups

    def _create_or_load(self,
                        depth: int,
                        group_path_str: str,
                        group_structure: dict,
                        groups: _typing.List[_orm.Group]):
        """Recursively creates groups from possibly nested dict according to GroupHierarchyMaker.TEMPLATE.
        """
        base_path = group_path_str
        for group_label, attrs in group_structure.items():
            if group_label in GroupHierarchyMaker._ignored_keys:
                continue
            group_path_str = base_path + group_label
            group_path = _aiida_groups.GroupPath(group_path_str)
            group, created = group_path.get_or_create_group()
            group.description = attrs["description"]
            if "extras" in self._to_insert and depth <= self._insert_to_depth:
                if self._overwrite_extras:
                    group.set_extra_many(self._to_insert["extras"])
                else:
                    for k, v in self._to_insert["extras"].items():
                        group.set_extra(k, v)
            # let override by local extras
            if "extras" in attrs:
                if self._overwrite_extras:
                    group.set_extra_many(attrs["extras"])
                else:
                    for k, v in attrs["extras"].items():
                        group.set_extra(k, v)
            if "SUBGROUPS" in attrs:
                self._create_or_load(depth=depth + 1,
                                     group_path_str=group_path_str + "/",
                                     group_structure=attrs["SUBGROUPS"],
                                     groups=groups)
            groups.append(group)


def verdi_group_list(projection: _typing.List[str] = ['label', 'id', 'type_string'],
                     with_header: bool = True,
                     label_filter: str = None) -> _typing.List[_typing.List]:
    """Equivalent to CLI ``verdi group list -a`` (minus user mail address).

    :param projection: query projection
    :param with_header: True: first list in return argument is the projection argument
    :param label_filter: optional: only include groups with this substring in their label
    :return: list of lists, one entry per projection value, for each group
    """
    qb = _orm.QueryBuilder()
    groups = qb.append(_orm.Group, project=projection).all()

    if 'label' in projection and label_filter:
        index_of_label = projection.index('label')
        groups = [item for item in groups if label_filter in item[index_of_label]]

    groups.sort(key=lambda item: item[0].lower())

    if with_header:
        groups.insert(0, projection)

    if len(projection) == 1:
        groups = [singlelist[0] for singlelist in groups]

    return groups


def get_subgroups(group: _orm.Group) -> _typing.List[_orm.Group]:
    """Get all subgroups of a group.

    In accordance with aiida GroupPath, the group with label "foo/bar" is a valid subgroup
    of a group with label "foo".

    :param group: a group with possible subgroups
    :return: subgroups
    """
    group_labels = [group.label for group in _orm.Group.objects.all()]
    subgroup_labels = [label for label in group_labels if label.startswith(group.label)
                       and len(label) > len(group.label)]
    return [_orm.Group.get(label=label) for label in subgroup_labels]


def move_nodes(origin: _orm.Group,
               destination: _orm.Group):
    """Move all nodes from one group to another, possibly sub/supergroup.

    :param origin: origin group
    :param destination: destination group

    Note: if the new group does not exit yet, prefer relabling the group with group.label = new_label.
    """
    destination.add_nodes(list(origin.nodes))
    origin.remove_nodes(list(origin.nodes))


def get_nodes(group_label: str) -> _typing.Generator[_orm.Node, None, None]:
    """Get all nodes from given group (or subgroup) by label (path).

    Deprecated: just use group.nodes, or list(group.nodes).

    :param group_label: e.g. for a subgroup, "groupA/subgroupB/subgroupC".
    :return: nodes as generator for efficient iteration (convert via list() to list)
    """
    group = _orm.Group.get(label=group_label)
    return group.nodes


def group_new_nodes(new_group_label: str,
                    blacklist: _typing.List[_typing.Type[_orm.Node]] = [_orm.Code, _orm.Computer],
                    right_date: _datetime.datetime = None, left_date: _datetime.datetime = None) -> \
        _typing.Optional[_orm.Group]:
    """Groups new nodes with ctime in timerange (left_date,right_date] into new group

    If you're working on one project at a time, everytime you finish a project you can use this function to
    group your nodes. I.e. this is a utility function for a time-linear sequential grouping strategy. Letting
    the function find the appropriate time range is the standard / recommended usage. If the group already exists
    and the intended nodes are already added, repeated calls will change nothing.

    :param new_group_label: label of new group/subgroup
    :param blacklist: node types in timerange to exclude from grouping. Normally Code, Computer.
    :param right_date: if not given (usually as datetime.now()), will take right_date = newest ctime, > left_date,
           of any node, ungrouped nodes included.
    :param left_date: if not given, will take left_date=newest ctime of any grouped node
    :return: the new populated, stored, group, or None if no new nodes found
    """

    timezone = _pytz.UTC

    ## step1: find d7=infdate from all *grouped* nodes

    # new group to create or add
    new_path = _aiida_groups.GroupPath(path=new_group_label)
    new_group = new_path.get_or_create_group()[0]

    # get all groups, exclude new group if present
    group_labels = verdi_group_list(projection=['label'], with_header=False)
    try:
        group_labels.remove(new_group_label)
    except ValueError:
        pass
    groups = [_orm.Group.get(label=label) for label in group_labels]

    # find tuple (group,node) with largest node.ctime across all groups
    left_date_computed = _datetime.datetime(year=1, month=1, day=1, tzinfo=_pytz.UTC)
    for group in groups:
        for node in group.nodes:
            left_date_computed = max(left_date_computed, node.ctime)

    if left_date is not None:
        left_date = timezone.localize(left_date)
        if left_date < left_date_computed:
            print(
                f"WARNING: left_date {left_date} < computed left date from groups {left_date_computed}, "
                f"grouping overlap likely.")
        elif left_date > left_date_computed:
            print(
                f"WARNING: left_date {left_date} > computed left date from groups {left_date_computed}, "
                f"leftover ungrouped nodes likely.")
    else:
        left_date = left_date_computed

        ## step2: find d8=maxdate from all nodes newer than d7

    qb = _orm.QueryBuilder()
    new_nodes = qb.append(_orm.Node, filters={'ctime': {'>': left_date}}).all(flat=True)
    if not new_nodes:
        print(f"Info: found no nodes newer than last grouped at date {left_date}. "
              f"Attempting to delete group '{new_group_label}' if empty.")
        delete_groups([new_group_label])
        return None
    else:
        right_date_computed = max(node.ctime for node in new_nodes)

        if right_date is not None:
            right_date = timezone.localize(right_date)
            if right_date < right_date_computed:
                print(
                    f"WARNING: right_date {right_date} < computed right date from groups {right_date_computed}, "
                    f"leftover ungrouped nodes likely.")
        else:
            right_date = right_date_computed

        ## step3: query all nodes in daterange (d7,d8], optional: exclude blacklist

        daterange_filter = {
            'and': [
                {'ctime': {'>': left_date}},  # newer than
                {'ctime': {'<=': right_date}}  # older than
            ]
        }
        qb = _orm.QueryBuilder()
        new_nodes = qb.append(_orm.Node, filters=daterange_filter).distinct().all(flat=True)
        drops = []
        for i, node in enumerate(new_nodes):
            for blacktype in blacklist:
                if isinstance(node, blacktype):
                    drops.append(i)
        for drop in drops:
            new_nodes.pop(drop)

        ## step4: add new nodes to new group

        new_group.add_nodes(new_nodes)
        new_group.store()

        return new_group


def delete_groups(group_labels: _typing.List[str],
                  skip_nonempty_groups: bool = True,
                  silent: bool = False):
    """Delete group(s). Does not delete nodes in group(s). Use delete_groups_with_nodes() for that.

    :param group_labels: list of group labels
    :param skip_nonempty_groups: True: skip them. False: don't skip. Nodes get removed from group, not deleted.
    :param silent: True: do not print information.
    """
    for label in group_labels:
        try:
            group = _orm.Group.get(label=label)
        except _aiida.common.exceptions.NotExistent:
            print(f"Warning: group to delete '{label}' does not exist.")
        else:
            if group.count() > 0 and skip_nonempty_groups:
                if not silent:
                    print(f"Info: Skipping non-empty group<{label}>: contains {group.count()} nodes.")
            else:
                group.clear()  # remove nodes from group
                _orm.Group.objects.delete(group.pk)
                if not silent:
                    print(f"Group '{label}' deleted.")


def delete_groups_with_nodes(group_labels: _typing.List[str],
                             dry_run: bool = True,
                             verbosity: int = _logging.INFO,
                             leave_groups: bool = False):
    """Delete all nodes in each group (including repo files), then delete the groups themselves.

    :param group_labels: list of group labels
    :param dry_run: perform test run. if output looks good, set to false and repeat.
    :param verbosity: 20 = logging.INFO (show node count) (default), 10 = DEBUG (show all uuids), all other: silent.
    :param leave_groups: True: Leave empty groups as is after deleting all nodes in them.
    """
    # get the node delete function used by 'verdi node delete -v -n/-f'
    # DEVNOTE: deprecation warning: aiida-core v1.6.0: replaced aiida.manage.database.delete.nodes.delete_nodes
    # aiida.tools.delete_nodes
    # (reference: https://github.com/aiidateam/aiida-core/blob/develop/CHANGELOG.md#v160---2021-03-15).

    # get full periodic table pandas dataframe from mendeleev
    # DEVNOTE: breaking change in mendeleev v0.7.0: replaced get_table with fetch.fetch_table.
    version = _aiida.__version__
    version_info = tuple(int(num) for num in version.split("."))
    is_aiida_v160_plus = version_info >= (1, 6, 0)

    if is_aiida_v160_plus:
        from aiida.tools import delete_nodes
        # deprecation warning: verbosity argument replaced with setting logger
        from aiida.common.log import AIIDA_LOGGER
        DELETE_LOGGER = AIIDA_LOGGER.getChild('delete')
        DELETE_LOGGER.setLevel(verbosity)
    else:
        from aiida.manage.database.delete.nodes import delete_nodes

    print("Deleting nodes in groups...")

    # get the groups of all stated group labels
    groups = [_orm.Group.get(label=label) for label in group_labels]

    # get the pks of all nodes in all groups
    pks = []
    for group in groups:
        pks += [node.pk for node in group.nodes]

    # delete nodes
    if is_aiida_v160_plus:
        delete_nodes(pks=pks, dry_run=dry_run)
    else:
        delete_nodes(pks=pks, dry_run=dry_run, force=True, verbosity=verbosity)

    # now delete groups
    print(f"Deleting groups: {not leave_groups}...")
    if leave_groups:
        if dry_run:
            print("Dry run: groups unchanged.")
        else:
            are_empty = {group.label: group.is_empty for group in groups}
            print(f"Groups are now empty: {are_empty}")
    elif dry_run:
        print("Dry run: Skipping deleting groups.")
    else:
        delete_groups(group_labels=group_labels)


def get_nodes_by_query(group_label: str,
                       node_type: _typing.Type[_orm.Node] = _orm.Node,
                       return_query: bool = False,
                       return_iter: bool = True,
                       node_tag: str = "") -> _typing.List[_orm.Node]:
    """Get all nodes from given group (or subgroup) by label (path).

    DEVNOTE: this is how it is done on the aiida cheatsheet, via query.
    but don't see why not simply use group.nodes instead.

    :param group_label: e.g. for a subgroup, "groupA/subgroupB/subgroupC".
    :param node_type: filter out nodes in group of this type
    :param return_query: True: return QueryBuilder object instead of result
    :param return_iter: True: return generator, False: return list
    :param node_tag: if return_query, can add tag to node_type for further query building.
    :return: nodes
    """
    qb = _orm.QueryBuilder()

    qb.append(_orm.Group, filters={'label': group_label}, tag='group')
    qb.append(node_type, with_group='group', tag=node_tag, project='*')

    # DEVNOTE: equivalent:
    # qb.append(node_type, tag="nodes", project="*")
    # qb.append(_orm.Group, with_node="nodes", filters={"label": group_label})

    if return_query:
        return qb
    if return_iter:
        return qb.iterall()
    else:
        return qb.all(flat=True)