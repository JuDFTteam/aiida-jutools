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
"""Tools for working with aiida Group entities."""

import sys
import typing
from datetime import datetime

import aiida.orm
import aiida.tools.groups
import pytz

# _all__: visible namespace of the module on import. prevents namespace pollution.
# reference: https://stackoverflow.com/a/7424390/8116031.
__all__ = ("verdi_group_list", "move_nodes", "get_nodes", "group_new_nodes", "delete_groups",
           "delete_groups_with_nodes")


class GroupsFromDict:
    TEMPLATE = {
        "INSERT_IN_ALL": {
            "TO_DEPTH": sys.maxsize,
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
    _ignore_labels = ["TEMPLATE", "INSERT_IN_ALL"]
    _insert_label = "INSERT_IN_ALL"

    def get_template_dict(self, with_example_group: bool = True, print_dict: bool = True, indent: int = 4) -> dict:
        """Print a valid example dict with nested groups as input for load_or_create().

        :param with_example_group: add a valid example group structure to template dict
        :param print_dict: pretty print dict as well as returning it
        :param indent: indent for the printed dict
        :return: template dict
        """
        import copy
        import json
        template = copy.deepcopy(GroupsFromDict.TEMPLATE)
        if with_example_group:
            template["my_base_group1"] = {
                "description": "Short description of this group.",
                "SUBGROUPS": {
                    "my_subgroupA": {
                        "description": "Short description of this group.",

                    },  # subgroup
                    "my_subgroupB": {
                        "description": "Short description of this group.",
                    }  # subgroup
                }  # base subgroups
            }  # base group
        if print_dict:
            print(json.dumps(template, indent=indent))
        return template

    def load_or_create(self, group_labeling: dict, overwrite_extras: bool = True) -> list:
        """Given a dict describing a group structure, create or load these groups.

        If group(s), exist, will just be loaded. But extras will be modified according to dict.

        :param group_labeling: group structure. See GroupFromDict.TEMPLATE for valid template.
        :param overwrite_extras: replace if True, add if False
        :type group_labeling: dict
        :return: list of created or loaded groups
        """
        # TODO validate dict structure against template dict
        self._to_insert = group_labeling[GroupsFromDict._insert_label]["INSERT"]
        self._insert_to_depth = group_labeling[GroupsFromDict._insert_label]["TO_DEPTH"]
        self._overwrite_extras = overwrite_extras

        depth = 0
        group_path_str = ""
        dict_of_groups = group_labeling
        groups = []
        self._create_or_load(depth, group_path_str, group_labeling, groups)
        return groups

    def _create_or_load(self, depth: int, group_path_str: str, dict_of_groups: dict, groups: list):
        """Recursively creates groups from possibly nested dict according to GroupFromDict.TEMPLATE.
        """
        base_path = group_path_str
        for group_label, attrs in dict_of_groups.items():
            if group_label in GroupsFromDict._ignore_labels:
                continue
            group_path_str = base_path + group_label
            group_path = aiida.tools.groups.GroupPath(group_path_str)
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
                self._create_or_load(depth + 1, group_path_str + "/", attrs["SUBGROUPS"], groups)
            groups.append(group)


def verdi_group_list(projection: typing.List[str] = ['label', 'id', 'type_string'],
                     with_header: bool = True, label_filter: str = None) -> list:
    """Equivalent to CLI "verdi group list -a" (minus user mail address).

    :param projection: query projection
    :param with_header: True: first list in return argument is the projection argument
    :param label_filter: optional: only include groups with this substring in their label
    :return: list of lists, one entry per projection value, for each group
    """
    qb = aiida.orm.QueryBuilder()
    group_list = qb.append(aiida.orm.Group, project=projection).all()

    if 'label' in projection and label_filter:
        index_of_label = projection.index('label')
        group_list = [item for item in group_list if label_filter in item[index_of_label]]

    group_list.sort(key=lambda item: item[0].lower())

    if with_header:
        group_list.insert(0, projection)

    if len(projection) == 1:
        group_list = [singlelist[0] for singlelist in group_list]

    return group_list


def get_subgroups(group: aiida.orm.Group) -> typing.List[aiida.orm.Group]:
    """Get all subgroups of a group.

    In accordance with aiida GroupPath, the group with label "foo/bar" is a valid subgroup
    of a group with label "foo".

    :param group: a group with possible subgroups
    :return: subgroups
    """
    from aiida.orm import Group
    group_labels = [group.label for group in Group.objects.all()]
    subgroup_labels = [label for label in group_labels if label.startswith(group.label)
                       and len(label) > len(group.label)]
    subgroups = [Group.get(label=label) for label in subgroup_labels]
    return subgroups


def move_nodes(origin: aiida.orm.Group, destination: aiida.orm.Group):
    """Move all nodes from one group to another, possibly sub/supergroup.

    :param origin: origin group
    :param destination: destination group
    """
    destination.add_nodes(list(origin.nodes))
    origin.remove_nodes(list(origin.nodes))


def get_nodes(group_label: str):
    """Get all nodes from given group (or subgroup) by label (path).

    Deprecated: just use group.nodes, or list(group.nodes).

    :param group_label: e.g. for a subgroup, "groupA/subgroupB/subgroupC".
    :return: nodes as generator for efficient iteration (convert via list() to list)
    """
    group = aiida.orm.Group.get(label=group_label)
    return group.nodes


def group_new_nodes(new_group_label: str, blacklist: typing.List[aiida.orm.Node] = [aiida.orm.Code, aiida.orm.Computer],
                    right_date: datetime = None, left_date: datetime = None):
    """Groups new nodes with ctime in timerange (left_date,right_date] into new group

    If you're working on one project at a time, everytime you finish a project you can use this function to
    group your nodes. I.e. this is a utility function for a time-linear sequential grouping strategy. Letting
    the function find the appropriate time range is the standard / recommended usage. If the group already exists
    and the intended nodes are already added, repeated calls will change nothing.

    :param new_group_label: label of new group/subgroup
    :param blacklist: nodes in timerange to exclude from grouping. Normally Code, Computer.
    :param right_date: if not given (usually as datetime.now()), will take right_date = newest ctime, > left_date, of any node, ungrouped nodes included.
    :param left_date: if not given, will take left_date=newest ctime of any grouped node
    :return: the new populated, stored, group, or None if no new nodes found
    :rtype: Group or None
    """

    timezone = pytz.UTC

    ## step1: find d7=infdate from all *grouped* nodes

    # new group to create or add
    new_path = aiida.tools.groups.GroupPath(path=new_group_label)
    new_group = new_path.get_or_create_group()[0]

    # get all groups, exclude new group if present
    group_labels = verdi_group_list(projection=['label'], with_header=False)
    try:
        group_labels.remove(new_group_label)
    except ValueError:
        pass
    groups = [aiida.orm.Group.get(label=label) for label in group_labels]

    # find tuple (group,node) with largest node.ctime across all groups
    left_date_computed = datetime(year=1, month=1, day=1, tzinfo=pytz.UTC)
    for group in groups:
        for node in group.nodes:
            left_date_computed = max(left_date_computed, node.ctime)

    if left_date is not None:
        left_date = timezone.localize(left_date)
        if left_date < left_date_computed:
            print(
                f"WARNING: left_date {left_date} < computed left date from groups {left_date_computed}, grouping overlap likely.")
        elif left_date > left_date_computed:
            print(
                f"WARNING: left_date {left_date} > computed left date from groups {left_date_computed}, leftover ungrouped nodes likely.")
    else:
        left_date = left_date_computed

        ## step2: find d8=maxdate from all nodes newer than d7

    qb = aiida.orm.QueryBuilder()
    new_nodes = qb.append(aiida.orm.Node, filters={'ctime': {'>': left_date}}).all(flat=True)
    if not new_nodes:
        print(f"Info: found no nodes newer than last grouped at date {left_date}. "
              f"Attempting to delete group '{new_group_label}' if empty.")
        delete_groups([new_group_label])
        return None
    else:
        right_date_computed = max([node.ctime for node in new_nodes])

        if right_date is not None:
            right_date = timezone.localize(right_date)
            if right_date < right_date_computed:
                print(
                    f"WARNING: right_date {right_date} < computed right date from groups {right_date_computed}, leftover ungrouped nodes likely.")
        else:
            right_date = right_date_computed

        ## step3: query all nodes in daterange (d7,d8], optional: exclude blacklist

        daterange_filter = {
            'and': [
                {'ctime': {'>': left_date}},  # newer than
                {'ctime': {'<=': right_date}}  # older than
            ]
        }
        qb = aiida.orm.QueryBuilder()
        new_nodes = qb.append(aiida.orm.Node, filters=daterange_filter).distinct().all(flat=True)
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


def delete_groups(group_labels: typing.List[str]):
    """Delete a group. Aborts if group is non-empty. Use delete_groups_with_nodes() for that.

    :param group_labels: list of group labels
    """

    for label in group_labels:
        try:
            group = aiida.orm.Group.get(label=label)
        except aiida.common.exceptions.NotExistent:
            print(f"Warning: group to delete '{label}' does not exist.")
        else:
            if group.count() > 0:
                print(f"Info: Skipping non-empty group<{label}>: contains {group.count()} nodes.")
            else:
                group.clear()
                aiida.orm.Group.objects.delete(group.pk)
                print(f"Group '{label}' deleted.")


def delete_groups_with_nodes(group_labels: typing.List[str], dry_run: bool = True,
                             verbosity: int = 1, leave_groups: bool = False):
    """Delete all nodes in each group (including repo files), then delete the groups themselves.

    :param group_labels: list of group labels
    :param dry_run: perform test run. if output looks good, set to false and repeat.
    :param verbosity: 0, 1 or 2, 2=max=default.
    :param leave_groups: True: Leave empty groups as is after deleting all nodes in them.
    """
    # get the node delete function used by 'verdi node delete -v -n/-f'
    from aiida.manage.database.delete.nodes import delete_nodes

    print("Deleting nodes in groups...")

    # get the groups of all stated group labels
    groups = [aiida.orm.Group.get(label=label) for label in group_labels]

    # get the pks of all nodes in all groups
    pks = []
    for group in groups:
        pks += [node.pk for node in group.nodes]

    # delete nodes
    delete_nodes(pks=pks, dry_run=dry_run, force=True, verbosity=verbosity)

    # now delete groups
    print(f"Deleting groups: {not leave_groups}...")
    if leave_groups:
        if dry_run:
            print("Dry run: groups unchanged.")
        else:
            are_empty = {group.label: group.is_empty for group in groups}
            print(f"Groups are now empty: {are_empty}")
    else:
        if dry_run:
            print("Dry run: Skipping deleting groups.")
        else:
            delete_groups(group_labels=group_labels)


def get_nodes_by_group(group_label: str = None, node_type: aiida.orm.Node = aiida.orm.Node, return_query: bool = False,
                       return_iter: bool = True, node_tag: str = ""):
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
    qb = aiida.orm.QueryBuilder()

    qb.append(aiida.orm.Group, filters={'label': group_label}, tag='group')
    qb.append(node_type, with_group='group', tag=node_tag, project='*')

    # DEVNOTE: equivalent:
    # qb.append(node_type, tag="nodes", project="*")
    # qb.append(aiida.orm.Group, with_node="nodes", filters={"label": group_label})

    if return_query:
        return qb
    if return_iter:
        return qb.iterall()
    else:
        return qb.all(flat=True)
