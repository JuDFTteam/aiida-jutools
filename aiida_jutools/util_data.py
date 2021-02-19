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
"""Tools for working with aiida Data nodes."""

from pathlib import Path

from aiida.orm import CifData
from aiida.orm import Dict, Group
from aiida.orm import StructureData, QueryBuilder
from aiida.tools.groups import GroupPath


# @calcfunction
# def cif2structure(cif:CifData, converter='pymatgen', store:bool=True, conversion_settings:Dict=Dict({})) -> StructureData:
#     return cif.get_structure(converter, store, conversion_settings.get_dict())


def query_elemental_structure(symbol: str, group=None) -> list:
    """Query structures for a single chemical element.

    :param symbol: chemical element symbo case-senstive, like 'He'
    :param group: optionally, search only within this group
    :type group:
    :return: list of results
    """
    qb = QueryBuilder()
    if group:
        qb.append(Group, filters={'label': group.label}, tag='group')
        qb.append(StructureData, with_group='group', filters={'attributes.kinds.0.name': symbol})
    else:
        qb.append(StructureData, filters={'attributes.kinds.0.name': symbol})  # more general
        # # alternative: require extras
        # qb.append(StructureData, with_group='group', filters={"extras.symbol": symbol})

    return qb.first()[0]
    # # DEVNOTE:
    # # alternative: require extras
    # the following eqvt. solution is ~3x times slower (for a group of ~1e2 structures):
    # return next((structure for structure in structures if structure.extras['symbol']  == symbol), None)


class CifImporter:
    def __init__(self):
        """

        Data management tips:
        1) When reading cif files, first set helpful label, description on created cifdata,
        then call store(). 2) Set extras. E.g. if elemental, set 'atomic_number' and 'symbol'.
        Then extras can be used for sorting, e.g. sorted(cifs, key=lambda cif: cif.get_extra("atomic_number")).
        3) Store imported cif files in a group. 4) Do the same for any converted data. Store the conversion 
        settings also in the converted group. Aiida stores the provenance between CifData nodes and 
        output format nodes, so you don't have to take care of that manually.

        """
        self.cif_group = None
        self.struc_group = None
        self.conversion_settings = {
            'aiida_structure': Dict(dict={'converter': 'pymatgen', 'store': True, 'primitive_cell': True})
        }

    def from_file(self, cif_file: Path):
        """Read CIF file.

        :param cif_file:
        :return: unstored cif node
        :rtype: CifData
        """
        cif = CifData()
        filename = cif_file.name
        cif.set_file(file=str(cif_file), filename=filename)
        return cif

    def import_cif_files(self, cif_files_path: Path):
        """

        :param cif_files_path:
        :type cif_files_path:
        :return:
        :rtype:
        """
        cifs = []
        cifs_names = []
        cifs_numbers = []
        for cif_file in cif_files_path.iterdir():
            cif = self.from_file(cif_file)

        return list(cifs)

    def convert(self, cifgroup_label: str, structure_group_label: str, as_subgroup: bool = True,
                structure_group_description: str = None, assume_empty_group: bool = True,
                conversion_settings: Dict = None, store_new_nodes: bool = False):
        """Load or create group in other format from group of CifData.

        Note: conversion_settings node will be selected by this priority: 1) existing conversion settings node in
        new structure group, 2) conversion settings node supplied as argument, 3) default conversion settings (if None
        for 1) and 2)).

        :param cifgroup_label: group label of CifData group.
        :param structure_group_label: label of converted group.
        :param as_subgroup: load or create converted group with label 'cif_group_label/structure_group_label'
        :param structure_group_description: description for converted group. ignored if already exist and stored.
        :param assume_empty_group: if converted group not empty: True: do no conversion. False: add new nodes.
        :param conversion_settings: settings arguments supplied to the respective CifData.to_OtherFormat() method.
        :type conversion_settings: Dict
        :param store_new_nodes: default False: don't store yet to allow adding label, description.
        :return: converted group
        :rtype: Group
        """
        # load or create structures group
        self.cif_group = Group.get(label=cifgroup_label)
        if as_subgroup:
            self.struc_grouppath = GroupPath(cifgroup_label + "/" + structure_group_label)
        else:
            self.struc_grouppath = structure_group_label
        self.struc_group, created = self.struc_grouppath.get_or_create_group()

        if created and not self.struc_group.is_stored:
            if structure_group_description:
                self.struc_group.description = structure_group_description
            self.struc_group.store()

        # load or add cif2structure conversion settings
        # this assumes there is a unique Dict node inside the structures group, if already exist
        hits = [node for node in self.struc_group.nodes if type(node) == Dict]
        if len(hits) > 1:
            raise ValueError(
                f"More than one conversion settings node in group '{self.struc_group.label}', delete one first!")
            # self.struc_group.remove_nodes(hits[1:])
        elif len(hits) == 1:
            if conversion_settings:
                print(
                    f"Info: Found unique conversion settings node in group '{self.struc_group.label}', "
                    f"will ignore supplied settings.")
                self.conversion_settings['aiida_structure'] = hits[0]
        else:
            if conversion_settings:
                self.conversion_settings['aiida_structure'] = conversion_settings
            else:
                print("Info: No conversion settings supplied or found in group, will use default settings.")

        conv_set = self.conversion_settings['aiida_structure']
        if not conv_set.is_stored:
            conv_set.store()
        self.struc_group.add_nodes([conv_set])

        # load or create structures
        def create():
            structures = [cif.get_structure(**conv_set) for cif in self.cif_group.nodes]
            self.struc_group.add_nodes(structures)
            if store_new_nodes:
                for structure in structures:
                    structure.store()
            print(f"Created {len(structures)} structure nodes, added to group '{self.struc_group.label}', "
                  f"nodes stored: {store_new_nodes}.")

        if assume_empty_group:
            if not any([type(node) == StructureData for node in self.struc_group.nodes]):
                create()
            else:
                structures = [node for node in self.struc_group.nodes if type(node) == StructureData]
                print(f"Loaded {len(structures)} structure nodes from group '{self.struc_group.label}'.")
        else:
            create()

        # check results
        warning_msg = f"WARNING: Cif group '{self.cif_group.label}' has {len(list(self.cif_group.nodes))} CifData " \
                      f"nodes, but structure group '{self.struc_group.label}' has {len(structures)} StructureData " \
                      f"nodes."
        if len(list(self.struc_group.nodes)) != len(structures):
            print(warning_msg)
        if assume_empty_group:
            if len(list(self.struc_group.nodes)) != (len(structures) + 1):  # conv setting node
                print(warning_msg + f" Since assumed empty group, structure group should have "
                                    f"{len(list(self.cif_group.nodes))} + 1 nodes (plus one for conversion settings "
                                    f"Dict).")

        return self.struc_group
