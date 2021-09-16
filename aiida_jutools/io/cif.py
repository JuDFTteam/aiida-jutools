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
"""Tools for working with AiiDA IO: CIF files."""

import typing as _typing
from pathlib import Path as _Path
from pprint import pprint

import aiida as _aiida
from aiida import orm as _orm
from aiida.tools import groups as _aiida_groups

from aiida_jutools._dev.terminal_colors import *
from aiida_jutools.structure import analyze_symmetry

class CifImporter:
    DEFAULT_CONVERSION_SETTINGS = {
        'converter': 'pymatgen',
        'store': True,
        'primitive_cell': True
    }

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
        self._clear()

    def _clear(self):
        """Reinitialize attributes to default.
        """
        self.cif_group = None
        self.struc_group = None
        self.conversion_settings = None

    @staticmethod
    def from_file(cif_file: _Path,
                  silent: bool = False) -> _typing.Optional[_orm.CifData]:
        """Read CIF file.

        Note: no check on CIF file content validity is performed.

        :param cif_file: path to CIF file.
        :param silent: Do not print info/warnings.
        :return: unstored CifData node if file ends with ".cif", case-insensitive, else None.
        """
        if not cif_file.exists():
            raise FileNotFoundError(f"File {cif_file} does not exist.")

        filename = cif_file.name

        if not filename.lower().endswith(".cif"):
            if not silent:
                print(f"Info: File {cif_file.name} does not have .cif extension. I ignore it and return None.")
            return None

        else:
            cif = _orm.CifData()
            cif.set_file(file=str(cif_file), filename=filename)
            return cif

    def import_cif_files(self,
                         cif_files_path: _Path,
                         cif_group_label: str = None) -> _typing.Union[_typing.List[_orm.CifData], _orm.Group]:
        """Read all CIF files in a folder (files with extension ".cif", case-insensitive).

        Note: no check on CIF file content validity is performed.

        :param cif_files_path: folder path with CIF files
        :param cif_group_label: if supplied, add CifData nodes to this group. Create if not exist.
        :return: list of unstored CifData nodes, or group with stored CifData nodes, if supplied.
        """
        cifs = []
        for cif_file in cif_files_path.iterdir():
            cif = self.from_file(cif_file=cif_file, silent=True)
            if cif:
                cifs.append(cif)

        if not cif_group_label:
            return cifs

        for cif in cifs:
            cif.store()
        cif_group_path = _aiida_groups.GroupPath(cif_group_label)
        cif_group, created = cif_group_path.get_or_create_group()
        cif_group.store()
        cif_group.add_nodes(cifs)
        self.cif_group = cif_group
        return cif_group

    def load_or_convert(self, cifgroup_label: str,
                        structure_group_label: str,
                        as_subgroup: bool = True,
                        structure_group_description: str = None,
                        load_over_create: bool = True,
                        conversion_settings: _orm.Dict = None,
                        dry_run: bool = True,
                        silent: bool = False) -> _orm.Group:
        """Load or create group of StructureData nodes from group of CifData nodes.

        :param cifgroup_label: group label of CifData group.
        :param structure_group_label: label of converted group.
        :param as_subgroup: load or create converted group with label 'cif_group_label/structure_group_label'
        :param structure_group_description: description for converted group. ignored if already exist and stored.
        :param load_over_create: if converted group exists and has structure nodes:
               load nodes instead of create (convert).
        :param conversion_settings: settings arguments supplied to the respective CifData.to_OtherFormat() method.
        :param dry_run: default True: perform a dry run and print what the method *would* do.
        :param silent: default True: if not dry_run do not print info messages.
        :return: converted group

        Note: load_over_create=True does not check an existing StructureData nodes set whether it matches the
        supplied CifData nodes set. Instead, it just aborts conversion and loads the StructureData nodes if any
        already exist.

        Note: Conversion settings are those accepted by CifData.get_structure(). Conversion_settings node will loaded
        or created by this priority: 1) new structure group exists and has a conversion settings node, 2) else
        conversion settings node supplied as argument, 3) create from default conversion settings.

        Note: newly created converted data nodes label and description may be changed, even after having been
        already stored.
        """
        # DEVNOTE:
        # group.add_nodes([node]) works only if both group and node are stored. keep in mind when changing
        # the code logic order.

        # first clear the cache
        self._clear()

        # load or create structures group
        would_or_will = "would" if dry_run else "will"

        def _get_cif_nodes() -> _typing.List[_orm.CifData]:
            self.cif_group = _orm.Group.get(label=cifgroup_label)
            cifnodes = [node for node in self.cif_group.nodes if isinstance(node, _orm.CifData)]

            msg = 40 * '-' + '\n'
            msg += f"Task: Convert {len(cifnodes)} {_orm.CifData.__name__} " \
                   f"nodes in group '{self.cif_group.label}' to {_orm.StructureData.__name__} nodes." \
                   f"\nPerform dry run: {dry_run}.\n"
            if not cifnodes:
                msg += "Nothing to convert. Done."

            if dry_run or not silent:
                print(msg)

            return cifnodes

        def _determine_structure_group_path(structure_group_label: str) -> _aiida_groups.GroupPath:
            # irrespective of whether dry_run or not
            if not as_subgroup:
                struc_grouppath = _aiida_groups.GroupPath(structure_group_label)
            else:
                # trim off path separators if present
                structure_group_label = structure_group_label if not structure_group_label.endswith(
                    "/") else structure_group_label[:-1]
                struc_grouppath = _aiida_groups.GroupPath(cifgroup_label + "/" + structure_group_label)
            return struc_grouppath

        def _load_or_create_structure_group(struc_grouppath: _aiida_groups.GroupPath) -> None:
            """If dry_run, don't create if not exist, return None. Else return group."""
            struc_group = None

            msg = ""
            # find out if structure group exists or not
            exists = True
            try:
                struc_group = _orm.Group.get(label=struc_grouppath.path)
            except _aiida.common.exceptions.NotExistent as err:
                exists = False
            load_or_create = "load existing" if exists else "create new"
            msg += f"I {would_or_will} {load_or_create} {_orm.StructureData.__name__} group '{struc_grouppath.path}'."

            if dry_run or not silent:
                print(msg)
            if not dry_run:
                struc_group, created = struc_grouppath.get_or_create_group()
                # only add description and store if not existed already
                if created and not struc_group.is_stored:
                    if structure_group_description:
                        struc_group.description = structure_group_description
                    struc_group.store()

            self.struc_group = struc_group

        def _load_or_create_conversion_settings() -> None:
            def _try_load_conversion_settings(struc_group: _orm.Group):
                if not struc_group:
                    return None
                else:
                    hits = [node for node in struc_group.nodes if type(node) == _orm.Dict]
                    if len(hits) > 1:
                        raise ValueError(
                            f"I found more than one conversion settings node in group '{struc_group.label}'. "
                            f"Ambiguous conversion settings. Please delete the spurious conversion settings nodes "
                            f"first. Found conversion nodes:\n{hits}")
                    return hits[0] if hits else None

            # three cases: A) load, or B) use from argument, or C) create
            # if structure group doesn't exist already, only B), C) left
            convset_node = _try_load_conversion_settings(self.struc_group)
            msg = ""
            if self.struc_group and convset_node:
                msg += f"I found conversion settings in the structure group '{self.struc_group.label}' and " \
                       f"{would_or_will} use them:"
            else:
                msg += "I found no conversion settings in the database."
                if conversion_settings:
                    convset_node = conversion_settings
                    msg += f" Conversion settings were passed as argument:"
                else:
                    convset_node = _orm.Dict(dict=self.DEFAULT_CONVERSION_SETTINGS)
                    msg += f" Conversion settings were not passed as argument. I {would_or_will} use the " \
                           f"default conversion settings:"
            msg += f"\nConversion settings node pk: {convset_node.pk}, settings:\n{convset_node.attributes}"

            if dry_run or not silent:
                print(msg)
            if not dry_run:
                # conv_set node will be stored and added to group in next step,
                # depending on whether any conversion is required.
                pass

            self.conversion_settings = convset_node

        def _check_conversion_settings() -> None:
            msg = ""

            setting_key = 'store'
            setting_should = True
            setting_is = self.conversion_settings.attributes.get(setting_key, None)
            reason_for_requirement = f"\nReason for requirement: If new {_orm.StructureData.__name__} nodes get " \
                                     f"created instead of loaded, adding them to the structure group " \
                                     f"{would_or_will} fail."
            if setting_is is not None and not (setting_is == setting_should):
                msg += f"Selected conversion settings node setting '{setting_key}' is required to have " \
                       f"value {setting_should}, but has value {setting_is}."
                if self.conversion_settings.is_stored:
                    msg += f" WARNING: I cannot change the setting since the settings node is stored."
                else:
                    msg += f" INFO: The settings node has not been stored yet. I {would_or_will} changed the " \
                           f"setting to {setting_should}."
                    if not dry_run:
                        self.conversion_settings[setting_key] = setting_should
                msg += reason_for_requirement
                # print even if silent
                print(msg)

        def _load_or_convert(cif_nodes: _typing.List[_orm.CifData],
                             struc_grouppath: _aiida_groups.GroupPath) -> _typing.List[_orm.StructureData]:

            msg = ""
            structure_nodes = None

            if self.struc_group:
                structure_nodes = [node for node in self.struc_group.nodes if isinstance(node, _orm.StructureData)]

            if load_over_create and structure_nodes:
                msg += f"Found {len(structure_nodes)} pre-existing {_orm.StructureData.__name__} nodes in " \
                       f"{_orm.StructureData.__name__} group '{self.struc_group.label}'.\nI will not perform any " \
                       f"conversion."

                if dry_run or not silent:
                    print(msg)
                    return structure_nodes

            else:
                # DEVNOTE: for structure group node label, we use the grouppath here because dry_run distinction
                # leaves possibility that structure group doesn't exit yet.
                msg += f"I {would_or_will} perform the {_orm.CifData.__name__} -> {_orm.StructureData.__name__} " \
                       f"conversion now and add the converted nodes to the group '{struc_grouppath.path}', " \
                       f"along with the conversion settings node."
                if dry_run or not silent:
                    print(msg)

                if not dry_run:

                    if not self.conversion_settings.is_stored:
                        self.conversion_settings.store()
                        self.struc_group.add_nodes([self.conversion_settings])

                    structure_nodes = [cif.get_structure(**self.conversion_settings) for cif in cif_nodes]
                    self.struc_group.add_nodes(structure_nodes)
                    if not dry_run:
                        # print this even if silent
                        print(f"Created {len(structure_nodes)} structure nodes, added to group "
                              f"'{self.struc_group.label}' along with conversion settings node.")

            return structure_nodes

        def _post_conversion_check(cif_nodes: _typing.List[_orm.CifData],
                                   struc_grouppath: _aiida_groups.GroupPath,
                                   structure_nodes: _typing.List[_orm.StructureData]) -> None:
            if structure_nodes and (not len(cif_nodes) == len(structure_nodes)):
                # print this even if silent
                print(f"Warning: The {_orm.CifData.__name__} group '{self.cif_group.label}' has {len(cif_nodes)} "
                      f"{_orm.CifData.__name__} nodes, but the {_orm.StructureData.__name__} group "
                      f"'{struc_grouppath.path}' has {len(structure_nodes)} {_orm.StructureData.__name__} nodes. "
                      f"It is recommended to keep a one-to-one node-to-node conversion relation between such "
                      f"conversion groups to corectly reflect provenance.")

        cif_nodes = _get_cif_nodes()
        if cif_nodes:
            struc_grouppath = _determine_structure_group_path(structure_group_label=structure_group_label)
            _load_or_create_structure_group(struc_grouppath=struc_grouppath)
            _load_or_create_conversion_settings()
            _check_conversion_settings()
            struc_nodes = _load_or_convert(cif_nodes=cif_nodes, struc_grouppath=struc_grouppath)
            _post_conversion_check(cif_nodes=cif_nodes, struc_grouppath=struc_grouppath, structure_nodes=struc_nodes)

        return self.struc_group


def cif2astr(cifpath):
    """Runs :py:meth:`~.aiida_jutools.structure.analyze_symmetry` on a CIF file and prints color-coded summary."""
    prompt = ""

    dd = {
        'fmt': 'cif',
        'cifpath': cifpath,
        'outmode': ['a_conv']
    }

    structure = analyze_symmetry(dd)

    if structure.get('aiida_structure_conventional'):
        print(prompt + structure['aiida_structure_conventional'].extras['check_cif']['message'] + '\n')
        print(prompt + 'label:           ' + CC1 + structure['aiida_structure_conventional'].label + CEND)
        print(prompt + 'description:     ' + structure['aiida_structure_conventional'].description)
        print(prompt + 'prototype:       ' + CC2 + structure['aiida_structure_conventional'].extras['prototype'][
            'nprot'] + CEND +
              ' : ' + CC2 + structure['aiida_structure_conventional'].extras['prototype']['nrw'] + CEND)
        print(
            prompt + 'specification:   ' + structure['aiida_structure_conventional'].extras['system']['specification'])

        print('\n' + 'extras:' + '\n')
        pprint(structure['aiida_structure_conventional'].extras, width=256)