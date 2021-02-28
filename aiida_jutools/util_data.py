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
    from aiida.orm import QueryBuilder, Group, StructureData

    qb = QueryBuilder()
    if group:
        qb.append(Group, filters={'label': group.label}, tag='group')
        qb.append(StructureData, with_group='group', filters={'attributes.kinds.0.name': symbol})
    else:
        qb.append(StructureData, filters={'attributes.kinds.0.name': symbol})  # more general
        # # alternative: require extras
        # qb.append(StructureData, with_group='group', filters={"extras.symbol": symbol})

    # return qb.first()[0]
    return qb.all(flat=True)
    # # DEVNOTE:
    # # alternative: require extras
    # the following eqvt. solution is ~3x times slower (for a group of ~1e2 structures):
    # return next((structure for structure in structures if structure.extras['symbol']  == symbol), None)


class CifImporter:
    DEFAULT_CONVERSION_SETTINGS = {'converter': 'pymatgen', 'store': True, 'primitive_cell': True}

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

    def from_file(self, cif_file: Path, silent: bool = False):
        """Read CIF file.

        Note: no check on CIF file content validity is performed.

        :param cif_file: path to CIF file.
        :param silent: Do not print info/warnings.
        :return: unstored CifData node if file ends with ".cif", case-insensitive, else None.
        :rtype: CifData or None
        """
        from aiida.orm import CifData

        if not cif_file.exists():
            raise FileNotFoundError(f"File {cif_file} does not exist.")

        filename = cif_file.name

        if not filename.lower().endswith(".cif"):
            if not silent:
                print(f"Info: File {cif_file.name} does not have .cif extension. I ignore it and return None.")
            return None

        else:
            cif = CifData()
            cif.set_file(file=str(cif_file), filename=filename)
            return cif

    def import_cif_files(self, cif_files_path: Path, cif_group_label: str = None):
        """Read all CIF files in a folder (files with extension ".cif", case-insensitive).

        Note: no check on CIF file content validity is performed.

        :param cif_files_path: folder path with CIF files
        :param cif_group_label: if supplied, add CifData nodes to this group. Create if not exist.
        :return: list of unstored CifData nodes, or group with stored CifData nodes, if supplied.
        """
        from aiida.tools.groups import GroupPath

        cifs = []
        for cif_file in cif_files_path.iterdir():
            cif = self.from_file(cif_file=cif_file, silent=True)
            if cif:
                cifs.append(cif)

        if not cif_group_label:
            return cifs

        else:
            for cif in cifs:
                cif.store()
            cif_group_path = GroupPath(cif_group_label)
            cif_group, created = cif_group_path.get_or_create_group()
            cif_group.store()
            cif_group.add_nodes(cifs)
            self.cif_group = cif_group
            return cif_group

    def load_or_convert(self, cifgroup_label: str, structure_group_label: str, as_subgroup: bool = True,
                        structure_group_description: str = None, load_over_create: bool = True,
                        conversion_settings=None, dry_run: bool = True, silent: bool = False):
        """Load or create group of StructureData nodes from group of CifData nodes.

        :param cifgroup_label: group label of CifData group.
        :param structure_group_label: label of converted group.
        :param as_subgroup: load or create converted group with label 'cif_group_label/structure_group_label'
        :param structure_group_description: description for converted group. ignored if already exist and stored.
        :param load_over_create: if converted group exists and has structure nodes: load nodes instead of create (convert).
        :param conversion_settings: settings arguments supplied to the respective CifData.to_OtherFormat() method.
        :type conversion_settings: Dict
        :param dry_run: default True: perform a dry run and print what the method *would* do.
        :param silent: default True: if not dry_run do not print info messages.
        :return: converted group
        :rtype: Group

        Note: load_over_create=True does not check an existing StructureData nodes set whether it matches the
        supplied CifData nodes set. Instead, it just aborts conversion and loads the StructureData nodes if any already exist.

        Note: Conversion settings are those accepted by CifData.get_structure(). Conversion_settings node will loaded or created by
        this priority: 1) new structure group exists and has a conversion settings node, 2) else conversion settings node supplied
        as argument, 3) create from default conversion settings.

        Note: newly created converted data nodes label and description may be changed, even after having been already stored.
        """
        from aiida.orm import Group, CifData, StructureData
        from aiida.tools.groups import GroupPath

        # DEVNOTE:
        # group.add_nodes([node]) works only if both group and node are stored. keep in mind when changing
        # the code logic order.

        # first clear the cache
        self._clear()

        # load or create structures group
        would_or_will = "would" if dry_run else "will"

        def _get_cif_nodes() -> list:
            self.cif_group = Group.get(label=cifgroup_label)
            cifnodes = [node for node in self.cif_group.nodes if isinstance(node, CifData)]

            msg = 40 * '-' + '\n'
            msg += f"Task: Convert {len(cifnodes)} {CifData.__name__} " \
                   f"nodes in group '{self.cif_group.label}' to {StructureData.__name__} nodes.\nPerform dry run: {dry_run}.\n"
            if not cifnodes:
                msg += "Nothing to convert. Done."

            if dry_run or not silent:
                print(msg)

            return cifnodes

        def _determine_structure_group_path(structure_group_label) -> GroupPath:
            # irrespective of whether dry_run or not
            if not as_subgroup:
                struc_grouppath = GroupPath(structure_group_label)
            else:
                # trim off path separators if present
                structure_group_label = structure_group_label if not structure_group_label.endswith(
                    "/") else structure_group_label[:-1]
                struc_grouppath = GroupPath(cifgroup_label + "/" + structure_group_label)
            return struc_grouppath

        def _load_or_create_structure_group(struc_grouppath) -> None:
            """If dry_run, don't create if not exist, return None. Else return group."""
            struc_group = None

            msg = ""
            from aiida.common.exceptions import NotExistent
            # find out if structure group exists or not
            exists = True
            try:
                struc_group = Group.get(label=struc_grouppath.path)
            except NotExistent as err:
                exists = False
            load_or_create = "load existing" if exists else "create new"
            msg += f"I {would_or_will} {load_or_create} {StructureData.__name__} group '{struc_grouppath.path}'."

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
            from aiida.orm import Dict

            def _try_load_conversion_settings(struc_group):
                if not struc_group:
                    return None
                else:
                    hits = [node for node in struc_group.nodes if type(node) == Dict]
                    if len(hits) > 1:
                        raise ValueError(
                            f"I found more than one conversion settings node in group '{struc_group.label}'. "
                            f"Ambiguous conversion settings. Please delete the spurious conversion settings nodes first. "
                            f"Found conversion nodes:\n{hits}")
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
                    convset_node = Dict(dict=self.DEFAULT_CONVERSION_SETTINGS)
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
            reason_for_requirement = f"\nReason for requirement: If new {StructureData.__name__} nodes get " \
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

        def _load_or_convert(cif_nodes, struc_grouppath) -> list:

            msg = ""
            structure_nodes = None

            if self.struc_group:
                structure_nodes = [node for node in self.struc_group.nodes if isinstance(node, StructureData)]

            if load_over_create and structure_nodes:
                msg += f"Found {len(structure_nodes)} pre-existing {StructureData.__name__} nodes in " \
                       f"{StructureData.__name__} group '{self.struc_group.label}'.\nI will not perform any conversion."

                if dry_run or not silent:
                    print(msg)
                    return structure_nodes

            else:
                # DEVNOTE: for structure group node label, we use the grouppath here because dry_run distinction
                # leaves possibility that structure group doesn't exit yet.
                msg += f"I {would_or_will} perform the {CifData.__name__} -> {StructureData.__name__} conversion " \
                       f"now and add the converted nodes to the group '{struc_grouppath.path}', along with the " \
                       f"conversion settings node."
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

        def _post_conversion_check(cif_nodes, struc_grouppath, structure_nodes) -> None:
            if structure_nodes and (not len(cif_nodes) == len(structure_nodes)):
                # print this even if silent
                print(f"Warning: The {CifData.__name__} group '{self.cif_group.label}' has {len(cif_nodes)} "
                      f"{CifData.__name__} nodes, but the {StructureData.__name__} group '{struc_grouppath.path}' has "
                      f"{len(structure_nodes)} {StructureData.__name__} nodes. It is recommended to keep a one-to-one "
                      f"node-to-node conversion relation between such conversion groups to corectly reflect provenance.")

        cif_nodes = _get_cif_nodes()
        if cif_nodes:
            struc_grouppath = _determine_structure_group_path(structure_group_label=structure_group_label)
            _load_or_create_structure_group(struc_grouppath=struc_grouppath)
            _load_or_create_conversion_settings()
            _check_conversion_settings()
            struc_nodes = _load_or_convert(cif_nodes=cif_nodes, struc_grouppath=struc_grouppath)
            _post_conversion_check(cif_nodes=cif_nodes, struc_grouppath=struc_grouppath, structure_nodes=struc_nodes)

        return self.struc_group


def load_or_rescale_structures(input_structure_group, output_structure_group_label: str, scale_factor,
                               set_extra: bool = True, dry_run: bool = True, silent: bool = False):
    """Rescale a group of structures and put them in a new or existing group.

    Only input structures which do not already have a rescaled output structure in the output structure group
    will be rescaled.

    :param input_structure_group: group with StructureData nodes to rescale. Ignores other nodes in the group.
    :type input_structure_group: Group
    :param output_structure_group_label: name of group for rescaled structures. Create if not exist.
    :param scale_factor: scale factor with which to scale the lattice constant of the input structure
    :type scale_factor: Float
    :param set_extra: True: set extra 'scale_factor' : scale_factor.value to structures rescaled in this run.
    :param dry_run: default True: perform a dry run and print what the method *would* do.
    :param silent: True: do not print info messages
    :return: output group of rescaled structures
    :rtype: Group
    """
    from aiida.tools.groups import GroupPath
    from aiida.orm import Group, StructureData, Float
    from aiida_jutools.process_functions.rescale_structure import rescale_structure

    assert isinstance(scale_factor, Float)

    would_or_will = "would" if dry_run else "will"

    out_structure_grouppath = GroupPath(path=output_structure_group_label)
    out_structure_group, created = out_structure_grouppath.get_or_create_group()

    inp_structures = {node.uuid: node for node in input_structure_group.nodes if isinstance(node, StructureData)}
    already_rescaled = {}

    if dry_run or not silent:
        print(40 * '-')
        print(f"Task: Rescale {len(inp_structures.keys())} {StructureData.__name__} nodes in group "
              f"'{input_structure_group.label}' with scale factor {scale_factor.value}.\nPerform dry run: {dry_run}.")

    # try load structures
    out_structures_existing = [node for node in out_structure_group.nodes if isinstance(node, StructureData)]
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
            f"I {would_or_will} rescale {len(inp_structures.keys())} {StructureData.__name__} nodes from the input group. "
            f" I would add the new nodes to output group '{output_structure_group_label}'.\n"
            f"{len(already_rescaled.keys())} {StructureData.__name__} of the input nodes already have been rescaled and added "
            f"to this output target previously.")
    if not dry_run:
        for inp_structure in inp_structures.values():
            out_structure = rescale_structure(input_structure=inp_structure, scale_factor=scale_factor)
            if set_extra:
                out_structure.set_extra("scale_factor", scale_factor.value)
            out_structure_group.add_nodes([out_structure])
    if not dry_run and not silent:
        print(
            f"Created {len(inp_structures.keys())} {StructureData.__name__} nodes and added them to group "
            f"'{output_structure_group_label}'.")
    return out_structure_group


def query_modified_input_structure(modified_structure, invariant_kinds: bool = False) -> list:
    """Given a structure modified via a CalcFunction, query its input structure(s).

    :param modified_structure: structure modified via a single CalcFunction
    :type modified_structure: StructureData
    :param invariant_kinds: to make query more precise., assume that the 'kinds' attribute has not been modified.
    :return: list of input structures, if any.
    """
    from aiida.orm import QueryBuilder, CalcFunctionNode, StructureData

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

    qb = QueryBuilder()
    # qb.append(Group, filters={'label': output_structure_group.label}, tag='group')
    # qb.append(StructureData, with_group='group', filters={"uuid" : modified_structure.uuid}, tag='out_struc')
    qb.append(StructureData, filters={"uuid": modified_structure.uuid}, tag='out_struc')
    qb.append(CalcFunctionNode, with_outgoing='out_struc', tag='calc_fun')
    qb.append(StructureData, with_outgoing='calc_fun', filters=input_structure_filters)
    return qb.all(flat=True)
