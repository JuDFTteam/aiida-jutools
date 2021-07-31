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
"""Tools for working with aiida-kkr nodes."""

import enum as _enum
import os as _os

import numpy as _np
import pandas as _pd
from aiida.orm import CalcJobNode as _CalcJobNode, WorkChainNode as _WorkChainNode
from aiida.orm import Dict as _Dict
from aiida.orm import QueryBuilder as _QueryBuilder, Group as _Group, RemoteData as _RemoteData, \
    StructureData as _StructureData
from aiida_kkr.calculations import VoronoiCalculation as _VoronoiCalculation
from aiida_kkr.workflows import kkr_imp_wc as _kkr_imp_wc, kkr_scf_wc as _kkr_scf_wc, \
    kkr_startpot_wc as _kkr_startpot_wc
from masci_tools.util import math_util as _math_util
from masci_tools.util.chemical_elements import ChemicalElements as _ChemicalElements
from masci_tools.util.constants import ANG_BOHR_KKR as _ANG_BOHR_KKR


def check_if_kkr_calc_converged(kkr_calc: _CalcJobNode):
    """Assert (fail if false) that kkr calculation has converged.

    DEVNOTE: used aiida base node type for argument type so it works with all kkr calc node types.

    E.g. needed for host GF writeout

    Reference: https://aiida-kkr.readthedocs.io/en/stable/user_guide/calculations.html#special-run-modes-host-gf-writeout-for-kkrimp

    :param kkr_calc: performed kkr calculation
    """
    try:
        assert kkr_calc.outputs.output_parameters.get_dict()["convergence_group"]["calculation_converged"] == True
    except KeyError as err:
        print("Error: calculation is not a kkr calculation.")
        raise err


def query_kkr_wc(cls=_kkr_imp_wc, symbols: list = ['H', 'H'], group=None) -> _QueryBuilder:
    """Query kkr workchains based on their input structures.

    Constraints:

    - if kkr_scf_wc, and symbols given, queries with first symbol only (elemental crystal).
    - if kkr_imp_wc, requires symbols, queries with first symbol = impurity, second symbol = host crystal.

    For general workchain queries, use :py:func:`~aiida_jutools.util_process.query_processes` instead.

    :param cls: kkr workchain class
    :type cls: kkr_scf_wc or kkr_imp_wc
    :param group: given: search in group, not: search in database
    :type group: aiido.orm.Group
    :param symbols: list of chemical element symbols.
    :type symbols: list of str
    :return: the built query for matching workchains
    """
    if isinstance(symbols, str):
        symbols = [symbols]

    qb = _QueryBuilder()
    if group:
        qb.append(_Group, filters={'label': group.label}, tag='group')
    if issubclass(cls, _kkr_scf_wc):
        if group:
            qb.append(_kkr_scf_wc, with_group='group', tag='workchain', project='*')
        else:
            qb.append(_kkr_scf_wc, tag='workchain', project='*')
        if symbols:
            qb.append(_StructureData, with_outgoing='workchain',
                      filters={'attributes.kinds.0.name': symbols[0]})
            # # alternative: require extras
            # qb.append(_StructureData, with_outgoing='workchain', filters={"extras.symbol": symbols[0]})
    elif issubclass(cls, _kkr_imp_wc):
        if not symbols:
            raise KeyError("No symbols supplied.")
        if len(symbols) == 2:
            elmts = _ChemicalElements()
            imp_number = elmts[symbols[0]]
            # wc.inputs.impurity_info.attributes['Zimp']
            if group:
                qb.append(_kkr_imp_wc, with_group='group', tag='imp_wc', project='*')
            else:
                qb.append(_kkr_imp_wc, tag='imp_wc', project='*')
            qb.append(_Dict, with_outgoing='imp_wc', filters={'attributes.Zimp': imp_number})
            qb.append(_RemoteData, with_outgoing='imp_wc', tag='remotedata')
            qb.append(_kkr_scf_wc, with_outgoing='remotedata', tag='scf_wc')
            qb.append(_StructureData, with_outgoing='scf_wc',
                      filters={'attributes.kinds.0.name': symbols[1]})
            # # alternative: require extras
            # qb.append(_StructureData, with_outgoing='scf_wc', filters={"extras.symbol": symbols[1]})

            # # alternative: require extras
            # # note: don't set symbol in workchain extras anymore, so this is deprecated.
            # imp_symbol = ":".join(symbols)
            # if group:
            #     qb.append(_kkr_imp_wc, with_group='group', filters={"extras.embedding_symbol": imp_symbol})
            # else:
            #     qb.append(_kkr_imp_wc, filters={"extras.embedding_symbol": imp_symbol})
        else:
            raise NotImplementedError(f"query not implemented for kkr_imp_wc with no. of symbols other than 2.")
    else:
        raise NotImplementedError(f"workchain query not implemented for class {cls}.")
    return qb  # .all(flat=True)


def query_structure_from(wc: _WorkChainNode) -> _StructureData:
    """Get structure from kkr workchain.

    :param wc: workchain
    :type wc: WorkChainNode of subtype kkr_scf_wc or kkr_imp_wc
    :return: structure if found else None
    :rtype: StructureData
    """
    from aiida.orm import WorkChainNode
    from aiida.engine import WorkChain
    assert isinstance(wc, WorkChain) or isinstance(wc, WorkChainNode)

    wc_cls_str = wc.attributes['process_label']
    if wc_cls_str == '_kkr_scf_wc':
        # solution1: timing 7ms
        return wc.inputs.structure
        # # solution2: timing 27ms
        # return VoronoiCalculation.find_parent_structure(wc)
    elif wc_cls_str == '_kkr_imp_wc':
        # solution1: timing 18 ms
        qb = _QueryBuilder()
        qb.append(_StructureData, tag='struc', project='*')
        qb.append(_kkr_scf_wc, with_incoming='struc', tag='scf_wc')
        qb.append(_RemoteData, with_incoming='scf_wc', tag='remotedata')
        qb.append(_kkr_imp_wc, with_incoming='remotedata', filters={'uuid': wc.uuid})
        res = qb.all(flat=True)
        return res[0] if res else None

        # # solution2: timing 23ms
        # scf = wci.inputs.remote_data_host.get_incoming(node_class=_kkr_scf_wc).all_nodes()
        # return scf[0].inputs.structure if scf else None
    else:
        raise NotImplementedError(f"workchain query not implemented for class {wc_cls_str}.")


def find_Rcut(structure: _StructureData, shell_count: int = 2, rcut_init: float = 7.0) -> float:
    """For GF writeout / impurity workflows: find radius such that only nearest-neighbor shells are included.

    :param structure: structure.
    :param shell_count: include this many nearest-neighbor shells around intended impurity site.
    :param rcut_init: initial maximal rcut value, will be iteratively decreased until fit.
    :return: rcut radius
    """
    import numpy as np

    struc_pmg = structure.get_pymatgen()

    rcut = rcut_init
    nc = 0
    while nc < shell_count:
        dists = struc_pmg.get_neighbor_list(rcut, sites=[struc_pmg.sites[0]])[-1]
        dists = [np.round(i, 5) for i in dists]
        dists.sort()
        nc = len(set(dists))
        rcut += 5

    if nc > shell_count:
        n3start = dists.index(np.sort(list(set(dists)))[shell_count])
        d0, d1 = dists[n3start - 1:n3start + 1]
        rcut = d0 + (d1 - d0) / 2.

    return rcut


class KkrConstantType(_enum.Enum):
    """Used by :py:class:`~aiida_jutools.util_kkr.KkrConstantsChecker`."""
    OLD = 0
    NEW = 1
    NEITHER = 2
    UNDECISIVE = 3


class KkrConstantsChecker:
    """Find out with which version of constants ``ANG_BOHR_KKR``, ``RY_TO_EV_KKR`` the finished workchain used.
    
    In 2021-05, the values of the conversion constants ``ANG_BOHR_KKR`` and  ``RY_TO_EV_KKR`` in
    :py:mod:`~masci_tools.util.constants` were changed from previous values to their NIST values.
    As a result, calculations with the old values cannot be reused in new calculations, otherwise
    the calculation fails with the error ``[read_potential] error ALAT value in potential is does not match``
    in the ``_scheduler-stderr.txt`` output. (The ``constants`` module above offers an option to switch
    back to the old constants values, see its documentation.)
    """

    def __init__(self):
        """Initialization reads the constants' runtime values and compares with environment settings."""
        # from masci_tools.io.constants
        # used by masci_tools.io.common_functions > get_Ang2aBohr()
        # old: before 2021-05
        # new: since 2021-05, NIST value
        # switch to old with os.environ['MASCI_TOOLS_USE_OLD_CONSTANTS']='True'

        # problem is that masci_tools.util.constants constants ANG_BOHR_KKR, RY_TO_EV_KKR definitions (values) depend on
        # the value of the env var os.environ['MASCI_TOOLS_USE_OLD_CONSTANTS'] at module initialization. After that,
        # the definition stays fixed, and changing the value of the env var does not alter it anymore.
        # So, only way is to redefine the constants values here (using the same values as there) in order to have both
        # available at runtime.
        # This also means that we could just query the current constants value to decide whether current env loaded
        # the old or new values. But we will still check the env var in order to cross-check findings. If they don't
        # agree, the implementation logic has likely changed, and this code may be out of order.

        #######################
        # 1) init internal data structures
        self._ANG_BOHR_KKR = {
            KkrConstantType.OLD: 1.8897261254578281,
            KkrConstantType.NEW: 1.8897261246257702
        }
        self._RY_TO_EV_KKR = {
            KkrConstantType.OLD: 13.605693009,
            KkrConstantType.NEW: 13.605693122994
        }
        self._runtime_const_type = {
            'ANG_BOHR_KKR': None,
            'RY_TO_EV_KKR': None
        }
        # create an empty DataFrame to hold one row of data for each check workchain.
        self._df_index_name = 'workchain_uuid'
        self._df_schema = {
            'ctime': object,  # workchain ctime
            'group': str, # group label, if specified
            'ANG_BOHR_KKR': _np.float64,  # recalculated from alat, bravais
            'constant_type': object,  # type of recalculated ANG_BOHR_KKR (old, new, neither) based on abs_tol
            'diff_old': _np.float64,  # abs. difference recalculated - old ANG_BOHR_KKR value
            'diff_new': _np.float64  # abs. difference recalculated - new ANG_BOHR_KKR value
        }
        self._df = _pd.DataFrame(columns=self._df_schema.keys()).astype(self._df_schema)
        self._df.index.name = self._df_index_name

        #######################
        # 2) read in current constants values and cross-check with environment

        # get the current ANG_BOHR_KKR value
        # note: aiida-kkr uses masci_tools.io.common_functions get_Ang2aBohr (=ANG_BOHR_KKR),
        #       get_aBohr2Ang() (=1/ANG_BOHR_KKR), get_Ry2eV (=RY_TO_EV_KKR) instead, but this is redundant.
        #       Here we import the constants directly.
        msg_suffix = "This could indicate an implementation change. " \
                     "As a result, this function might not work correctly anymore."

        if _ANG_BOHR_KKR == self.ANG_BOHR_KKR[KkrConstantType.NEW]:
            self._runtime_const_type['ANG_BOHR_KKR'] = KkrConstantType.NEW
        if _ANG_BOHR_KKR == self.ANG_BOHR_KKR[KkrConstantType.OLD]:
            self._runtime_const_type['ANG_BOHR_KKR'] = KkrConstantType.OLD
        else:
            self._runtime_const_type['ANG_BOHR_KKR'] = KkrConstantType.NEITHER
            print(f"Warning: The runtime value of constant ANG_BOHR_KKR matches no expected value. {msg_suffix}")

        # env var cases: 3: None, 'True', not {None, 'True'}.
        # const type cases: 3: NEW, OLD, NEITHER.
        # cross-product: 3 x 3 = 9.
        #
        # | env var            | const type | valid | defined | reaction  | case |
        # | ------------------ | ---------- | ----- | ------- | --------- | ---- |
        # | None               | NEW        | yes   | yes     | pass      | D    |
        # | None               | OLD        | no    | no      | exception | A    |
        # | None               | NEITHER    | no    | no      | exception | A    |
        # | 'True'             | New        | no    | no      | exception | B    |
        # | 'True'             | OLD        | yes   | yes     | pass      | D    |
        # | 'True'             | NEITHER    | no    | no      | exception | B    |
        # | not {None, 'True'} | NEW        | no    | no      | pass(1)   | D    |
        # | not {None, 'True'} | OLD        | no    | no      | exception | C    |
        # | not {None, 'True'} | NEITHER    | no    | no      | exception | C    |
        # Annotations:
        # - case 'D' = 'else' = 'pass'.
        # - (1): passes with warning, from const type NEITHER above.

        # double-check with environment variable
        env_var_key = 'MASCI_TOOLS_USE_OLD_CONSTANTS'
        env_var_val = _os.environ.get(env_var_key, None)
        const_type = self._runtime_const_type['ANG_BOHR_KKR']
        cases = {
            'A': (env_var_val is None and const_type != KkrConstantType.NEW),
            'B': (env_var_val == 'True' and const_type != KkrConstantType.OLD),
            'C': (env_var_val not in [None, 'True'] and const_type != KkrConstantType.NEW)
        }
        if cases['A'] or cases['C']:
            raise ValueError(
                f"Based on environment variable {env_var_key}={env_var_val}, I expected constant values to "
                f"be of type {KkrConstantType.NEW}, but they are of type {const_type}. "
                f"{msg_suffix}")
        elif cases['B']:
            raise ValueError(
                f"Based on environment variable {env_var_key}={env_var_val}, I expected constant values to "
                f"be of type {KkrConstantType.OLD}, but they are of type {const_type}. "
                f"{msg_suffix}")
        else:
            pass

    @property
    def ANG_BOHR_KKR(self) -> dict:
        """Old and new value of KKR constant ANG_BOHR_KKR."""
        return self._ANG_BOHR_KKR

    @property
    def RY_TO_EV_KKR(self) -> dict:
        """Old and new value of KKR constant RY_TO_EV_KKR."""
        return self._RY_TO_EV_KKR

    def get_runtime_type(self, constant_name: str = 'ANG_BOHR_KKR') -> KkrConstantType:
        """Get KKR constant type (old, new or neither) of runtime constant value.

        :param constant_name: name of the constant.
        """
        if constant_name in self._runtime_const_type:
            return self._runtime_const_type[constant_name]
        else:
            print(f"Warning: Unknown constant name '{constant_name}'. "
                  f"Known constant names: {self._runtime_const_type[constant_name]}. "
                  f"I will return nothing.")

    @property
    def df(self) -> _pd.DataFrame:
        """DataFrame containing all checked workchain data."""
        return self._df

    def clear(self):
        """Clear all previous workchain checks."""
        # drop all rows from dataframe
        self._df = self._df.drop(labels=self._df.index)

    def check_workchain(self, wc: _WorkChainNode,
                        zero_threshold:float = 1e-15,
                        group_label:str=None):
        """This function finds out whether the given finished workchain was run with the old or new constants values.

        :param wc: finished workchain of type kkr_scf_wc.
        :param zero_threshold: Set structure cell elements below threshold to zero to counter rounding errors.
        :param group_label: optional: specify group label the workchain belongs to.
        """

        if wc.uuid in self._df.index:
            print(f"Info: skipping Workchain {wc}: is already checked.")
            return

        #######################
        # 1) init internal variables

        # temp values for new dataframe row elements
        row = {key: _np.NAN for key in self._df_schema}
        ALATBASIS = None
        BRAVAIS = None
        POSITIONS = None
        ANG_BOHR_KKR = None
        constant_type = None
        diff_old = None
        diff_new = None

        structure = wc.inputs.structure

        structure_cell = _np.array(structure.cell)
        _math_util.set_zero_below_threshold(structure_cell, threshold=zero_threshold)

        structure_positions = []
        for sites in structure.sites:
            structure_positions.append(sites.position)
        structure_positions = _np.array(structure_positions)
        _math_util.set_zero_below_threshold(structure_positions, threshold=zero_threshold)

        #######################
        # 2) read original alat and bravais from first inputcard
        # For now, this is implemented for kkr_scf_wc > kkr_startpot_wc > VoronoiCalculation only.

        startpots = wc.get_outgoing(node_class=_kkr_startpot_wc).all_nodes()

        msg_prefix = f"Warning: skipping Workchain {wc}"
        msg_suffix = f"Method not implemented for such workchains"
        if not startpots:
            print(f"{msg_prefix}: Does not have a {_kkr_startpot_wc.__name__} descendant. {msg_suffix}.")
            return
        else:
            vorocalcs = startpots[0].get_outgoing(node_class=_VoronoiCalculation).all_nodes()
            if not vorocalcs:
                print(f"{msg_prefix}: Does not have a {_VoronoiCalculation.__name__} descendant. {msg_suffix}.")
                return
            else:
                vorocalc = vorocalcs[0]
                # vorocalc.get_retrieve_list()
                inputcard = vorocalc.get_object_content('inputcard')
                inputcard = inputcard.split('\n')

                # read alat value
                indices = [idx for idx, line in enumerate(inputcard) if 'ALATBASIS' in line]
                if len(indices) == 1:
                    ALATBASIS = float(inputcard[indices[0]].split()[1])
                else:
                    print(f"{msg_prefix}: Could not read 'ALATBASIS' value from inputcard file. {msg_suffix}.")
                    return

                def read_field(keyword:str):
                    lines = []
                    reading = False
                    for i, line in enumerate(inputcard):
                        if reading:
                            if line.startswith(' '):
                                lines.append(line)
                            else:
                                reading = False
                        if keyword in line:
                            reading = True
                    array = []
                    for line in lines:
                        array.append([float(numstr) for numstr in line.split()])
                    array = _np.array(array)
                    return array

                # read bravais value(s)
                # Typically, inputcard has line 'BRAVAIS', followed by 3 linex of 1x3 bravais matrix values.
                BRAVAIS = read_field(keyword='BRAVAIS')

                # read position value(s)
                # Typically, inputcard has line '<RBASIS>', followed by x linex of 1x3 bravais matrix values.
                POSITIONS = read_field(keyword='<RBASIS>')


        #######################
        # 3) Recalculate ANG_BOHR_KKR from inputcard alat and bravais

        # print(40*'-')
        # print('wc', wc.uuid)
        # print('alat')
        # print(ALATBASIS)
        # print('bravais')
        # print(BRAVAIS)
        # print('structure cell')
        # print(structure_cell)
        # print('positions')
        # print(POSITIONS)
        # print('structure positions')
        # print(structure_positions)

        # def reverse_calc_ANG_BOHR_KKR(inp_arr:_np.ndarray, struc_arr:_np.ndarray):
        #     def reverse_calc_single_ANG_BOHR_KKR(x: float, y: float):
        #         return ALATBASIS * x / y if (y != 0.0 and x != 0.0) else 0.0
        #
        #     if inp_arr.shape == struc_arr.shape:
        #         ANG_BOHR_KKR = _np.mean([reverse_calc_single_ANG_BOHR_KKR(x, y)
        #                                  for x, y in _np.nditer([inp_arr, struc_arr])
        #                                  if x != 0.0 and y != 0.0])
        #         return ANG_BOHR_KKR
        #     else:
        #         print(f"{msg_prefix}: Shapes of inputcard matrix and structure matrix "
        #               f"do not match: {inp_arr.shape} != {struc_arr.shape}.")
        #         return

        # a2b_bra = reverse_calc_ANG_BOHR_KKR(BRAVAIS, structure_cell)
        # a2b_pos = reverse_calc_ANG_BOHR_KKR(POSITIONS, structure_positions)
        # ANG_BOHR_KKR = (a2b_bra + a2b_pos) / 2


        def reverse_calc_ANG_BOHR_KKR(inp_arr:_np.ndarray, struc_arr:_np.ndarray):
            def reverse_calc_single_ANG_BOHR_KKR(x: float, y: float):
                # print(f'calc ALATBASIS * {x} / {y}')
                return ALATBASIS * x / y if (y != 0.0 and x != 0.0) else 0.0


            if inp_arr.shape == struc_arr.shape:
                ANG_BOHR_KKR_list = [reverse_calc_single_ANG_BOHR_KKR(x, y)
                                         for x, y in _np.nditer([inp_arr, struc_arr])]
                return ANG_BOHR_KKR_list
            else:
                print(f"{msg_prefix}: Shapes of inputcard matrix and structure matrix "
                      f"do not match: {inp_arr.shape} != {struc_arr.shape}.")
                return

        a2b_list = []
        a2b_list.extend(reverse_calc_ANG_BOHR_KKR(BRAVAIS, structure_cell))
        a2b_list.extend(reverse_calc_ANG_BOHR_KKR(POSITIONS, structure_positions))
        a2b_list = _np.array(a2b_list)

        a2b_list = _math_util.drop_values(a2b_list, 'zero', 'nan')

        # print('a2b_list')
        # print(a2b_list)

        ANG_BOHR_KKR = _np.mean(a2b_list)


        diff_old = abs(ANG_BOHR_KKR - self.ANG_BOHR_KKR[KkrConstantType.OLD])
        diff_new = abs(ANG_BOHR_KKR - self.ANG_BOHR_KKR[KkrConstantType.NEW])
        if (diff_new > 1e-9 and diff_old > 1e-9):
            # in this case, reverse calculation likely failed and can't say anything about original constant value
            constant_type = KkrConstantType.UNDECISIVE
        else:
            constant_type = KkrConstantType.NEW if (diff_new < diff_old) else KkrConstantType.OLD
            constant_type = constant_type if (diff_new != diff_old) else KkrConstantType.UNDECISIVE

        #######################
        # 4) Add results to dataframe
        if group_label:
            row['group'] = group_label
        row['ctime'] = wc.ctime
        row['ANG_BOHR_KKR'] = ANG_BOHR_KKR
        row['constant_type'] = constant_type
        row['diff_old'] = diff_old
        row['diff_new'] = diff_new

        self._df = self._df.append(_pd.Series(name=wc.uuid, data=row))

    def check_workchain_group(self, group: _Group,
                              process_labels: list = ['kkr_scf_wc'],
                              zero_threshold:float = 1e-15):
        """Reverse calculate ANG_BOHR_KKR constants for a group of workchains.

        :param group: a group with workchain nodes.
        :param process_labels: list of valid process labels, e.g. ['kkr_scf_wc'] for aiida-kkr plugin.
        :param zero_threshold: Set structure cell elements below threshold to zero to counter rounding errors.
        """
        for node in group.nodes:
            if isinstance(node, _WorkChainNode) and node.process_label in process_labels:
                self.check_workchain(wc=node,
                                     zero_threshold=zero_threshold,
                                     group_label=group.label)
