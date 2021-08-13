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

import collections as _collections
import datetime as _datetime
import enum as _enum
import os as _os
import typing as _typing

import aiida.engine as _aiida_engine
import aiida.orm as _orm
import aiida_kkr.calculations as _kkr_calculations
import aiida_kkr.workflows as _kkr_workflows
import masci_tools.util.chemical_elements as _masci_chemical_elements
import masci_tools.util.constants as _masci_constants
import masci_tools.util.python_util as _masci_python_util
import numpy as _np
import pandas as _pd
import pytz as _pytz
from masci_tools.util import math_util as _masci_math_util


def has_kkr_calc_converged(kkr_calc: _orm.CalcJobNode) -> bool:
    """Assert (fail if false) that kkr calculation has converged.

    DEVNOTE: used aiida base node type for argument type so it works with all kkr calc node types.

    E.g. needed for host GF writeout

    Reference: https://aiida-kkr.readthedocs.io/en/stable/user_guide/calculations.html#special-run-modes-host-gf-writeout-for-kkrimp

    :param kkr_calc: performed kkr calculation
    :return: True if converged, else False.
    """
    try:
        return kkr_calc.outputs.output_parameters.get_dict()["convergence_group"]["calculation_converged"] is True
    except KeyError as err:
        print("Error: calculation is not a kkr calculation.")
        raise err


def query_kkr_wc(cls: _typing.Type[_aiida_engine.WorkChain],
                 symbols: _typing.List[str] = ['H', 'H'],
                 group: _orm.Group = None) -> _orm.QueryBuilder:
    """Query kkr workchains based on their input structures.

    Constraints:

    - if kkr_scf_wc, and symbols given, queries with first symbol only (elemental crystal).
    - if kkr_imp_wc, requires symbols, queries with first symbol = impurity, second symbol = host crystal.

    For general workchain queries, use :py:func:`~aiida_jutools.util_process.query_processes` instead.

    :param cls: kkr workchain class. kkr_scf_wc or kkr_imp_wc.
    :param symbols: list of chemical element symbols.
    :param group: given: search in group, not: search in database
    :return: the built query for matching workchains
    """
    if isinstance(symbols, str):
        symbols = [symbols]

    qb = _orm.QueryBuilder()
    if group:
        qb.append(_orm.Group, filters={'label': group.label}, tag='group')
    if issubclass(cls, _kkr_workflows.kkr_scf_wc):
        if group:
            qb.append(_kkr_workflows.kkr_scf_wc, with_group='group', tag='workchain', project='*')
        else:
            qb.append(_kkr_workflows.kkr_scf_wc, tag='workchain', project='*')
        if symbols:
            qb.append(_orm.StructureData, with_outgoing='workchain',
                      filters={'attributes.kinds.0.name': symbols[0]})
            # # alternative: require extras
            # qb.append(_orm.StructureData, with_outgoing='workchain', filters={"extras.symbol": symbols[0]})
    elif issubclass(cls, _kkr_workflows.kkr_imp_wc):
        if not symbols:
            raise KeyError("No symbols supplied.")
        if len(symbols) != 2:
            raise NotImplementedError(f"query not implemented for kkr_imp_wc with no. of symbols other than 2.")
        elmts = _masci_chemical_elements.ChemicalElements()
        imp_number = elmts[symbols[0]]
        # wc.inputs.impurity_info.attributes['Zimp']
        if group:
            qb.append(_kkr_workflows.kkr_imp_wc, with_group='group', tag='imp_wc', project='*')
        else:
            qb.append(_kkr_workflows.kkr_imp_wc, tag='imp_wc', project='*')
        qb.append(_orm.Dict, with_outgoing='imp_wc', filters={'attributes.Zimp': imp_number})
        qb.append(_orm.RemoteData, with_outgoing='imp_wc', tag='remotedata')
        qb.append(_kkr_workflows.kkr_scf_wc, with_outgoing='remotedata', tag='scf_wc')
        qb.append(_orm.StructureData, with_outgoing='scf_wc',
                  filters={'attributes.kinds.0.name': symbols[1]})

        # # alternative: require extras
        # qb.append(_orm.StructureData, with_outgoing='scf_wc', filters={"extras.symbol": symbols[1]})

        # # alternative: require extras
        # # note: don't set symbol in workchain extras anymore, so this is deprecated.
        # imp_symbol = ":".join(symbols)
        # if group:
        #     qb.append(_kkr_workflows.kkr_imp_wc, with_group='group', filters={"extras.embedding_symbol": imp_symbol})
        # else:
        #     qb.append(_kkr_workflows.kkr_imp_wc, filters={"extras.embedding_symbol": imp_symbol})
    else:
        raise NotImplementedError(f"workchain query not implemented for class {cls}.")
    return qb  # .all(flat=True)


def query_structure_from(wc: _orm.WorkChainNode) -> _orm.StructureData:
    """Get structure from kkr workchain.

    :param wc: workchain
    :return: structure if found else None
    """
    assert isinstance(wc, (_aiida_engine.WorkChain, _orm.WorkChainNode))

    wc_cls_str = wc.attributes['process_label']
    if wc_cls_str == 'kkr_scf_wc':
        # solution1: timing 7ms
        return wc.inputs.structure
        # # solution2: timing 27ms
        # return VoronoiCalculation.find_parent_structure(wc)
    elif wc_cls_str == 'kkr_imp_wc':
        # solution1: timing 18 ms
        qb = _orm.QueryBuilder()
        qb.append(_orm.StructureData, tag='struc', project='*')
        qb.append(_kkr_workflows.kkr_scf_wc, with_incoming='struc', tag='scf_wc')
        qb.append(_orm.RemoteData, with_incoming='scf_wc', tag='remotedata')
        qb.append(_kkr_workflows.kkr_imp_wc, with_incoming='remotedata', filters={'uuid': wc.uuid})
        res = qb.all(flat=True)
        return res[0] if res else None

        # # solution2: timing 23ms
        # scf = wci.inputs.remote_data_host.get_incoming(node_class=_kkr_workflows.kkr_scf_wc).all_nodes()
        # return scf[0].inputs.structure if scf else None
    else:
        raise NotImplementedError(f"workchain query not implemented for class {wc_cls_str}.")


def find_Rcut(structure: _orm.StructureData,
              shell_count: int = 2,
              rcut_init: float = 7.0) -> float:
    """For GF writeout / impurity workflows: find radius such that only nearest-neighbor shells are included.

    :param structure: structure.
    :param shell_count: include this many nearest-neighbor shells around intended impurity site (cluster size).
    :param rcut_init: initial maximal rcut value, will be iteratively decreased until fit to shell count.
    :return: rcut radius
    """
    struc_pmg = structure.get_pymatgen()

    rcut = rcut_init
    nc = 0
    while nc < shell_count:
        dists = struc_pmg.get_neighbor_list(rcut, sites=[struc_pmg.sites[0]])[-1]
        dists = [_np.round(i, 5) for i in dists]
        dists.sort()
        nc = len(set(dists))
        rcut += 5

    if nc > shell_count:
        n3start = dists.index(_np.sort(list(set(dists)))[shell_count])
        d0, d1 = dists[n3start - 1:n3start + 1]
        rcut = d0 + (d1 - d0) / 2.

    return rcut


class KkrConstantsVersion(_enum.Enum):
    """Enum for labeling different KKR constants version.

    Used by :py:class:`~aiida_jutools.util_kkr.KkrConstantsChecker`.

    The enum values represent the respective constants values from different time spans

    - :py:func:`~masci_tools.io.common_functions.get_Ang2aBohr`
    - :py:func:`~masci_tools.io.common_functions.get_aBohr2Ang`
    - :py:func:`~masci_tools.io.common_functions.get_Ry2eV`
    - :py:data:`~masci_tools.util.constants.ANG_BOHR_KKR`
    - :py:data:`~masci_tools.util.constants.RY_TO_EV_KKR`

    Here is an overview of their values from the commit history timespan when the values underwent change.

    ==========  ===========  =========  ===================  ====================  ==================
    date        commit hash  type       ang 2 bohr constant  bohr to ang constant  ry to ev constant
    ==========  ===========  =========  ===================  ====================  ==================
    2018-10-26  04d55ea      'old'      1.8897261254578281   0.5291772106700000    13.605693009000000
    2021-02-16  c171563      'interim'  1.8897261249935897   0.5291772108000000    13.605693122994000
    2021-04-28  66953f8      'old'      1.8897261254578281   0.5291772106700000    13.605693009000000
    2021-04-28  66953f8      'new'      1.8897261246257702   0.5291772109030000    13.605693122994000
    ==========  ===========  =========  ===================  ====================  ==================

    Use :py:attr:`~aiida_jutools.util_kkr.KkrConstantsVersion.OLD.description` (or on any other enum) to get a
    machine-readable version of this table.

    So we have the following correspondence for ang 2 bohr constant / bohr to ang constant:

    - OLD: [2018-10-26, 2021-02-16] and [2021-04-28,] (type 'old')
    - INTERIM: [2021-02-16, 2021-04-28]
    - NEW: [2021-04-28,] (type 'new')

    For ry to ev constant, we have:

    - OLD: as above
    - INTERIM: same as NEW.
    - NEW: [2021-02-16, 2021-04-28] and [2021-04-28,] (type 'new')

    For constants values reverse-calculated from finished workchain for classification, we have the additional enums:

    - NEITHER: for constants values recalculated from workchains which fit neither of the above by a wide margin
    - UNDECISIVE: for constants values recalculated from workchains which fit neither of the above but are in range

    Note: The order here reflects the importance. NEW should be preferred, OLD be used for old workchains performed
    with these values, INTERIM should be avoided since masci-tools versions 2021-04-28 do not know these values,
    NEITHER and UNDECISIVE are for workchain, not constants, reverse classification purposes only.
    """
    NEW = 0
    OLD = 1
    INTERIM = 2
    NEITHER = 3
    UNDECISIVE = 4

    @property
    def description(self) -> _typing.Union[_typing.Dict[str, _typing.Union[str, _datetime.datetime]], str]:
        """Describe constants versions.

        Returns either a dictionary or a string, depending on the enum.

        The returned dictionary describes from when to when the respective KKR constants
        version was defined in the respective masci-tools version, here denoted by
        commit hashes, as they are more accurate than the librarie's version numbers.
        The left and right time limits are denoted by datetime objects of year one, and now.

        This can be taken as a machine-readable indicator for comparison against classification results of
        :py:class:`~aiida_jutools.util_kkr.KkrConstantsChecker` to validate whether the constants versions
        there found for a workchain fit within the respective constants version's timefame described here.

        Keep in mind though that the respective inspected workchain might have been run with an older masci-tools
        version at creation time. So any constants version older than the workchain's creation time are legit,
        only newer ones are impossible.
        """
        if self.name == KkrConstantsVersion.NEW.name:
            return {'commit': "66953f8",
                    'valid_from': _datetime.datetime(year=2021, month=4, day=28,
                                                     hour=14, minute=2, second=0,
                                                     microsecond=0, tzinfo=_pytz.UTC),
                    'valid_until': _masci_python_util.now()
                    }
        elif self.name == KkrConstantsVersion.INTERIM.name:
            return {'commit': "c171563",
                    'valid_from': _datetime.datetime(year=2021, month=2, day=16,
                                                     hour=19, minute=40, second=0,
                                                     microsecond=0, tzinfo=_pytz.UTC),
                    'valid_until': _datetime.datetime(year=2021, month=4, day=28,
                                                      hour=14, minute=2, second=0,
                                                      microsecond=0, tzinfo=_pytz.UTC)
                    }
        elif self.name == KkrConstantsVersion.OLD.name:
            return {'commit': "04d55ea",
                    'valid_from': _datetime.datetime(year=1, month=1, day=1,
                                                     hour=0, minute=0, second=0,
                                                     microsecond=0, tzinfo=_pytz.UTC),
                    'valid_until': _datetime.datetime(year=2021, month=2, day=16,
                                                      hour=19, minute=40, second=0,
                                                      microsecond=0, tzinfo=_pytz.UTC)
                    }
        elif self.name in [KkrConstantsVersion.NEITHER.name, KkrConstantsVersion.UNDECISIVE.name]:
            return f"For classification of aiida-kkr workchains by class {KkrConstantsVersionChecker.__name__}."
        else:
            raise NotImplementedError("Enum with undefined behavior. Contact developer.")


class KkrConstantsVersionChecker:
    """Find out with which version of constants ``ANG_BOHR_KKR``, ``RY_TO_EV_KKR`` finished aiida-kkr
    workchain were run.

    Between 2021-02-16 and 2021-04-28, the values of the conversion constants ``ANG_BOHR_KKR`` and
    ``RY_TO_EV_KKR`` in :py:mod:`~masci_tools.util.constants` were changed from previous values to a set of
    intermediate values, and then finally to NIST values. See :py:class:`~aiida_jutools.util_kkr.KkrConstantsVersion`
    docstring for a complete list. The ``constants`` module mentioned above offers an option to switch
    back to the older constants versions, see its documentation.

    As a result, calculations with the old constants versions cannot be reused in new calculations, otherwise
    the calculation fails with the error ``[read_potential] error ALAT value in potential is does not match``
    in the ``_scheduler-stderr.txt`` output.

    This class checks aiida-kkr workchains for the constants version it likely was performed with. The result
    is a DataFrame with one row for each checked workchain.

    Currently, this class only reverse-calculates the ``ANG_BOHR_KKR`` constant from a workchain for version checking.
    The ``RY_TO_EV_KKR`` constant is not used.
    """

    def __init__(self,
                 check_env: bool = True):
        """Initialization reads the constants' runtime versions and cross-checks with environment settings.

        :param check_env: True: check found runtime version against expected environment variable setting,
                          except if unexpected setting. False: don't check.
        """

        # problem is that masci_tools.util.constants constants ANG_BOHR_KKR, RY_TO_EV_KKR definitions (values) depend on
        # the value of the env var os.environ['MASCI_TOOLS_USE_OLD_CONSTANTS'] at module initialization. After that,
        # the definition stays fixed, and changing the value of the env var does not alter it anymore.
        # So, only way to have access to all versions at runtime is to redefine the constants values here
        # (using the same values as there).
        # This also means that we could just query the current constants value to decide whether current env loaded
        # the old or new values. But we will still check the env var in order to cross-check findings. If they don't
        # agree, the implementation logic has likely changed, and this code may be out of order.

        #######################
        # 1) init internal data structures
        self._ANG_BOHR_KKR = {  # order importance (not by value): NEW > OLD > INTERIM
            KkrConstantsVersion.NEW: 1.8897261246257702,
            KkrConstantsVersion.INTERIM: 1.8897261249935897,
            KkrConstantsVersion.OLD: 1.8897261254578281,
        }
        self._RY_TO_EV_KKR = {  # order importance (not by value): NEW > OLD
            KkrConstantsVersion.NEW: 13.605693122994,
            KkrConstantsVersion.INTERIM: 13.605693122994,
            KkrConstantsVersion.OLD: 13.605693009,
        }
        self._runtime_version = None

        # create an empty DataFrame to hold one row of data for each check workchain.
        self._df_index_name = 'workchain_uuid'
        self._df_schema = {
            'ctime': object,  # workchain ctime
            'group': str,  # group label, if specified
            'ANG_BOHR_KKR': _np.float64,  # recalculated from alat, bravais
            'constants_version': object,  # type of recalculated ANG_BOHR_KKR (old, new, neither) based on abs_tol
            'diff_new': _np.float64,  # abs. difference recalculated - new ANG_BOHR_KKR value
            'diff_old': _np.float64,  # abs. difference recalculated - old ANG_BOHR_KKR value
            'diff_interim': _np.float64,  # abs. difference recalculated - interim ANG_BOHR_KKR value
        }
        self._records = _pd.DataFrame(columns=self._df_schema.keys()).astype(self._df_schema)
        self._records.index.name = self._df_index_name

        #######################
        # 2) read in current constants values and cross-check with environment

        # get the current ANG_BOHR_KKR value
        # note: aiida-kkr uses masci_tools.io.common_functions get_Ang2aBohr (=ANG_BOHR_KKR),
        #       get_aBohr2Ang() (=1/ANG_BOHR_KKR), get_Ry2eV (=RY_TO_EV_KKR) instead, but this is redundant.
        #       Here we import the constants directly.
        # note:
        msg_suffix = "This could indicate an implementation change. " \
                     "As a result, this function might not work correctly anymore."

        if _masci_constants.ANG_BOHR_KKR == self.ANG_BOHR_KKR[KkrConstantsVersion.NEW]:
            self._runtime_version = KkrConstantsVersion.NEW
        elif _masci_constants.ANG_BOHR_KKR == self.ANG_BOHR_KKR[KkrConstantsVersion.INTERIM]:
            self._runtime_version = KkrConstantsVersion.INTERIM
        elif _masci_constants.ANG_BOHR_KKR == self.ANG_BOHR_KKR[KkrConstantsVersion.OLD]:
            self._runtime_version = KkrConstantsVersion.OLD
        else:
            self._runtime_version = KkrConstantsVersion.NEITHER
            print(f"Warning: The KKR constants version the runtime is using could not be determined: "
                  f"The runtime value of constant ANG_BOHR_KKR matches no expected value. {msg_suffix}")

        # env var cases: 4: None, 'interim', 'old', not {None, 'old', 'interim'}.
        # const type cases: 4: NEW, OLD, INTERIM, NEITHER.
        # cross-product: 4 x 4 = 16.
        # this assumes that current masci-tools version supports the environment switch WITH the 'Interim' option,
        # i.e. from 2021-01-08 or newer (switch was implemented 2021-04-28, without 'Interim' option).
        #
        # | env var                       | const type | valid | defined | reaction  | case |
        # | ------------------            | ---------- | ----- | ------- | --------- | ---- |
        # | None                          | NEW        | yes   | yes     | pass      | E    |
        # | None                          | INTERIM    | no    | no      | exception | A    |
        # | None                          | OLD        | no    | no      | exception | A    |
        # | None                          | NEITHER    | no    | no      | exception | A    |
        # | 'Interim                      | New        | no    | no      | exception | B    |
        # | 'interim'                     | INTERIM    | yes   | yes     | pass      | E    |
        # | 'interim'                     | OLD        | yes   | yes     | exception | B    |
        # | 'interim'                     | NEITHER    | no    | no      | exception | B    |
        # | 'old'                         | New        | no    | no      | exception | C    |
        # | 'old'                         | INTERIM    | no    | no      | exception | C    |
        # | 'old'                         | OLD        | yes   | yes     | pass      | E    |
        # | 'old'                         | NEITHER    | no    | no      | exception | C    |
        # | not {None, 'old', 'interim'}  | NEW        | no    | no      | pass(1)   | E    |
        # | not {None, 'old', 'interim'}  | INTERIM    | no    | no      | exception | D    |
        # | not {None, 'old', 'interim'}  | OLD        | no    | no      | exception | D    |
        # | not {None, 'old', 'interim'}  | NEITHER    | no    | no      | exception | D    |
        # Annotations:
        # - case 'D' = 'else' = 'pass'.
        # - (1): passes with warning, from const type NEITHER above.

        if check_env:
            # double-check with environment variable
            env_var_key = 'MASCI_TOOLS_USE_OLD_CONSTANTS'
            env_var_val = _os.environ.get(env_var_key, None)
            runtime_version = self.runtime_version

            cases = {
                'A': (runtime_version != KkrConstantsVersion.NEW and env_var_val is None),
                'B': (runtime_version != KkrConstantsVersion.INTERIM and env_var_val == 'interim'),
                'C': (runtime_version != KkrConstantsVersion.OLD and env_var_val == 'old'),
                'D': (runtime_version != KkrConstantsVersion.NEW and env_var_val not in [None, 'old', 'interim'])
            }
            if cases['A'] or cases['D']:
                raise ValueError(
                    f"Based on environment variable {env_var_key}={env_var_val}, I expected constant values to "
                    f"be of type {KkrConstantsVersion.NEW}, but they are of type {runtime_version}. "
                    f"{msg_suffix}")
            elif cases['B']:
                raise ValueError(
                    f"Based on environment variable {env_var_key}={env_var_val}, I expected constant values to "
                    f"be of type {KkrConstantsVersion.INTERIM}, but they are of type {runtime_version}. "
                    f"{msg_suffix}")
            elif cases['C']:
                raise ValueError(
                    f"Based on environment variable {env_var_key}={env_var_val}, I expected constant values to "
                    f"be of type {KkrConstantsVersion.OLD}, but they are of type {runtime_version}. "
                    f"{msg_suffix}")

    @property
    def ANG_BOHR_KKR(self) -> _typing.Dict[KkrConstantsVersion, float]:
        """All constants versions of the conversion constant ``ANG_BOHR_KKR`` (Angstrom to Bohr radius)."""
        return self._ANG_BOHR_KKR

    @property
    def RY_TO_EV_KKR(self) -> _typing.Dict[KkrConstantsVersion, float]:
        """All constants versions of the conversion constant ``RY_TO_EV_KKR`` (Rydberg to electron Volt)."""
        return self._RY_TO_EV_KKR

    @property
    def runtime_version(self) -> KkrConstantsVersion:
        """Get KKR constant version which the interpreter is using at runtime."""
        return self._runtime_version

    @property
    def records(self) -> _pd.DataFrame:
        """DataFrame containing all checked workchain records."""
        return self._records

    def clear(self):
        """Drop the records from previous workchain checks from memory."""
        self._records = self._records.drop(labels=self._records.index)

    def check_single_workchain(self,
                               wc: _orm.WorkChainNode,
                               record: bool = False,
                               set_extra: bool = False,
                               zero_threshold: float = 1e-15,
                               group_label: str = None) -> _typing.Optional[KkrConstantsVersion]:
        """Classify a finished workchain by its used KKR constants version by reverse-calculation.

        Current implementation only works with aiida-kkr workflows which have a ``kkr_startpot_wc`` descendant.
        These are: ``kkr_scf_wc``, ``kkr_eos_wc``, ``kkr_imp_wc``.

        If ``record`` is False, the constants version used by the workchain is returned. If ``record`` is False,
        the result is appended to the dataframe  :py:attr:`~aiida_jutools.util_kkr.KkrConstantsVersionChecker.records`.

        Will always check constants version by recalculating it, even if it may have already been set as an
        extra. To filter workchains by this extra, assuming it has already been set for them, use the method
        :py:meth:`~aiida_jutools.util_kkr.filter_using_runtime_version` instead.

        :param wc: finished aiida-kkr workchain.
        :param record: False: return constants version of workchain. True: record results in dataframe.
        :param zero_threshold: Set structure cell elements below this threshold to zero to counter rounding errors.
        :param set_extra: True: Set an extra on the workchain denoting the identified KKR constants version and values.
        :param group_label: optional: specify group label the workchain belongs to.
        """

        if wc.uuid in self._records.index:
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
        constants_version = None

        structure = query_structure_from(wc)

        structure_cell = _np.array(structure.cell)
        _masci_math_util.set_zero_below_threshold(structure_cell, threshold=zero_threshold)

        structure_positions = []
        for sites in structure.sites:
            structure_positions.append(sites.position)
        structure_positions = _np.array(structure_positions)
        _masci_math_util.set_zero_below_threshold(structure_positions, threshold=zero_threshold)

        #######################
        # 2) Read original alat and bravais from first inputcard
        # For now, this is implemented for aiida-kkr workchains with a single
        #  kkr_startpot_wc > VoronoiCalculation descendant only.

        startpots = wc.get_outgoing(node_class=_kkr_workflows.kkr_startpot_wc).all_nodes()

        msg_prefix = f"Warning: skipping Workchain {wc}"
        msg_suffix = f"Method not implemented for such workchains"
        if not startpots:
            print(f"{msg_prefix}: Does not have a {_kkr_workflows.kkr_startpot_wc.__name__} descendant. {msg_suffix}.")
            return
        else:
            vorocalcs = None
            # workchain might have several startpot descendants, one of which should hava a vorocalc descendant.
            for startpot in startpots:
                vorocalcs = startpot.get_outgoing(node_class=_kkr_calculations.VoronoiCalculation).all_nodes()
                if vorocalcs:
                    break
            if not vorocalcs:
                print(
                    f"{msg_prefix}: Does not have a {_kkr_calculations.VoronoiCalculation.__name__} descendant. "
                    f"{msg_suffix}.")
                return
            else:
                vorocalc = vorocalcs[0]
                # vorocalc.get_retrieve_list()
                try:
                    inputcard = vorocalc.get_object_content('inputcard')
                    inputcard = inputcard.split('\n')

                    # read alat value
                    indices = [idx for idx, line in enumerate(inputcard) if 'ALATBASIS' in line]
                    if len(indices) == 1:
                        ALATBASIS = float(inputcard[indices[0]].split()[1])
                    else:
                        print(f"{msg_prefix}: Could not read 'ALATBASIS' value from inputcard file. {msg_suffix}.")
                        return

                    def read_field(keyword: str) -> _np.ndarray:
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

                except FileNotFoundError as err:
                    print(f"{msg_prefix}: {FileNotFoundError.__name__}: Could not retrieve inputcard from its "
                          f"{_kkr_calculations.VoronoiCalculation.__name__} {vorocalc}.")
                    return

        #######################
        # 3) Recalculate ANG_BOHR_KKR from inputcard alat and bravais
        def reverse_calc_ANG_BOHR_KKR(inp_arr: _np.ndarray,
                                      struc_arr: _np.ndarray):
            def reverse_calc_single_ANG_BOHR_KKR(x: float,
                                                 y: float) -> float:
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

        a2b_list = _masci_math_util.drop_values(a2b_list, 'zero', 'nan')

        # print('a2b_list')
        # print(a2b_list)

        ANG_BOHR_KKR = _np.mean(a2b_list)

        #######################
        # 4) Determine constant type from reverse-calculated constant

        difference = _collections.OrderedDict()
        # difference = {}
        for ctype, value in self.ANG_BOHR_KKR.items():
            difference[ctype] = abs(ANG_BOHR_KKR - value)

        # find indices of minima
        indices = [i for i, val in enumerate(difference.values()) if val == min(difference.values())]
        # in case there are more than one minimum, assign by constants type importance order:
        #  lower index = higher importance. But issue a warning.
        constants_version = list(difference.keys())[indices[0]]
        if len(indices) > 1:
            print(f"Info: Workchain {wc} reverse-calculated 'ANG_BOHR_KKR' value undecisive. Could be either of "
                  f"{[list(difference.keys())[i] for i in indices]}. Chose {constants_version}.")

        #######################
        # 5) Set extra.
        if set_extra:
            extra = {
                'constants_version': constants_version.name,
                'ANG_BOHR_KKR': None,
                'RY_TO_EV_KKR': None
            }

            if constants_version in [KkrConstantsVersion.NEW,
                                     KkrConstantsVersion.INTERIM,
                                     KkrConstantsVersion.OLD]:
                extra['ANG_BOHR_KKR'] = self.ANG_BOHR_KKR[constants_version]
                extra['RY_TO_EV_KKR'] = self.RY_TO_EV_KKR[constants_version]
            else:
                extra['ANG_BOHR_KKR'] = ANG_BOHR_KKR
                extra['RY_TO_EV_KKR'] = None  # TODO recalculate as well

            wc.set_extra('kkr_constants_version', extra)

        #######################
        # 6) Return used version, or record results in dataframe
        if record:
            if group_label:
                row['group'] = group_label
            row['ctime'] = wc.ctime
            row['ANG_BOHR_KKR'] = ANG_BOHR_KKR
            row['constants_version'] = constants_version
            row['diff_new'] = difference[KkrConstantsVersion.NEW]
            row['diff_old'] = difference[KkrConstantsVersion.OLD]
            row['diff_interim'] = difference[KkrConstantsVersion.INTERIM]

            self._records = self._records.append(_pd.Series(name=wc.uuid, data=row))
        else:
            return constants_version

    def check_workchain_group(self,
                              group: _orm.Group,
                              process_labels: _typing.List[str] = [],
                              set_extra: bool = False,
                              zero_threshold: float = 1e-15):
        """Classify a group of finished workchains by their used KKR constants versions by reverse-calculation.

        Current implementation only works with aiida-kkr workflows which have a ``kkr_startpot_wc`` descendant.
        These are: ``kkr_scf_wc``, ``kkr_eos_wc``, ``kkr_imp_wc``.

        The results are appended to the dataframe :py:attr:`~aiida_jutools.util_kkr.KkrConstantsVersionChecker.records`.

        Will always check constants version by recalculating it, even if it may have already been set as an
        extra. To filter workchains by this extra, assuming it has already been set for them, use the method
        :py:meth:`~aiida_jutools.util_kkr.filter_using_runtime_version` instead.

        :param group: a group with aiida-kkr workchain nodes. Workchains must have a ``kkr_startpot_wc`` descendant.
        :param process_labels: list of valid aiida-kkr workchain process labels, e.g. ['kkr_scf_wc', ...].
        :param set_extra: True: Set an extra on the workchain denoting the identified KKR constants version and values.
        :param zero_threshold: Set structure cell elements below this threshold to zero to counter rounding errors.
        """
        if not process_labels:
            print("Warning: No process labels specified. I will do nothing. Specify labels of processes which have "
                  "a 'kkr_startpot_wc' descendant. Valid example: ['kkr_scf_wc', 'kkr_imp_wc'].")
        else:
            for node in group.nodes:
                if isinstance(node, _orm.WorkChainNode) and node.process_label in process_labels:
                    self.check_single_workchain(wc=node,
                                                record=True,
                                                set_extra=set_extra,
                                                zero_threshold=zero_threshold,
                                                group_label=group.label)

    def filter_using_runtime_version(self,
                                     wcs: _typing.List[_orm.WorkChainNode],
                                     select: bool = True,
                                     set_extra: bool = False) -> _typing.Union[
        _typing.List[bool], _typing.List[_orm.WorkChainNode]]:
        """Filter workchains by which of them are using the same KKR constants version as the interpreter at runtime.

        This method is useful for selecting those workchains which can be reused at runtime for new calculations.
        Using one which was performed with a constants version different from the runtime version would fail with an
        error after submission, unless the runtime is reset to match its KKR constants version.

        For instance, submitting a kkr_imp_wc with a parent GF writeout kkr_imp_wc with a different constants version
        would result in the error kkr_imp_wc [144] > kkr_imp_sub_wc [130] > KkrimpCalculation [302], with the
        log file ``_scheduler-stderr.txt`` containing the error message
        ``[read_potential] error ALAT value in potential is does not match``.

        This method assumes that each workchain has the KKR constants version which it uses set as an extra via
        :py:class:`~aiida_jutools.util_kkr.KkrConstantsVersionChecker.check_single_workchain` or
        :py:class:`~aiida_jutools.util_kkr.KkrConstantsVersionChecker.check_workchain_group`. If the extra cannot
        be read, the constants version gets recalculated by the former method.

        :param wcs: list of workchains nodes.
        :param select: True: return sublist of workchains using runtime version. False: return boolean mask.
        :param set_extra: True: If extra could not be read, set recalculated version as extra. False: Only recalculate.
        :return: list of matching workchains, or boolean mask of matching workchains (True = same version as runtime.)
        """

        def _uses_runtime_version(wc, set_extra: bool = False):
            version = None
            extra = wc.extras.get('kkr_constants_version', None)
            convert_extra_failed = False

            if extra:
                version_str = extra.get('constants_version', None)
                try:
                    version = KkrConstantsVersion[version_str.upper()]
                except KeyError as err:
                    convert_extra_failed = True

            if not extra or convert_extra_failed:
                do_set_extra = set_extra if not convert_extra_failed else True
                version = self.check_single_workchain(wc, record=False, set_extra=do_set_extra)

            return version == self.runtime_version

        mask = [_uses_runtime_version(wc, set_extra=set_extra) for wc in wcs]
        if select:
            return [wc for i, wc in enumerate(wcs) if mask[i]]
        else:
            return mask

    @staticmethod
    def check_single_workchain_provenance(wc: _orm.WorkChainNode):
        """Check whether the workchain and all its ancestors of a workchain used the same KKR constants versions.

        This requires that the constants version on the workchain AND its ancestors was set as extra before with either
        :py:class:`~aiida_jutools.util_kkr.KkrConstantsVersionChecker.check_single_workchain` or
        :py:class:`~aiida_jutools.util_kkr.KkrConstantsVersionChecker.check_workchain_group`.

        Currently, only ``kkr_imp_wc`` workchains supported.

        Currently checked nodes provenance path (these must have the extras): ``kkr_scf_wc`` > ``kkr_imp_wc``.

        In theory, all constants versions along the provenance path MUST be identical, and if not, the workchain
        should have failed. If it has finished successfully however, the extras must be wrong.

        :param wc: finished aiida-kkr workchain. Must have a ``kkr_startpot_wc`` descendant.
        """
        # TODO: Include intermediate kkr_imp_wc in provenance path check (e.g. GF writeout kkr_imp_wc).
        # TODO: store findings in dataframe or dict

        if wc.process_label != 'kkr_imp_wc':
            print(
                f"Workchain '{wc.label}', pk={wc.pk} is not a {_kkr_workflows.kkr_imp_wc.__name__}. "
                f"Currently not supported.")
        else:
            try:
                imp_version = wc.extras['kkr_constants_version']['constants_version']

                scf_wcs = wc.get_incoming(node_class=_orm.RemoteData,
                                          link_label_filter='remote_data_host').all_nodes()[0].get_incoming(
                    node_class=_kkr_workflows.kkr_scf_wc).all_nodes()
                if not scf_wcs:
                    print(
                        f"Workchain '{wc.label}', pk={wc.pk} does not have a "
                        f"{_kkr_workflows.kkr_scf_wc.__name__} ancestor.")
                else:
                    try:
                        scf_version = scf_wcs[0].extras['kkr_constants_version']['constants_version']
                        if imp_version != scf_version:
                            print(f"Mismatch in {KkrConstantsVersion.__name__} extras for kkr_imp_wc pk={wc.pk}, "
                                  f"label='{wc.label}': parent kkr_scf_wc {scf_version}, kkr_imp_wc {imp_version}.")
                    except KeyError as err:
                        print(f"Workchain '{wc.label}', pk={wc.pk} is missing 'kkr_constants_version' extra.")
            except KeyError as err:
                print(f"Workchain '{wc.label}', pk={wc.pk} is missing 'kkr_constants_version' extra.")

    def check_workchain_group_provenance(self,
                                         group: _orm.Group,
                                         process_labels: _typing.List[str] = ['kkr_imp_wc']):
        """Check whether the workchain and all its ancestors of a workchain used the same KKR constants versions.

        This requires that the constants version on the workchain AND its ancestors was set as extra before with either
        :py:class:`~aiida_jutools.util_kkr.KkrConstantsVersionChecker.check_single_workchain` or
        :py:class:`~aiida_jutools.util_kkr.KkrConstantsVersionChecker.check_workchain_group`.

        Currently, only ``kkr_imp_wc`` workchains supported.

        Currently checked nodes provenance path (these must have the extras): ``kkr_scf_wc`` > ``kkr_imp_wc``.

        Currently, findings are only printed, not stored in any way.

        In theory, all constants versions along the provenance path MUST be identical, and if not, the workchain
        should have failed. If it has finished successfully however, the extras must be wrong.

        :param group: a group with aiida-kkr workchain nodes. Workchains must have a ``kkr_startpot_wc`` descendant.
        :param process_labels: currently only ['kkr_imp_wc'] supported.
        """
        if not process_labels or process_labels != ['kkr_imp_wc']:
            print("Warning: Unsupported process_labels list. I will do nothing. Currently supported: ['kkr_imp_wc'].")
        else:
            for node in group.nodes:
                if isinstance(node, _orm.WorkChainNode) and node.process_label in process_labels:
                    self.check_single_workchain_provenance(node)
