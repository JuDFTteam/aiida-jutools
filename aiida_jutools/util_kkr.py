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

from aiida.orm import CalcJobNode as _CalcJobNode, WorkChainNode as _WorkChainNode

from aiida.orm import Dict as _Dict
from aiida.orm import QueryBuilder as _QueryBuilder, Group as _Group, RemoteData as _RemoteData, \
    StructureData as _StructureData

from aiida_kkr.workflows import kkr_imp_wc as _kkr_imp_wc, kkr_scf_wc as _kkr_scf_wc
from masci_tools.util.chemical_elements import ChemicalElements as _ChemicalElements


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


def find_Rcut(structure:_StructureData, shell_count:int=2, rcut_init:float=7.0) -> float:
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
