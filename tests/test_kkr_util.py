# -*- coding: utf-8 -*-
"""Tests for aiida_jutools.plugins.kkr.util module."""

import pytest

# find_Rcut lives in a module that imports aiida_kkr at import time, and it calls
# StructureData.get_pymatgen(). Skip the whole file where those are not installed.
pytest.importorskip("aiida")
pytest.importorskip("aiida_kkr")
pytest.importorskip("pymatgen")

# Conventional cubic cells, fractional coordinates.
_BCC = [[0.0, 0.0, 0.0], [0.5, 0.5, 0.5]]
_FCC = [[0.0, 0.0, 0.0], [0.0, 0.5, 0.5], [0.5, 0.0, 0.5], [0.5, 0.5, 0.0]]


def _cubic_structure(symbol, a, frac_coords):
    """Build an unstored cubic StructureData. Nothing is stored; no database write happens."""
    from aiida import orm

    structure = orm.StructureData(cell=[[a, 0.0, 0.0], [0.0, a, 0.0], [0.0, 0.0, a]])
    for frac in frac_coords:
        structure.append_atom(position=[a * x for x in frac], symbols=symbol)
    return structure


# ============================================================================
# find_Rcut
# ============================================================================

@pytest.mark.unit
def test_find_Rcut_trims_when_growth_loop_lands_exactly_on_shell_count(aiida_profile):
    """Regression guard: barium bcc used to return the untrimmed 12 A instead of ~6.06 A.

    The growth loop lands exactly on shell_count=2 for this lattice, which used to skip
    the trim and return rcut_init + 5 -- a radius that was never even measured. Measured
    on a real barium cell, that gave a 113-site cluster where the intended two shells are
    15 sites (1 + 8 + 6 for bcc; the fcc count is 19).
    """
    from aiida_jutools.plugins.kkr.util import find_Rcut

    rcut = find_Rcut(_cubic_structure("Ba", 5.02, _BCC), shell_count=2, rcut_init=7.0)

    assert rcut < 7.0, "returned radius must never exceed the initial one it was grown from"
    # 2nd shell 5.02 A, 3rd shell 7.0994 A: the radius must sit strictly between them.
    assert 5.02 < rcut < 7.0994
    assert rcut == pytest.approx(6.0597, abs=1e-4)


@pytest.mark.unit
def test_find_Rcut_unchanged_where_growth_loop_already_overshoots(aiida_profile):
    """Aluminium fcc already overshot shell_count, so the fix must not move its radius."""
    from aiida_jutools.plugins.kkr.util import find_Rcut

    rcut = find_Rcut(_cubic_structure("Al", 4.05, _FCC), shell_count=2, rcut_init=7.0)

    # 2nd shell 4.05 A, 3rd shell 4.9602 A.
    assert 4.05 < rcut < 4.9602
    assert rcut == pytest.approx(4.5051, abs=1e-4)


# ============================================================================
# find_Rcut -> create_scoef_array
# ============================================================================
#
# The radius is only half the claim. `find_Rcut` returns a number; what the impurity
# calculation actually gets is whatever `create_scoef_array` (aiida-kkr) builds when fed
# that number, and until now nothing checked that the two agree. These two tests close
# that gap: they assert the corrected radius produces the *intended site count*, which is
# the quantity the fix was always about.

@pytest.mark.unit
def test_rcut_gives_intended_site_count_bcc(aiida_profile):
    """The corrected radius must build 15 sites, not the ~113 the untrimmed 12 A gave.

    This is the case the fix changes: the growth loop lands exactly on shell_count, which
    used to skip the trim. Two bcc shells are 1 + 8 + 6 = 15 sites.
    """
    from aiida_jutools.plugins.kkr.util import find_Rcut
    from aiida_kkr.tools.tools_kkrimp import create_scoef_array

    structure = _cubic_structure("Ba", 5.02, _BCC)
    rcut = find_Rcut(structure, shell_count=2, rcut_init=7.0)

    assert len(create_scoef_array(structure, rcut)) == 15
    # The untrimmed radius is what the bug returned; it is here to show the size of the
    # difference the fix makes, not as a target.
    assert len(create_scoef_array(structure, 12.0)) > 100


@pytest.mark.unit
def test_rcut_gives_intended_site_count_fcc(aiida_profile):
    """The unaffected case must still build 19 sites: 1 + 12 + 6 for two fcc shells.

    Copper at scale factor 1.0 in the single-impurity database is this geometry, and its
    stored embeddings record exactly 19 sites at Rcut 4.045575 A -- so this is the count a
    fresh submission has to reproduce for the database to stay homogeneous.
    """
    from aiida_jutools.plugins.kkr.util import find_Rcut
    from aiida_kkr.tools.tools_kkrimp import create_scoef_array

    structure = _cubic_structure("Al", 4.05, _FCC)
    rcut = find_Rcut(structure, shell_count=2, rcut_init=7.0)

    assert len(create_scoef_array(structure, rcut)) == 19
