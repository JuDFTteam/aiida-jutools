# -*- coding: utf-8 -*-
# pylint: disable=unused-import
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
"""Tools for working with plugins: aiida-kkr."""

from .constants import \
    get_runtime_kkr_constants_version, \
    KkrConstantsVersion, \
    KkrConstantsVersionChecker

from .util import \
    find_Rcut, \
    has_kkr_calc_converged, \
    query_kkr_wc, \
    query_structure_from
