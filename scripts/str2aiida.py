#!/usr/bin/env python
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
"""
Testing script, change '/path/to/cif/file.cif' in last line to existing cif file path
- apart from aiida, pymatgen and PyCifRW packages are needed to use > analyze_symmetry <
"""

# aiida imports
from aiida import load_profile

load_profile()

import aiida_jutools as jutools

__copyright__ = (u"Copyright (c), 2019-2020, Forschungszentrum Jülich GmbH, "
                 "IAS-1/PGI-1, Germany. All rights reserved.")
__license__ = "MIT license, see LICENSE.txt file"
__version__ = "0.1"
__contributors__ = u"Roman Kováčik"

if __name__ == '__main__':
    jutools.io.cif2astr('/path/to/cif/file.cif')
