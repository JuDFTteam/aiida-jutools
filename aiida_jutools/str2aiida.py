#!/usr/bin/env python
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

import sys

# aiida imports
from aiida import load_profile
load_profile()

from structure_analyzer import analyze_symmetry
from terminal_colors import *
from pprint import pprint

__copyright__ = (u"Copyright (c), 2019-2020, Forschungszentrum Jülich GmbH, "
                 "IAS-1/PGI-1, Germany. All rights reserved.")
__license__ = "MIT license, see LICENSE.txt file"
__version__ = "0.1"
__contributors__ = u"Roman Kováčik"

def cif2astr(cifpath):

    prompt = ""

    dd = {
        'fmt' : 'cif',
        'cifpath' : cifpath,
        'outmode' : ['a_conv']
    }

    structure = analyze_symmetry(dd)

    if structure.get('aiida_structure_conventional'):

        print(prompt + structure['aiida_structure_conventional'].extras['check_cif']['message']+'\n')
        print(prompt + 'label:           ' + CC1 + structure['aiida_structure_conventional'].label + CEND)
        print(prompt + 'description:     ' + structure['aiida_structure_conventional'].description)
        print(prompt + 'prototype:       ' + CC2 + structure['aiida_structure_conventional'].extras['prototype']['nprot'] + CEND +
              ' : ' + CC2 + structure['aiida_structure_conventional'].extras['prototype']['nrw'] + CEND)
        print(prompt + 'specification:   ' + structure['aiida_structure_conventional'].extras['system']['specification'])

        print('\n'+'extras:'+'\n')
        pprint(structure['aiida_structure_conventional'].extras, width=256)

cif2astr('/path/to/cif/file.cif')
