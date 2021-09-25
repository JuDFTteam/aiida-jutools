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
"""AiiDA JuTools.

We recommended to use this library with the import statement ``import aiida_jutools as jutools``. In your code,
you can then call all available tools like so: ``jutools.package.tool()``.
"""
__version__ = "0.1.0-dev1"

# Import all of the library's user packages.
from . import code
from . import computer
from . import group
from . import io
from . import kkr
from . import logging
from . import meta
from . import node
from . import process
from . import process_functions
from . import submit
from . import structure
# # import all of the library's developer packages.
from . import _dev

# Potentially problematic imports:
# - kkr: As soon as aiida-kkr becomes dependent on aiida-jutools, this import MIGHT introduce a circular
#        dependencies. A simple test (made aiida-kkr import aiida-jutools) had no such effect. But if it
#        occurs, here a few potential solutions:
#        - Decouple the kkr package = remove from import list above. Then all code using it must be updated
#          to import it separately, like from aiida_jutools import kkr as _jutools_kkr. Might break external code.
#        - Hide all aiida-kkr imports = in resp. module, move them inside the tools that use them. If it works,
#          this might be a solution that does not break external code.
#
# The potential problem and the solution stated above, if it becomes one, applies to other JuDFTTeam plugins as well,
# should they start using aiida-jutools as common codebase (aiida-fleur, aiida-spirit, aiida-spex, ...).
