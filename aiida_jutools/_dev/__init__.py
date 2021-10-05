# pylint: disable=unused-import
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
"""AiiDA-JuTools: dev tools. Non-public (prefix with ``_``)."""

from .periodic_tables import \
    minimal_periodic_table

# not importing constants from .terminal_colors to this namespace (currently, jutools._dev) since large number of
# constants. instead, import locally from module explicitly:
# from .terminal_colors import *
