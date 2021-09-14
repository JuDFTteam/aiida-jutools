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

For users:

We recommended to use this package with the import statement ``import aiida_jutools as jutools``. In your code,
you can then call all available tools like so: ``jutools.module.tool()``.

For developers:

- Place larger classes in a subpackage (subfolder) in a separate module (file), smaller functions in the respective
  subpackage's ``util.py``.
- Make all available at subpackage level via import in ``subpackage/__init__.py``. See existing files as templates.
- Also import each new subpackage in the package's top-level ``__init__.py``.
- Prefix non-user tools with ``_`` to keep user namespace clean and organized.
- Prefix all imports inside modules with ``_`` to keep user namespace clean and organized. See existing modules
  for conventions, e.g. w.r.t. AiiDA imports.
- Add ``typing`` hints wherever possible and sensible. See existing files for examples.
- When manipulating AiiDA nodes, implement with 'load or create' pattern: load nodes if already exist, otherwise create.
  Provide a ``dry_run:bool=True`` and verbosity options (``verbosity:int``, ``verbose:bool``, or ``silent:bool``).
"""
__version__ = "0.1.0-dev1"

# Import all subpackages.
import code
import computer
import group
import io
import kkr
import logging
import node
import process_functions
import structure

