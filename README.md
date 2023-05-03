[![Documentation Status](https://readthedocs.org/projects/aiida-jutools/badge/?version=latest)](https://aiida-jutools.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://travis-ci.org/JuDFTteam/aiida-jutools.svg?branch=master)](https://travis-ci.org/JuDFTteam/aiida-jutools)
[![codecov](https://codecov.io/gh/JuDFTteam/aiida-jutools/branch/master/graph/badge.svg)](https://codecov.io/gh/JuDFTteam/aiida-jutools)
[![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT)
[![GitHub version](https://badge.fury.io/gh/JuDFTteam%2Faiida-jutools.svg)](https://badge.fury.io/gh/JuDFTteam%2Faiida-jutools)
[![PyPI version](https://badge.fury.io/py/aiida-jutools.svg)](https://badge.fury.io/py/aiida-jutools)

# aiida-jutools

This package offers

1. **For users:** Tools for simplifying daily work with the [AiiDA](https://aiida.net) workflow engine, especically with respect to data management and high-throughput computing.
2. **For developers**: Common AiiDA layer for the [JuDFTteam AiiDA plugins](https://github.com/JuDFTteam). The underlying common Python layer is called [masci-tools](https://github.com/JuDFTteam/masci-tools).
<!-- 3. Science tools: -->
<!--   - `jutools.structure.structure_analyzer.analyze_symmetry` -->


Just import with ``import aiida_jutools as jutools``. Then you can call all tools like so: ``jutools.subpackage.tool
()``.

## Installation

AiiDA-JuTools is not on PyPI or Anaconda yet. You can still install it from this repository directly, though.

```bash
git clone git@github.com:JuDFTteam/aiida-jutools.git
cd aiida-jutools
pip install -e .
```

## Documentation

Under construction. However, all classes and methods have comprehensive docstrings.

### For developers

Written some AiiDA code potentially useful for others? Please add it here!

Please adhere to the developer coding conventions:
- Place larger classes in a subpackage (subfolder) in a separate module (file). Smaller stuff like functions go in the
  respective subpackage's ``subpackage/util.py``.
- Make all tools available at subpackage level via import in ``subpackage/__init__.py``. See existing files for how
  to do that. Also import each new subpackage in the package's top-level ``__init__.py``. Together, this enables to
  access all tools with tab completion like ``jutools.subpackage.tool()`` instead of needing to import single modules.
  The former is better practice. For that purpose, keep subpackage and module names short but clear, preferably one
  word only, as opposed to tool names which should be as long as needed to be self-explanatory.
- Prefix non-user tools with ``_`` to keep user namespace clean and organized.
- Prefix all imports inside modules with ``_`` to keep user namespace clean and organized. Prefer top namespace
  imports to avoid name conflicts and ambiguities. See existing modules for conventions, e.g. with respect to AiiDA
  imports.
- Add docstring for every added tool. Add ``typing`` hints wherever possible and sensible. See existing modules for
  examples.
- When manipulating AiiDA nodes, implement with 'load or create' pattern: load nodes if already exist, otherwise create.
  Provide a ``dry_run:bool=True`` and verbosity options (``verbosity:int``, ``verbose:bool``, or ``silent:bool``).
- If you use [cross-references](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#cross-referencing-python-objects)
  in docstrings, do cross-referencing relative to the current location (i.e., prefixed by a dot `.`). Example:
  `` :py:func:`~.query_processes` `` instead of
  `` :py:func:`~aiida_jutools.process.query_processes` ``, when in module `aiida_jutools.process`.
