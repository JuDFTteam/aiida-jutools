[![Documentation Status](https://readthedocs.org/projects/aiida-jutools/badge/?version=latest)](https://aiida-jutools.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://travis-ci.org/JuDFTteam/aiida-jutools.svg?branch=master)](https://travis-ci.org/JuDFTteam/aiida-jutools)
[![codecov](https://codecov.io/gh/JuDFTteam/aiida-jutools/branch/master/graph/badge.svg)](https://codecov.io/gh/JuDFTteam/aiida-jutools)
[![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT)
[![GitHub version](https://badge.fury.io/gh/JuDFTteam%2Faiida-jutools.svg)](https://badge.fury.io/gh/JuDFTteam%2Faiida-jutools)
[![PyPI version](https://badge.fury.io/py/aiida-jutools.svg)](https://badge.fury.io/py/aiida-jutools)

# aiida-jutools

This package offers

1. Tools for managing high-throughput experiments (thousands or millions of database nodes) with [AiiDA]
   (https://aiida.net).
2. Science tools:
  - `jutools.structure.analyze_symmetry`
3. common plugin developer tools for
  - [aiida-fleur](https://github.com/JuDFTteam/aiida-kkr/)
  - [aiida-kkr](https://github.com/JuDFTteam/aiida-fleur)
  - [aiida-spex](https://iffgit.fz-juelich.de/chand/aiida-spex)
  - [aiida-spirit](https://github.com/JuDFTteam/aiida-spirit)

Just import with ``import aiida_jutools as jutools``. Then you can call all tools like so: ``jutools.subpackage.tool
()``.

## Installation

Until further notice, install either by cloning the repository and use locally, 
or by doing an editable install via `pip.`

```bash
# install dependencies
pip install aiida-core numpy pandas humanfriendly pytz
pip install pycifrw spglib pymatgen
pip install masci-tools aiida-kkr
# finally:
pip install -e git+https://github.com/JuDFTteam/aiida-jutools@develop#egg=aiida-jutools
```

## Documentation

Under construction. For the time being, see the extensive docstring documentation.

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

