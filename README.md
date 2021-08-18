[![Documentation Status](https://readthedocs.org/projects/aiida-jutools/badge/?version=latest)](https://aiida-jutools.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://travis-ci.org/JuDFTteam/aiida-jutools.svg?branch=master)](https://travis-ci.org/JuDFTteam/aiida-jutools)
[![codecov](https://codecov.io/gh/JuDFTteam/aiida-jutools/branch/master/graph/badge.svg)](https://codecov.io/gh/JuDFTteam/aiida-jutools)
[![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT)
[![GitHub version](https://badge.fury.io/gh/JuDFTteam%2Faiida-jutools.svg)](https://badge.fury.io/gh/JuDFTteam%2Faiida-jutools)
[![PyPI version](https://badge.fury.io/py/aiida-jutools.svg)](https://badge.fury.io/py/aiida-jutools)

# aiida-jutools

This package offers

1. tools for simpler management of larger [AiiDA](https://aiida.net) (thousands of nodes and up).
These can be found in the modules `util_*`, like `util_process`  for managing large numbers of AiiDA processes and process nodes.
2. science tools:
  * `structure_analyzer`
3. common plugin developer tools for
  * [aiida-fleur](https://github.com/JuDFTteam/aiida-kkr/)
  * [aiida-kkr](https://github.com/JuDFTteam/aiida-fleur)
  * [aiida-spex](https://iffgit.fz-juelich.de/chand/aiida-spex)
  * [aiida-spirit](https://github.com/JuDFTteam/aiida-spirit)
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

## Usage and Documentation

under construction

