# Changelog

All notable changes to aiida-jutools are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3] - 2026-09-18

### Fixed

- **`plugins.kkr.util.find_Rcut` returned an oversized impurity cluster radius.** The
  function grows a trial radius in 5 Å steps until the neighbour list around the first
  site holds at least `shell_count` distinct shells, then trims it back to the midpoint
  between the last requested shell and the next one out. That trim needs the next shell
  as a bracket. When the growth loop landed *exactly* on `shell_count`, the trim was
  skipped and the function returned a radius one step past the one actually measured —
  12 Å instead of roughly 6 Å at the default `rcut_init=7.0`.

  Verified against a live AiiDA profile of impurity embeddings: of 190 (host, scale
  factor) cells, 18 change radius and every one of them then lands exactly on central
  atom plus two shells — 15 sites for body-centred cubic hosts, 19 for face-centred
  cubic. Before the fix those cells held 59 to 135 sites, the worst case being seven
  times the intended cluster. The remaining 172 cells produce byte-identical
  `create_scoef_array` output before and after.

- **The package version was declared twice and had drifted.** `setup.json` said `0.1.2`
  while `aiida_jutools.__version__` still said `0.1.0-dev1`, so reading `__version__` to
  identify an installation gave the wrong answer. Both now agree.

### Added

- `aiida-kkr` declared as an **optional** dependency: `pip install aiida-jutools[kkr]`.
  It was imported at module scope by `plugins.kkr` but declared nowhere. Optional rather
  than required, because `import aiida_jutools` deliberately does not pull in the
  `plugins` subpackage — only `import aiida_jutools.plugins.kkr` needs aiida-kkr.
- A basic `pytest` test suite (`pytest.ini`, `tests/`), with `unit`, `integration`,
  `slow`, `requires_computer` and `requires_database` markers.
- `tests/test_kkr_util.py`, covering the `find_Rcut` fix above. It builds unstored
  `StructureData` — nothing stored, nothing queried, no database write — and skips where
  `aiida-kkr` or `pymatgen` is absent.
- `computer.get_queues`: more options.

### Changed

- `code` and `computer`: hardcoded values replaced with dynamic lookups.
- `get_code`: relaxed the queues lookup constraint.
- `process.query_processes`: updated to the new location of `CalculationQueryBuilder`.
- `structure_analyzer`: updated `gcd` calls.

## [0.1.2] - 2023-12-25

### Added

- `CITATION.cff`, and a DOI badge in the README.

## [0.1.1] - 2023-12-24

### Changed

- Updated the declared supported Python versions.

## [0.1.0] - 2023-12-01

First release. AiiDA helper tools for the JuDFT plugins, covering `code`, `computer`,
`group`, `io`, `logging`, `meta`, `node`, `process`, `structure` and `submit`, plus the
`plugins.kkr` subpackage and the `Tabulator`.

- AiiDA v2 support (`Computer.name` → `Computer.label`, `EntityExtrasMixin` →
  `EntityExtras`).
- Tools import without a loaded AiiDA profile where possible.

[Unreleased]: https://github.com/JuDFTteam/aiida-jutools/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/JuDFTteam/aiida-jutools/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/JuDFTteam/aiida-jutools/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/JuDFTteam/aiida-jutools/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/JuDFTteam/aiida-jutools/releases/tag/v0.1.0
