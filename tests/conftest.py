# -*- coding: utf-8 -*-
"""Pytest configuration and fixtures for aiida-jutools tests."""

import pytest


# ============================================================================
# Command-line options
# ============================================================================

def pytest_addoption(parser):
    """Add custom command-line options."""
    parser.addoption(
        "--strict-deprecation",
        action="store_true",
        default=False,
        help="Fail tests on deprecation warnings (stricter mode for CI/development)"
    )
    parser.addoption(
        "--with-computer",
        action="store",
        default=None,
        help="Specify AiiDA computer label for integration tests (e.g., 'iffslurm')"
    )


def pytest_configure(config):
    """Configure pytest based on command-line options."""
    # If --strict-deprecation is set, convert deprecation warnings to errors
    if config.getoption("--strict-deprecation"):
        import warnings
        warnings.filterwarnings("error", category=DeprecationWarning)
        warnings.filterwarnings("error", category=FutureWarning)
        # Re-allow specific third-party deprecations if needed
        # warnings.filterwarnings("default", category=DeprecationWarning, module="paramiko")


# ============================================================================
# Session-level fixtures (run once per test session)
# ============================================================================

@pytest.fixture(scope="session")
def aiida_profile():
    """Get the current AiiDA profile name."""
    try:
        from aiida import load_profile
        profile = load_profile()
        return profile.name
    except Exception as e:
        pytest.skip(f"AiiDA profile not available: {e}")


# ============================================================================
# Function-level fixtures (run for each test)
# ============================================================================

@pytest.fixture
def computer_label(request):
    """Get computer label from command line or skip test."""
    label = request.config.getoption("--with-computer")
    if label is None:
        pytest.skip("Test requires --with-computer option")
    return label


@pytest.fixture
def mock_computer():
    """Create a mock computer object for testing without AiiDA database."""
    from unittest.mock import MagicMock

    computer = MagicMock()
    computer.label = "test_slurm_computer"
    computer.scheduler_type = "core.slurm"

    return computer


@pytest.fixture
def mock_non_slurm_computer():
    """Create a mock non-SLURM computer for testing."""
    from unittest.mock import MagicMock

    computer = MagicMock()
    computer.label = "test_pbs_computer"
    computer.scheduler_type = "core.pbs"

    return computer


# ============================================================================
# Utility fixtures
# ============================================================================

@pytest.fixture
def capture_warnings():
    """Context manager to capture warnings during tests."""
    import warnings

    class WarningCapture:
        def __init__(self):
            self.warnings = []

        def __enter__(self):
            self._context = warnings.catch_warnings(record=True)
            self.warnings = self._context.__enter__()
            warnings.simplefilter("always")
            return self.warnings

        def __exit__(self, *args):
            return self._context.__exit__(*args)

        def get_aiida_deprecations(self):
            """Filter for AiiDA deprecation warnings."""
            return [w for w in self.warnings
                   if issubclass(w.category, DeprecationWarning)
                   and 'aiida' in str(w.message).lower()]

    return WarningCapture()