# -*- coding: utf-8 -*-
"""Tests for aiida_jutools.computer.util module."""

import pytest


# ============================================================================
# Unit tests (no AiiDA database required)
# ============================================================================

@pytest.mark.unit
def test_is_slurm_computer_with_core_slurm(mock_computer):
    """Test is_slurm_computer recognizes 'core.slurm' scheduler."""
    from aiida_jutools.computer.util import is_slurm_computer

    mock_computer.scheduler_type = "core.slurm"
    assert is_slurm_computer(mock_computer) is True


@pytest.mark.unit
def test_is_slurm_computer_with_slurm(mock_computer):
    """Test is_slurm_computer recognizes 'slurm' scheduler."""
    from aiida_jutools.computer.util import is_slurm_computer

    mock_computer.scheduler_type = "slurm"
    assert is_slurm_computer(mock_computer) is True


@pytest.mark.unit
def test_is_slurm_computer_with_uppercase(mock_computer):
    """Test is_slurm_computer is case-insensitive."""
    from aiida_jutools.computer.util import is_slurm_computer

    mock_computer.scheduler_type = "CORE.SLURM"
    assert is_slurm_computer(mock_computer) is True


@pytest.mark.unit
def test_is_slurm_computer_with_pbs(mock_non_slurm_computer):
    """Test is_slurm_computer returns False for PBS scheduler."""
    from aiida_jutools.computer.util import is_slurm_computer

    assert is_slurm_computer(mock_non_slurm_computer) is False


@pytest.mark.unit
def test_is_slurm_computer_with_sge(mock_computer):
    """Test is_slurm_computer returns False for SGE scheduler."""
    from aiida_jutools.computer.util import is_slurm_computer

    mock_computer.scheduler_type = "core.sge"
    assert is_slurm_computer(mock_computer) is False


# ============================================================================
# Integration tests (require AiiDA database and configured computer)
# ============================================================================

@pytest.mark.integration
@pytest.mark.requires_computer
def test_get_computers_returns_list(aiida_profile):
    """Test get_computers returns a list."""
    from aiida_jutools.computer.util import get_computers

    computers = get_computers()
    assert isinstance(computers, list)


@pytest.mark.integration
@pytest.mark.requires_computer
def test_get_computers_with_pattern(computer_label, aiida_profile):
    """Test get_computers can find a specific computer."""
    from aiida_jutools.computer.util import get_computers

    computers = get_computers(computer_label)
    assert len(computers) > 0
    assert any(computer_label.lower() in comp.label.lower() for comp in computers)


@pytest.mark.integration
@pytest.mark.requires_computer
@pytest.mark.slow
def test_get_queues_with_real_computer(computer_label, aiida_profile):
    """Test get_queues returns queue information for a real SLURM computer."""
    from aiida_jutools.computer.util import get_computers, get_queues, is_slurm_computer

    computers = get_computers(computer_label)
    if not computers:
        pytest.skip(f"Computer '{computer_label}' not found")

    computer = computers[0]

    if not is_slurm_computer(computer):
        pytest.skip(f"Computer '{computer.label}' is not a SLURM computer")

    # This will actually query the cluster, so mark as slow
    queues = get_queues(computer, with_node_count=False, silent=True)

    assert isinstance(queues, list)
    assert len(queues) > 0
    # Queues should be strings
    assert all(isinstance(q, str) for q in queues)


# ============================================================================
# Deprecation warning tests
# ============================================================================

@pytest.mark.unit
def test_no_import_deprecation_warnings(capture_warnings):
    """Test that importing the module doesn't trigger deprecation warnings."""
    with capture_warnings as w:
        # Import the module
        import aiida_jutools.computer.util

        # Check for deprecation warnings
        deprecations = [warning for warning in w
                       if issubclass(warning.category, DeprecationWarning)]

        if deprecations:
            messages = "\n".join(f"  {d.filename}:{d.lineno}: {d.message}"
                               for d in deprecations)
            pytest.fail(f"Found {len(deprecations)} deprecation warnings on import:\n{messages}")


# # ============================================================================
# # Example: Test with expected failure (optional, for demonstration)
# # ============================================================================
#
# @pytest.mark.unit
# @pytest.mark.xfail(reason="Example of expected failure - remove this test")
# def test_example_expected_failure():
#     """This test is expected to fail - it's just an example."""
#     assert False, "This is an intentional failure for demonstration"