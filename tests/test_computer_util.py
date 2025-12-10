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
    """Test get_queues returns queue names for a real SLURM computer."""
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
    # Queues should be strings when with_node_count=False
    assert all(isinstance(q, str) for q in queues)


@pytest.mark.integration
@pytest.mark.requires_computer
@pytest.mark.slow
def test_get_queues_with_node_counts(computer_label, aiida_profile):
    """Test get_queues returns node count information."""
    from aiida_jutools.computer.util import get_computers, get_queues, is_slurm_computer

    computers = get_computers(computer_label)
    if not computers:
        pytest.skip(f"Computer '{computer_label}' not found")

    computer = computers[0]

    if not is_slurm_computer(computer):
        pytest.skip(f"Computer '{computer.label}' is not a SLURM computer")

    # Get queues with node counts
    queues = get_queues(computer, with_node_count=True, with_arch=False, silent=True)

    assert isinstance(queues, list)
    assert len(queues) > 0

    # Each entry should be [queue_name, total_nodes, idle_nodes]
    for queue_info in queues:
        assert isinstance(queue_info, list)
        assert len(queue_info) == 3
        assert isinstance(queue_info[0], str)  # queue_name
        assert isinstance(queue_info[1], int)  # total_nodes
        assert isinstance(queue_info[2], int)  # idle_nodes
        # idle_nodes should be <= total_nodes
        assert queue_info[2] <= queue_info[1]


@pytest.mark.integration
@pytest.mark.requires_computer
@pytest.mark.slow
def test_get_queues_with_architecture(computer_label, aiida_profile):
    """Test get_queues returns architecture information."""
    from aiida_jutools.computer.util import get_computers, get_queues, is_slurm_computer

    computers = get_computers(computer_label)
    if not computers:
        pytest.skip(f"Computer '{computer_label}' not found")

    computer = computers[0]

    if not is_slurm_computer(computer):
        pytest.skip(f"Computer '{computer.label}' is not a SLURM computer")

    # Get queues with architecture but no node counts
    queues = get_queues(computer, with_node_count=False, with_arch=True, silent=True)

    assert isinstance(queues, list)
    assert len(queues) > 0

    # Each entry should be [queue_name, architecture]
    for queue_info in queues:
        assert isinstance(queue_info, list)
        assert len(queue_info) == 2
        assert isinstance(queue_info[0], str)  # queue_name
        assert isinstance(queue_info[1], str)  # architecture
        # Architecture should be AMD, intel, or unknown
        assert queue_info[1] in ['AMD', 'intel', 'unknown']


@pytest.mark.integration
@pytest.mark.requires_computer
@pytest.mark.slow
def test_get_queues_with_full_info(computer_label, aiida_profile):
    """Test get_queues returns complete information (node counts + architecture)."""
    from aiida_jutools.computer.util import get_computers, get_queues, is_slurm_computer

    computers = get_computers(computer_label)
    if not computers:
        pytest.skip(f"Computer '{computer_label}' not found")

    computer = computers[0]

    if not is_slurm_computer(computer):
        pytest.skip(f"Computer '{computer.label}' is not a SLURM computer")

    # Get queues with both node counts and architecture
    queues = get_queues(computer, with_node_count=True, with_arch=True, silent=True)

    assert isinstance(queues, list)
    assert len(queues) > 0

    # Each entry should be [queue_name, total_nodes, idle_nodes, architecture]
    for queue_info in queues:
        assert isinstance(queue_info, list)
        assert len(queue_info) == 4
        assert isinstance(queue_info[0], str)  # queue_name
        assert isinstance(queue_info[1], int)  # total_nodes
        assert isinstance(queue_info[2], int)  # idle_nodes
        assert isinstance(queue_info[3], str)  # architecture
        # idle_nodes should be <= total_nodes
        assert queue_info[2] <= queue_info[1]
        # Architecture should be AMD, intel, or unknown
        assert queue_info[3] in ['AMD', 'intel', 'unknown']


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