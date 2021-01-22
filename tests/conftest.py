# -*- coding: utf-8 -*-
"""Initialise a text database and profile for pytest."""

import pytest

pytest_plugins = ['aiida.manage.tests.pytest_fixtures']

@pytest.fixture(scope='function')
def fixture_sandbox():
    """Return a `SandboxFolder`."""
    from aiida.common.folders import SandboxFolder
    with SandboxFolder() as folder:
        yield folder


